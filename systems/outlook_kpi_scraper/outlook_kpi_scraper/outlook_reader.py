"""
Outlook COM reader – fetches MailItems from one or more folders.

Key design decisions for reliability on large mailboxes:
  * Index-based iteration (items.Item(idx)) instead of Python for-loop
    over COM enumerator – avoids OLE enumerator crashes.
  * Timezone-safe cutoff – always strips or matches tzinfo to prevent
    "can't compare offset-naive and offset-aware datetimes".
  * Consecutive-error circuit breaker – if N items in a row fail with
    COM errors, we stop that folder early and move on.
  * Per-folder error isolation – a crash in one folder never kills the
    whole pipeline.
  * Explicit COM reference release after each folder to prevent Outlook
    "Out of memory or system resources" on large mailboxes.
"""

import gc
import logging
import os
from datetime import datetime, timedelta, timezone

import pywintypes
import win32com.client

log = logging.getLogger(__name__)

# After this many *consecutive* COM errors in a single folder we assume
# Outlook is resource-exhausted and move to the next folder.
MAX_CONSECUTIVE_ERRORS = 50


# ------------------------------------------------------------------
# Exchange DN -> SMTP resolution
# ------------------------------------------------------------------
def _resolve_smtp_address(item) -> str | None:
    """Resolve Exchange DN sender addresses to real SMTP.

    Exchange stores internal senders as DN strings like:
      /O=EXCHANGELABS/OU=.../CN=RECIPIENTS/CN=...
    Returns None if resolution fails or sender is already SMTP.
    """
    sender_type = getattr(item, "SenderEmailType", None)
    if sender_type != "EX":
        return None

    # Method 1: Sender.GetExchangeUser().PrimarySmtpAddress
    try:
        sender = item.Sender
        if sender is not None:
            exch_user = sender.GetExchangeUser()
            if exch_user is not None:
                smtp = exch_user.PrimarySmtpAddress
                if smtp and "@" in smtp:
                    return smtp.strip()
    except Exception as exc:
        log.debug("GetExchangeUser resolution failed: %s", exc)

    # Method 2: PropertyAccessor with PR_SMTP_ADDRESS
    PR_SMTP_ADDRESS = "http://schemas.microsoft.com/mapi/proptag/0x39FE001E"
    try:
        smtp = item.PropertyAccessor.GetProperty(PR_SMTP_ADDRESS)
        if smtp and "@" in str(smtp):
            return str(smtp).strip()
    except Exception as exc:
        log.debug("PropertyAccessor SMTP resolution failed: %s", exc)

    return None


# ------------------------------------------------------------------
# Timezone helper
# ------------------------------------------------------------------
def _make_cutoff_tz_safe(cutoff, received_dt):
    """Return cutoff adjusted so it can be compared with *received_dt*.

    Outlook COM returns pywintypes.datetime which may or may not carry
    timezone info depending on the Exchange server config.  We need
    both sides to match.
    """
    if received_dt is None:
        return cutoff

    rcv_aware = hasattr(received_dt, 'tzinfo') and received_dt.tzinfo is not None
    cut_aware = hasattr(cutoff, 'tzinfo') and cutoff.tzinfo is not None

    if rcv_aware and not cut_aware:
        # Make cutoff aware using the same tz as received
        try:
            cutoff = cutoff.replace(tzinfo=received_dt.tzinfo)
        except Exception:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
    elif not rcv_aware and cut_aware:
        cutoff = cutoff.replace(tzinfo=None)

    return cutoff


# ------------------------------------------------------------------
# OutlookReader
# ------------------------------------------------------------------
class OutlookReader:
    def __init__(self, mailbox, folder, days, max_items, subfolder_days=None):
        self.mailbox = mailbox
        if isinstance(folder, list):
            self.folders = folder
        else:
            self.folders = [folder]
        self.folder = self.folders[0]  # backward compat
        self.days = days
        self.subfolder_days = subfolder_days
        self.max_items = max_items
        # Store raw COM items keyed by entry_id for later attachment downloading
        self._raw_items = {}

    # ---------------------------------------------------------------
    # Folder resolution
    # ---------------------------------------------------------------
    def _resolve_folder(self, store, folder_name):
        """Resolve a folder by name, supporting nested paths like
        'Inbox/Operating reports'.  '/' is the separator.
        Falls back to case-insensitive match at each level.
        """
        parts = folder_name.split("/")
        current = store
        for part in parts:
            part = part.strip()
            if not part:
                continue
            found = None
            try:
                found = current.Folders[part]
            except Exception:
                # Case-insensitive fallback
                try:
                    for i in range(1, current.Folders.Count + 1):
                        f = current.Folders.Item(i)
                        if f.Name.lower() == part.lower():
                            found = f
                            break
                except Exception:
                    pass
            if found is None:
                try:
                    avail = [current.Folders.Item(i).Name
                             for i in range(1, current.Folders.Count + 1)]
                except Exception:
                    avail = ["(unable to list)"]
                log.warning("Folder '%s' not found under '%s' in mailbox '%s'. Available: %s",
                            part, getattr(current, 'Name', self.mailbox),
                            self.mailbox, avail)
                return None
            current = found
        return current

    # ---------------------------------------------------------------
    # Public entry point
    # ---------------------------------------------------------------
    def fetch_messages(self):
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        store = None
        target = self.mailbox.strip().lower()
        for s in outlook.Folders:
            # Match by exact name (case-insensitive) or by store email address
            if s.Name.lower() == target:
                store = s
                break
            try:
                store_email = s.GetRootFolder().Store.DisplayName or ""
                if store_email.lower() == target:
                    store = s
                    break
            except Exception:
                pass
        # Fallback: partial / substring match (e.g. "chip" matches "Chip Ridge")
        if not store:
            for s in outlook.Folders:
                if target in s.Name.lower():
                    log.info("Fuzzy-matched mailbox '%s' → store '%s'", self.mailbox, s.Name)
                    store = s
                    break
        if not store:
            log.error("Mailbox '%s' not found. Available stores: %s",
                      self.mailbox, [s.Name for s in outlook.Folders])
            return []

        all_messages = []
        for folder_name in self.folders:
            is_subfolder = '/' in folder_name
            effective_days = (self.subfolder_days or self.days) if is_subfolder else self.days
            log.info("Scanning folder: '%s' (days=%d)", folder_name, effective_days)

            try:
                folder_msgs = self._fetch_from_folder(store, folder_name,
                                                      days_override=effective_days)
                all_messages.extend(folder_msgs)
                log.info("Folder '%s': fetched %d messages", folder_name, len(folder_msgs))
            except Exception as exc:
                log.error("FOLDER CRASH '%s': %s - skipping folder", folder_name, exc)

            # --- COM memory pressure relief between folders ---
            gc.collect()

        log.info("Total messages from %d folder(s): %d", len(self.folders), len(all_messages))
        return all_messages

    # ---------------------------------------------------------------
    # Per-folder fetch (index-based iteration + circuit breaker)
    # ---------------------------------------------------------------
    def _fetch_from_folder(self, store, folder_name, days_override=None):
        """Fetch messages from a single folder using safe index-based iteration."""
        folder = self._resolve_folder(store, folder_name)
        if folder is None:
            return []

        is_sent = folder_name.lower() in ("sent items", "sent")
        effective_days = days_override if days_override is not None else self.days
        cutoff = datetime.now() - timedelta(days=effective_days)

        # Sort items (newest first) so we can break on old dates
        sort_field = "SentOn" if is_sent else "ReceivedTime"
        items = folder.Items
        try:
            items.Sort(sort_field, True)
        except Exception:
            log.warning("Sort by '%s' failed for '%s', falling back to ReceivedTime",
                        sort_field, folder_name)
            try:
                items.Sort("ReceivedTime", True)
            except Exception:
                log.warning("Sort fallback also failed for '%s'", folder_name)

        total_count = 0
        try:
            total_count = items.Count
        except Exception:
            log.warning("Cannot get item count for '%s'", folder_name)
            return []

        if total_count == 0:
            log.info("OutlookReader summary [%s]: total_items_seen=0, mail_items_kept=0, "
                     "skipped_non_mail=0, skipped_missing_datetime=0, skipped_exceptions=0",
                     folder_name)
            return []

        log.info("Folder '%s': %d total items to scan", folder_name, total_count)

        # Calibrate cutoff timezone from the first accessible item
        cutoff = self._calibrate_cutoff(items, total_count, cutoff)

        messages = []
        kept = 0
        total_seen = 0
        skipped_non_mail = 0
        skipped_missing_dt = 0
        skipped_exceptions = 0
        consecutive_errors = 0

        # Use 1-based index (COM collections are 1-based)
        for idx in range(1, total_count + 1):
            if kept >= self.max_items:
                break

            # --- Circuit breaker: too many consecutive errors -> bail ---
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                log.warning("Circuit breaker: %d consecutive errors in '%s' at item %d/%d - "
                            "stopping folder scan",
                            consecutive_errors, folder_name, idx, total_count)
                break

            # --- Safe item access ---
            try:
                item = items.Item(idx)
            except pywintypes.com_error as exc:
                skipped_exceptions += 1
                consecutive_errors += 1
                if consecutive_errors <= 3:
                    log.debug("COM error accessing item %d in '%s': %s",
                              idx, folder_name, exc)
                continue
            except Exception as exc:
                skipped_exceptions += 1
                consecutive_errors += 1
                if consecutive_errors <= 3:
                    log.debug("Error accessing item %d in '%s': %s",
                              idx, folder_name, exc)
                continue

            total_seen += 1
            consecutive_errors = 0  # reset on successful access

            try:
                msg = self._process_item(item, folder_name, is_sent, cutoff)
                if msg is None:
                    item_class = getattr(item, 'Class', None)
                    if item_class != 43:
                        skipped_non_mail += 1
                    else:
                        skipped_missing_dt += 1
                    continue
                if msg == "STOP":
                    break

                messages.append(msg)
                entry_id = msg.get("entry_id")
                if entry_id:
                    self._raw_items[entry_id] = item
                kept += 1

            except TypeError as exc:
                # Catch "can't compare offset-naive and offset-aware datetimes"
                if "offset-naive" in str(exc) or "offset-aware" in str(exc):
                    received = getattr(item, 'ReceivedTime', None) or getattr(item, 'SentOn', None)
                    cutoff = _make_cutoff_tz_safe(cutoff, received)
                    try:
                        msg = self._process_item(item, folder_name, is_sent, cutoff)
                        if msg and msg != "STOP":
                            messages.append(msg)
                            entry_id = msg.get("entry_id")
                            if entry_id:
                                self._raw_items[entry_id] = item
                            kept += 1
                        elif msg == "STOP":
                            break
                    except Exception:
                        skipped_exceptions += 1
                else:
                    skipped_exceptions += 1
                    log.debug("TypeError processing item %d in '%s': %s", idx, folder_name, exc)

            except Exception as exc:
                skipped_exceptions += 1
                log.debug("Exception processing item %d in '%s': %s", idx, folder_name, exc)

        log.info("OutlookReader summary [%s]: total_items_seen=%d, mail_items_kept=%d, "
                 "skipped_non_mail=%d, skipped_missing_datetime=%d, skipped_exceptions=%d",
                 folder_name, total_seen, kept, skipped_non_mail,
                 skipped_missing_dt, skipped_exceptions)
        return messages

    # ---------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------
    def _calibrate_cutoff(self, items, total_count, cutoff):
        """Try to read timezone from the first accessible item and align cutoff."""
        for probe_idx in range(1, min(6, total_count + 1)):
            try:
                probe = items.Item(probe_idx)
                received = getattr(probe, 'ReceivedTime', None) or getattr(probe, 'SentOn', None)
                if received is not None:
                    return _make_cutoff_tz_safe(cutoff, received)
            except Exception:
                continue
        return cutoff

    def _process_item(self, item, folder_name, is_sent, cutoff):
        """Extract message dict from a COM MailItem.

        Returns:
          dict   - successfully extracted message
          None   - skipped (non-mail, missing datetime)
          "STOP" - item is older than cutoff, caller should break
        """
        item_class = getattr(item, 'Class', None)
        if item_class != 43:
            return None

        received = getattr(item, 'ReceivedTime', None)
        if received is None:
            received = getattr(item, 'SentOn', None)
            if received is not None:
                log.debug("MailItem missing ReceivedTime, using SentOn. Subject=%s",
                          getattr(item, 'Subject', None))
        if received is None:
            return None

        # Ensure timezone-safe comparison
        safe_cutoff = _make_cutoff_tz_safe(cutoff, received)
        if received < safe_cutoff:
            return "STOP"

        # --- Extract fields ---
        subj = getattr(item, 'Subject', None)
        att_meta = self._get_attachment_meta(item)
        has_attachments = len(att_meta) > 0
        att_names = ";".join(a["name"] for a in att_meta)
        kpi_exts = {".xlsx", ".xls", ".csv", ".pdf", ".docx"}
        has_kpi_attachment = any(a["ext"] in kpi_exts for a in att_meta)

        entry_id = getattr(item, 'EntryID', None)
        raw_sender_email = getattr(item, 'SenderEmailAddress', None)

        resolved_smtp = _resolve_smtp_address(item)
        if resolved_smtp:
            log.debug("Resolved Exchange DN to SMTP: %s -> %s",
                      (raw_sender_email or "")[:60], resolved_smtp)
        sender_email = resolved_smtp or raw_sender_email

        recipients_to = ""
        if is_sent:
            try:
                recipients_to = getattr(item, 'To', '') or ""
            except Exception:
                pass

        body = ""
        try:
            body = getattr(item, 'Body', '') or ""
        except Exception:
            pass

        conv_topic = ""
        try:
            conv_topic = getattr(item, 'ConversationTopic', '') or ""
        except Exception:
            pass

        msg = {
            'entry_id': entry_id,
            'subject': subj,
            'sender_name': getattr(item, 'SenderName', None),
            'sender_email': sender_email,
            'received_dt': str(received),
            'body': body[:50000],
            'has_attachments': has_attachments,
            'has_kpi_attachment': has_kpi_attachment,
            'attachment_names': att_names,
            'attachment_meta': att_meta,
            'conversation_topic': conv_topic,
            'source_folder': folder_name,
            'recipients_to': recipients_to,
        }
        return msg

    def get_raw_item(self, entry_id):
        """Return the raw COM MailItem for an entry_id (for attachment downloading)."""
        return self._raw_items.get(entry_id)

    @staticmethod
    def _get_attachment_meta(item):
        """Return list of {name, ext, size} for item's attachments."""
        result = []
        try:
            att_count = item.Attachments.Count
            for idx in range(1, att_count + 1):
                att = item.Attachments.Item(idx)
                name = getattr(att, "FileName", f"attachment_{idx}")
                ext = os.path.splitext(name)[1].lower()
                size = getattr(att, "Size", 0)
                result.append({"name": name, "ext": ext, "size": size})
        except Exception:
            pass
        return result
