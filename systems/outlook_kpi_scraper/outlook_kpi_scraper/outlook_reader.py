import win32com.client
import logging
import os
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


def _resolve_smtp_address(item) -> str | None:
    """Try to resolve Exchange DN sender addresses to a real SMTP address.

    Exchange stores internal senders as DN strings like:
      /O=EXCHANGELABS/OU=.../CN=RECIPIENTS/CN=...
    This uses the Outlook COM object model to resolve to the real SMTP
    address via Sender.GetExchangeUser().PrimarySmtpAddress.
    Falls back to PropertyAccessor PR_SMTP_ADDRESS if the first method fails.
    Returns None if resolution fails.
    """
    # Only attempt resolution for Exchange-type senders
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


class OutlookReader:
    def __init__(self, mailbox, folder, days, max_items, subfolder_days=None):
        self.mailbox = mailbox
        # Support single folder (str) or multiple folders (list)
        if isinstance(folder, list):
            self.folders = folder
        else:
            self.folders = [folder]
        self.folder = self.folders[0]  # backward compat
        self.days = days
        self.subfolder_days = subfolder_days  # optional longer lookback for nested folders
        self.max_items = max_items
        # Store raw COM items keyed by entry_id for later attachment downloading
        self._raw_items = {}

    def _resolve_folder(self, store, folder_name):
        """Resolve a folder by name, supporting nested paths like 'Inbox/Operating reports'.

        Uses '/' as a separator to navigate into subfolders.
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

    def fetch_messages(self):
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        store = None
        for s in outlook.Folders:
            if s.Name == self.mailbox:
                store = s
                break
        if not store:
            log.error("Mailbox '%s' not found. Available stores: %s",
                      self.mailbox, [s.Name for s in outlook.Folders])
            return []

        all_messages = []
        for folder_name in self.folders:
            # Use subfolder_days for nested folder paths (contains '/')
            is_subfolder = '/' in folder_name
            effective_days = (self.subfolder_days or self.days) if is_subfolder else self.days
            log.info("Scanning folder: '%s' (days=%d)", folder_name, effective_days)
            folder_msgs = self._fetch_from_folder(store, folder_name, days_override=effective_days)
            all_messages.extend(folder_msgs)
            log.info("Folder '%s': fetched %d messages", folder_name, len(folder_msgs))

        log.info("Total messages from %d folder(s): %d", len(self.folders), len(all_messages))
        return all_messages

    def _fetch_from_folder(self, store, folder_name, days_override=None):
        """Fetch messages from a single folder."""
        folder = self._resolve_folder(store, folder_name)
        if folder is None:
            return []

        is_sent = folder_name.lower() in ("sent items", "sent")
        effective_days = days_override if days_override is not None else self.days
        now = datetime.now()
        items = folder.Items
        try:
            first_item = items[0]
            received_sample = getattr(first_item, 'ReceivedTime', None)
            if received_sample and hasattr(received_sample, 'tzinfo') and received_sample.tzinfo is not None:
                now = now.replace(tzinfo=received_sample.tzinfo)
        except Exception:
            pass
        cutoff = now - timedelta(days=effective_days)

        # Sent Items uses SentOn for sorting; others use ReceivedTime
        sort_field = "SentOn" if is_sent else "ReceivedTime"
        items = folder.Items
        try:
            items.Sort(sort_field, True)
        except Exception:
            log.warning("Sort by '%s' failed for folder '%s', falling back to ReceivedTime",
                        sort_field, folder_name)
            try:
                items.Sort("ReceivedTime", True)
            except Exception:
                log.warning("Sort fallback also failed for folder '%s'", folder_name)

        messages = []
        count = 0
        total_items_seen = 0
        mail_items_kept = 0
        skipped_non_mail = 0
        skipped_missing_datetime = 0
        skipped_exceptions = 0
        for item in items:
            total_items_seen += 1
            if count >= self.max_items:
                break
            try:
                item_class = getattr(item, 'Class', None)
                subj = getattr(item, 'Subject', None)
                if item_class != 43:
                    log.debug("Skipping non-MailItem: Class=%s Subject=%s", item_class, subj)
                    skipped_non_mail += 1
                    continue
                received = getattr(item, 'ReceivedTime', None)
                if received is None:
                    received = getattr(item, 'SentOn', None)
                    import pywintypes
                    for idx in range(len(items)):
                        try:
                            item = items[idx]
                        except pywintypes.com_error as e:
                            log.error(f"COM error accessing item {idx}: {e}. Skipping item.")
                            skipped_exceptions += 1
                            continue
                        except Exception as e:
                            log.error(f"General error accessing item {idx}: {e}. Skipping item.")
                            skipped_exceptions += 1
                            continue
                        total_items_seen += 1
                        if count >= self.max_items:
                            break
                        try:
                            item_class = getattr(item, 'Class', None)
                            subj = getattr(item, 'Subject', None)
                            if item_class != 43:
                                log.debug("Skipping non-MailItem: Class=%s Subject=%s", item_class, subj)
                                skipped_non_mail += 1
                                continue
                            received = getattr(item, 'ReceivedTime', None)
                            if received is None:
                                received = getattr(item, 'SentOn', None)
                                if received is not None:
                                    log.debug("MailItem missing ReceivedTime, using SentOn. Subject=%s", subj)
                            if received is None:
                                log.debug("Skipping MailItem missing both ReceivedTime and SentOn. Subject=%s", subj)
                                skipped_missing_datetime += 1
                                continue
                            if received < cutoff:
                                break
                            try:
                                # Collect attachment metadata
                                att_meta = self._get_attachment_meta(item)
                                has_attachments = len(att_meta) > 0
                                att_names = ";".join(a["name"] for a in att_meta)
                                kpi_exts = {".xlsx", ".xls", ".csv", ".pdf", ".docx"}
                                has_kpi_attachment = any(a["ext"] in kpi_exts for a in att_meta)

                                entry_id = getattr(item, 'EntryID', None)
                                raw_sender_email = getattr(item, 'SenderEmailAddress', None)
                                # Resolve Exchange DN to real SMTP address
                                resolved_smtp = _resolve_smtp_address(item)
                                if resolved_smtp:
                                    log.debug("Resolved Exchange DN to SMTP: %s → %s",
                                              (raw_sender_email or "")[:60], resolved_smtp)
                                sender_email = resolved_smtp or raw_sender_email
                                # For Sent Items, capture recipients
                                recipients_to = ""
                                if is_sent:
                                    try:
                                        recipients_to = getattr(item, 'To', '') or ""
                                    except Exception:
                                        pass
                        'source_folder': folder_name,
                        'recipients_to': recipients_to,
                    }
                    messages.append(msg)
                    # Keep COM reference for later attachment downloading
                    if entry_id:
                        self._raw_items[entry_id] = item
                    count += 1
                    mail_items_kept += 1
                except Exception as e:
                    log.debug("Exception extracting fields from MailItem: Subject=%s Error=%s", subj, e)
                    skipped_exceptions += 1
            except Exception as e:
                log.debug("Exception iterating item: %s", e)
                skipped_exceptions += 1
        log.info("OutlookReader summary [%s]: total_items_seen=%d, mail_items_kept=%d, "
                 "skipped_non_mail=%d, skipped_missing_datetime=%d, skipped_exceptions=%d",
                 folder_name, total_items_seen, mail_items_kept, skipped_non_mail,
                 skipped_missing_datetime, skipped_exceptions)
        return messages

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
