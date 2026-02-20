import win32com.client
import logging
import os
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


class OutlookReader:
    def __init__(self, mailbox, folder, days, max_items):
        self.mailbox = mailbox
        self.folder = folder
        self.days = days
        self.max_items = max_items
        # Store raw COM items keyed by entry_id for later attachment downloading
        self._raw_items = {}

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
        folder = store.Folders[self.folder]
        now = datetime.now()
        items = folder.Items
        try:
            first_item = items[0]
            received_sample = getattr(first_item, 'ReceivedTime', None)
            if received_sample and hasattr(received_sample, 'tzinfo') and received_sample.tzinfo is not None:
                now = now.replace(tzinfo=received_sample.tzinfo)
        except Exception:
            pass
        cutoff = now - timedelta(days=self.days)
        items = folder.Items
        items.Sort("ReceivedTime", True)
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
                    msg = {
                        'subject': subj,
                        'sender_name': getattr(item, 'SenderName', None),
                        'sender_email': getattr(item, 'SenderEmailAddress', None),
                        'received_dt': received.strftime('%Y-%m-%dT%H:%M:%S'),
                        'body': getattr(item, 'Body', ''),
                        'entry_id': entry_id,
                        'internet_message_id': getattr(item, 'InternetMessageID', None),
                        'has_attachments': has_attachments,
                        'has_kpi_attachment': has_kpi_attachment,
                        'attachment_names': att_names,
                        'attachment_meta': att_meta,
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
        log.info("OutlookReader summary: total_items_seen=%d, mail_items_kept=%d, "
                 "skipped_non_mail=%d, skipped_missing_datetime=%d, skipped_exceptions=%d",
                 total_items_seen, mail_items_kept, skipped_non_mail,
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
