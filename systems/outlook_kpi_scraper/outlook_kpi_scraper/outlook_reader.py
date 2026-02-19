import win32com.client
import logging
from datetime import datetime, timedelta

class OutlookReader:
    def __init__(self, mailbox, folder, days, max_items):
        self.mailbox = mailbox
        self.folder = folder
        self.days = days
        self.max_items = max_items

    def fetch_messages(self):
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        store = None
        for s in outlook.Folders:
            if s.Name == self.mailbox:
                store = s
                break
        if not store:
            logging.error(f"Mailbox '{self.mailbox}' not found. Available stores: {[s.Name for s in outlook.Folders]}")
            return []
        folder = store.Folders[self.folder]
        cutoff = datetime.now() - timedelta(days=self.days)
        items = folder.Items
        items.Sort("ReceivedTime", True)
        messages = []
        count = 0
        for item in items:
            if count >= self.max_items:
                break
            received = item.ReceivedTime
            if received < cutoff:
                break
            msg = {
                'subject': item.Subject,
                'sender_name': getattr(item, 'SenderName', None),
                'sender_email': getattr(item, 'SenderEmailAddress', None),
                'received_dt': received.strftime('%Y-%m-%dT%H:%M:%S'),
                'body': getattr(item, 'Body', ''),
                'entry_id': item.EntryID,
                'internet_message_id': getattr(item, 'InternetMessageID', None)
            }
            messages.append(msg)
            count += 1
        return messages
