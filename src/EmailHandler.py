import os
import smtplib
from email.message import EmailMessage


class EmailHandler:

    def __init__(self, contacts, attachments):
        self.EMAIL_ADDRESS = os.environ.get('EMAIL_USER')
        self.EMAIL_PASSWORD = os.environ.get('EMAIL_PASS')
        # list of contacts
        self.contacts = contacts
        # list of files to attach
        self.attachments = attachments

    def send_email(self):
        msg = EmailMessage()
        msg['Subject'] = 'REDCap Soweto Report'
        msg['From'] = self.EMAIL_ADDRESS
        msg['To'] = ', '.join(self.contacts)

        msg.set_content("This is the content in the report")

        for file in self.attachments:
            with open(file, 'rb') as f:
                file_data = f.read()
                _, file_name = os.path.split(f.name)

                msg.add_attachment(file_data,
                                   maintype='application',
                                   subtype='octet-stream',
                                   filename=file_name)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(self.EMAIL_ADDRESS, self.EMAIL_PASSWORD)
            smtp.send_message(msg)