import os
import smtplib
from email.message import EmailMessage
import DataAnalyser


class EmailHandler:
    dataAnalyser = DataAnalyser.DataAnalyser()

    def __init__(self, contacts):
        self.report = self.dataAnalyser.getReport()
        self.EMAIL_ADDRESS = os.environ.get('EMAIL_USER')
        self.EMAIL_PASSWORD = os.environ.get('EMAIL_PASS')
        self.contacts = contacts

    def sendEmail(self):
        msg = EmailMessage()
        msg['Subject'] = 'REDCap Soweto Report'
        msg['From'] = self.EMAIL_ADDRESS
        msg['To'] = ', '.join(self.contacts)

        msg.set_content("This is the content in the report")

        files = [self.report, self.dataAnalyser.getBoxPlot(), self.dataAnalyser.getHeatMap()]

        for file in files:
            with open(file, 'rb') as f:
                fileData = f.read()
                fileName = f.name

                msg.add_attachment(fileData, maintype='application', subtype='octet-stream', filename=fileName)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(self.EMAIL_ADDRESS, self.EMAIL_PASSWORD)
            smtp.send_message(msg)