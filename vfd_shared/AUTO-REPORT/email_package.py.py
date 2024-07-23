# Databricks notebook source

class EmailOperations:
    def send_mail(filenames, SourcePathName, date, recipients, subject, name_of_recipient, body):
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.base import MIMEBase
        from email.mime.text import MIMEText
        from email import encoders
        
        msg = MIMEMultipart()
        msg['From'] = 'auto-report@vfdtech.ng'
        msg['To'] = recipients
        msg['Subject'] = f'{subject}'
        body = f"""
        Dear {name_of_recipient},

        {body}.

        Regards,
        """
        msg.attach(MIMEText(body, 'plain'))

        ## ATTACHMENT PART OF THE CODE IS HERE
        #file name to be sent
        

        #open the file path in read binary mode
        for file_name in filenames:
            file = open(SourcePathName + file_name, mode="rb")

            #mimebase object with maintype and subtype
            part = MIMEBase('application', 'octet-stream')

            #to change the payload into encoded form
            part.set_payload((file).read())

            #encode into base64
            encoders.encode_base64(part)

            #attach object mimebase into mimemultipart object
            part.add_header('Content-Disposition', f"attachment; filename = {file_name}")
            msg.attach(part)


        server = smtplib.SMTP('smtp.office365.com', 587)  ### put your relevant SMTP here
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login('Babatunde.Omotayo@vfdtech.ng', 'wmglfvqvnhdqgrrc')  ### if applicable
        server.send_message(msg)
        server.quit()
        print(f"Mail sent to {name_of_recipient} successfully!\n")
