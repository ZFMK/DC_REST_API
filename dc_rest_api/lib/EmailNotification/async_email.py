"""
http://blog.miguelgrinberg.com/post/the-flask-mega-tutorial-part-xi-email-support
"""
import pudb
import sys
import smtplib
from os.path import basename
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

import re

from datetime import datetime

from dc_rest_api.lib.decorators import asyncfunc

from configparser import ConfigParser
config = ConfigParser(allow_no_value=True)
config.read('./config.ini')

smtp_server = config.get('mail_service', 'smtp_server', fallback = None)
# for external smtp server
smtp_sender = config.get('mail_service', 'smtp_sender', fallback = None)
smtp_user = config.get('mail_service', 'smtp_user', fallback = None)
smtp_passwd = config.get('mail_service', 'smtp_password', fallback = None)
smtp_port = config.get('mail_service', 'smtp_port', fallback = None)



now = datetime.now()
datestring = now.strftime("%a, %d %b %Y %I:%M:%S%p %Z")

@asyncfunc
def send_async_email(msg):
	if smtp_user is not None and smtp_passwd is not None and smtp_port is not None:
		s = smtplib.SMTP(smtp_server, smtp_port)
		s.starttls()
		s.login(smtp_user, smtp_passwd)
	else:
		s = smtplib.SMTP(smtp_server)
	
	s.sendmail(msg['From'], msg['To'], msg.as_string())
	s.quit()


def notify_developers(text):
	developers = config.get('mail_service', 'dev_group', fallback = None)
	if developers:
		developers = [developer_mail.strip() for developer_mail in re.split('r[,;]', developers)]
		for developer in developers:
			msg = MIMEText(text)
			msg['Subject'] = 'DC_REST_API Error'
			msg['From'] = smtp_sender
			msg['To'] = developer
			msg['Date'] = datestring
			if smtp_server is not None:
				send_async_email(msg)
	return


def send_mail(mail_to, header, text):
	msg = MIMEText(text)
	msg['Subject'] = header
	msg['From'] = smtp_sender
	msg['To'] = mail_to
	msg['Date'] = datestring
	if smtp_server is not None:
		send_async_email(msg)
	return


def send_mail_with_attachment(mail_to, header, text, file, desired_file_name):
	msg = MIMEMultipart()
	msg['Subject'] = header
	msg['From'] = smtp_sender
	msg['To'] = mail_to
	msg['Date'] = datestring
	
	msg.attach(MIMEText(text))
	
	file_base_name = basename(file)
	with open(file, 'rb') as file_to_attach:
		attachment = MIMEApplication(file_to_attach.read(), Name=file_base_name)
	#attachment['Content-Disposition'] = 'attachment; filename="{0}"'.format(file_base_name)
	attachment['Content-Disposition'] = 'attachment; filename="{0}"'.format(desired_file_name)
	
	msg.attach(attachment)
	if smtp_server is not None:
		send_async_email(msg)
	return
