import os
import re
import pudb


class ConfigSetter():
	def __init__(self, sourcefile, targetfile):
		self.sourcefile = sourcefile
		self.targetfile = targetfile
		self.env_variables = {
		'VERIFY_HTTPS': 'True',
		'ACCNR_PREFIX': None,
		'SESSION_DB_HOST': None,
		'SESSION_DB_CHARSET': None,
		'SESSION_DB_USER': None,
		'SESSION_DB_PASSWORD': None,
		'SESSION_DB_DATABASE': None,
		'SESSION_DB_PORT': None,
		'SESSION_DB_EXPIRATION_TIME': None,
		'DC_DB_ACCRONYM': None,
		'DC_DB_SERVER': None,
		'DC_DB_DATABASE': None,
		'DC_DB_PORT': None,
		'DC_DB_DRIVER': None,
		'DC_DB_UID': None,
		'DC_DB_PASSWORD': None,
		'DC_DB_COLLATION': None,
		'DC_DB_TRUSTED_CONNECTION': None,
		'DC_DB_ENCRYPT': None,
		'DC_DB_TRUST_CERTIFICATE': None,
		'SMTP_SERVER': None,
		'SMTP_SENDER': None,
		'SMTP_USER': None,
		'SMTP_PASSWORD': None,
		'SMTP_PORT': None,
		'DEVELOPERS_MAIL': None,
		}
	
	
	def set_config(self):
		self.read_env_variables()
		self.set_config_parameters()
	
	
	def read_env_variables(self):
		for variable in self.env_variables:
			self.env_variables[variable] = os.getenv(variable, None)
	
	def set_config_parameters(self):
		with open(self.sourcefile, 'r') as fh_source:
			filecontent = fh_source.read()
			
			for variable in self.env_variables:
				if self.env_variables[variable] is not None:
					replacestring = '@@{0}@@'.format(variable)
					filecontent = filecontent.replace(replacestring, self.env_variables[variable])
		
		with open(self.targetfile, 'w') as fh_target:
			fh_target.write(filecontent)


if __name__ == '__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument("config_template")
	parser.add_argument("config_file")
	args = parser.parse_args()
	configsetter = ConfigSetter(args.config_template, args.config_file)
	configsetter.set_config()
	



