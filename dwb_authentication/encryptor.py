import logging
import logging.config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('dwb_authentication')
logger_query = logging.getLogger('query')
import bcrypt
from cryptography.fernet import Fernet

import pudb
import re


class Encryptor():
	"""
	Encrypt the password with a newly generated key. The key gets salted and extended by padding 
	and is then used as a token. Token and encrypted password are stored as session in a mysql database 
	- together with database connection parameters - so that they can be used to connect to the DWB database
	as long as the session has not been expired
	"""
	def __init__(self):
		self.salt_length = 29
		self.key_length = 44
		
		self.paddingpattern = re.compile(r'(\=+)$')
	
	
	def __add_padding(self, keystring):
		keystring = keystring.ljust(self.key_length, '=')
		return keystring
	
	def __remove_padding(self, keystring):
		keystring = re.sub(self.paddingpattern, '', keystring)
		return keystring
	
	def get_key_from_token(self, token):
		key = token[self.salt_length:]
		# add padding characters
		key = self.__add_padding(key)
		key = key.encode('utf-8')
		return key
	
	def get_salt_from_token(self, token):
		salt = token[:self.salt_length]
		return salt
	
	def create_token_from_key(self, keystring):
		'''
		add salt to the key
		'''
		key = keystring.decode('utf-8')
		paddingpattern = re.compile(r'')
		salt = bcrypt.gensalt()
		if isinstance(salt, bytes):
			salt = salt.decode('utf-8')
		key = self.__remove_padding(key)
		token = salt + key
		return token
	
	
	def encrypt_password(self, password):
		key = Fernet.generate_key()
		fernet_encryptor = Fernet(key)
		encrypted_pw = fernet_encryptor.encrypt(password.encode('utf-8'))
		return (encrypted_pw, key)
	
	def decrypt_password(self, encrypted_pw, key):
		if encrypted_pw is None:
			return None
		
		if not isinstance(encrypted_pw, bytes):
			encrypted_pw = encrypted_pw.encode('utf-8')
		
		fernet_encryptor = Fernet(key)
		pw = fernet_encryptor.decrypt(encrypted_pw).decode('utf-8')
		return pw
	
	def hash_token(self, token):
		salt = self.get_salt_from_token(token)
		key = self.get_key_from_token(token)
		if not isinstance(salt, bytes):
			salt = salt.encode('utf8')
		if not isinstance(key, bytes):
			key = key.encode('utf8')
		try:
			hashed_token = bcrypt.hashpw(key, salt)
		except:
			# check if token contains a salt that is not useable.
			# there must be a much better implementation to throw away broken tokens and nonsense tokens 
			hashed_token = bcrypt.hashpw(key)
		
		return hashed_token
	

