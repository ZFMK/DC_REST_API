import logging
import logging.config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('dwb_authentication')
logger_query = logging.getLogger('query')

import pudb
import string
#import secrets

from configparser import ConfigParser
config = ConfigParser(allow_no_value=True)
config.read('./config.ini')

from .encryptor import Encryptor

from DBConnectors.MSSQLConnector import MSSQLConnectionParams

from dwb_authentication.mysql_connect import mysql_connect
from dwb_authentication.DWB_Servers import DWB_Servers
from dwb_authentication.DWB_Account import DWB_Account


'''
# think about encrypting the token with a key that is valid as long as the server is running
# currently, the token is the key to decrypt users password on DWB, it is stored in servers SessionAuthenticationPolicy
# and hashed to store and compare it with entry in database table session

def get_onetime_secret():
	alphabet = string.ascii_letters + string.digits
	secret = ''.join(secrets.choice(alphabet) for i in range(8))
	return secret
'''


class DBSession():
	def __init__(self):
		self.dwb_servers = DWB_Servers()
		self.encryptor = Encryptor()
		self.expiration_interval = config.get('session_db', 'expiration_time', fallback = 20)
		
		self.con, self.cur = mysql_connect()
		
		# delete all expired sessions, regardless to whom they belong
		self.delete_old_sessions()


	def __check_hashed_token_exists(self, hashed_token):
		query = """
		SELECT s.`hashed_token`
		FROM sessions s
		WHERE s.hashed_token = %s 
		"""
		self.cur.execute(query, [hashed_token])
		row = self.cur.fetchone()
		if row is not None:
			return True
		else:
			return False


	def get_mssql_connectionparams_by_token(self, token):
		hashed_token = self.encryptor.hash_token(token)
		
		conparams = {}
		query = """
		SELECT mc.`db_connector_id`, mc.`accronym`, mc.`server`, mc.`port`, mc.`database`,mc. `driver`, 
		mc.`trust_certificate`, mc.`trusted_connection`, mc.`encrypt`, mc.`collation`
		FROM `mssql_connectors` mc
		INNER JOIN `sessions` s ON s.`db_connector_id` = mc.`db_connector_id`
		WHERE s.`hashed_token` = %s
		;"""

		#logger_query.info(query)
		#logger_query.info(hashed_token)
                
		self.cur.execute(query, [hashed_token])
		row = self.cur.fetchone()
		
		if row is not None:
			dwb_conparams = {
				'accronym': row[1],
				'server': row[2],
				'port': row[3],
				'database': row[4],
				'driver': row[5],
				'trust_certificate': row[6],
				'trusted_connection': row[7],
				'encrypt': row[8],
				'collation': row[9],
			}
			return dwb_conparams
		return None


	def set_session(self, username, password, connector_accronym = None):
		if connector_accronym is None:
			dwb_con = self.dwb_servers.get_default_connector()
		
		else:
			dwb_con = self.dwb_servers.get_dwb_con_by_accronym(connector_accronym)
		
		if dwb_con is None:
			return None
		
		dwb_con['username'] = username
		dwb_con['password'] = password
		
		dwb_account = DWB_Account(dwb_con)
		if dwb_account.isValid() is False:
			return None
		
		encrypted_pw, key = self.encryptor.encrypt_password(password)
		token = self.encryptor.create_token_from_key(key)
		
		hashed_token = self.encryptor.hash_token(token)
		hashed_token_exists = self.__check_hashed_token_exists(hashed_token)
		
		# guaranty that the randomly generated hashed_token does not exits
		# no token should occur twice at the same time
		while (hashed_token_exists is True):
			token = self.encryptor.create_token(key)
			hashed_token = self.encryptor.hash_token(token)
			hashed_token_exists = self.__check_hashed_token_exists(hashed_token)
		
		query = """
		INSERT INTO `sessions`
		(hashed_token, encrypted_passwd, `username`, db_connector_id, expiration_time)
		VALUES(%s, %s, %s, %s, NOW() + INTERVAL {0} MINUTE)
		;""".format(self.expiration_interval)
		
		self.cur.execute(query, [hashed_token, encrypted_pw, username, dwb_con['db_connector_id']])
		self.con.commit()
		self.__set_session_id(hashed_token)
		
		roles = dwb_account.server_roles
		self.insert_dwb_roles(roles)
		
		projects = dwb_account.users_projects
		self.insert_projects(projects)
		
		return token


	def __set_session_id(self, hashed_token):
		query = """
		SELECT session_id
		FROM `sessions`
		WHERE hashed_token = %s
		;"""
		self.cur.execute(query, [hashed_token])
		row = self.cur.fetchone()
		if row is not None:
			self.session_id = row[0]
		else:
			self.session_id = None


	def insert_dwb_roles(self, roles = []):
		if len(roles)> 0:
			values = []
			placeholders = []
			for role in roles:
				values.extend([role, self.session_id])
				placeholders.append('(%s, %s)')
			placeholderstring = ', '.join(placeholders)
			query = """
			INSERT INTO `dwb_roles`
			(`users_dwb_role`, `session_id`)
			VALUES {0}
			;""".format(placeholderstring)
			
			self.cur.execute(query, values)
			self.con.commit()
		return


	def insert_projects(self, projects = []):
		if len(projects)> 0:
			values = []
			placeholders = []
			for project in projects:
				values.extend([project[0], project[1], self.session_id])
				placeholders.append('(%s, %s, %s)')
			placeholderstring = ', '.join(placeholders)
			query = """
			INSERT INTO `dwb_projects`
			(`users_dwb_project_id`, `project_name`, `session_id`)
			VALUES {0}
			;""".format(placeholderstring)
			
			self.cur.execute(query, values)
			self.con.commit()
		return


	def get_user_roles_projects(self, hashed_token):
		user = None
		roles = []
		projects = []
		
		query = """
		SELECT s.`username`, r.`users_dwb_role`, p.`users_dwb_project_id`, p.`project_name`
		FROM `sessions` s
		LEFT JOIN `dwb_roles` r
		ON r.session_id = s.session_id
		LEFT JOIN `dwb_projects` p
		ON p.session_id = s.session_id
		WHERE s.`hashed_token` = %s
		"""
		self.cur.execute(query, hashed_token)
		rows = self.cur.fetchall()
		
		projectsdict = {}
		
		for row in rows:
			if user is None:
				user = row[0]
			if row[1] not in roles:
				roles.append(row[1])
			projectsdict[row[2]] = row[3]
		
		for key in projectsdict:
			projects.append([key, projectsdict[key]])
		
		return user, roles, projects


	def update_expiration_time(self, hashed_token):
		query = """
		UPDATE sessions
		SET expiration_time = NOW() + INTERVAL {0} MINUTE
		WHERE hashed_token = %s
		;""".format(self.expiration_interval)
		self.cur.execute(query, [hashed_token])
		self.con.commit()


	def get_session_id_by_token(self, token):
		if token is None:
			return
		hashed_token = self.encryptor.hash_token(token)
		
		query = """
		SELECT s.`session_id`
		FROM `sessions` s
		WHERE s.`hashed_token` = %s
		;"""
		self.cur.execute(query, hashed_token)
		row = self.cur.fetchone()
		if row is not None:
			return row[0]
		else:
			return None


	def __delete_sessions(self):
		
		query = """
		DELETE r 
		FROM `dwb_roles` r
		INNER JOIN sessions s
		ON (r.session_id = s.session_id)
		INNER JOIN sessions_to_delete sd
		ON sd.`hashed_token` = s.`hashed_token`
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DELETE p
		FROM `dwb_projects` p
		INNER JOIN sessions s
		ON (p.session_id = s.session_id)
		INNER JOIN sessions_to_delete sd
		ON sd.`hashed_token` = s.`hashed_token`
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DELETE s
		FROM `sessions` s 
		INNER JOIN sessions_to_delete sd
		ON sd.`hashed_token` = s.`hashed_token`
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		return


	def delete_old_sessions(self):
		query = """
		DROP TEMPORARY 
		TABLE IF EXISTS sessions_to_delete
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TEMPORARY 
		TABLE sessions_to_delete
		SELECT `hashed_token`
		FROM `sessions` s 
		WHERE s.expiration_time < NOW()
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		self.__delete_sessions()
		return


	def delete_session_by_token(self, token):
		if token is None:
			return
		hashed_token = self.encryptor.hash_token(token)
		
		query = """
		DROP TEMPORARY 
		TABLE IF EXISTS sessions_to_delete
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TEMPORARY 
		TABLE sessions_to_delete
		SELECT `hashed_token`
		FROM `sessions` s 
		WHERE s.hashed_token = %s
		;"""
		self.cur.execute(query, [hashed_token])
		self.con.commit()
		
		self.__delete_sessions()
		return
	
	
	def get_credentials_from_session(self, token):
		if token is None:
			return [None, None]
		hashed_token = self.encryptor.hash_token(token)
		
		query = """
		SELECT s.username, s.encrypted_passwd
		FROM sessions s
		WHERE s.`hashed_token` = %s
		"""
		self.cur.execute(query, [hashed_token])
		row = self.cur.fetchone()
		if row is None:
			return [None, None]
		
		else:
			username = row[0]
			key = self.encryptor.get_key_from_token(token)
			password = self.encryptor.decrypt_password(row[1], key)
			self.update_expiration_time(hashed_token)
			return [username, password]


	def get_identity_by_token(self, token):
		if token is None:
			return None
		hashed_token = self.encryptor.hash_token(token)
		
		username, dwb_roles, projects = self.get_user_roles_projects(hashed_token)
		
		if username is None or len(projects) < 1 or len(dwb_roles) < 1:
			return None
		
		# TODO: how to manage the project information in a simple non-redundant way?
		identity = {
			'username': username,
			'projects': projects,
			#'project_ids': project_ids,
			#'project_names': project_names,
			'dwb_roles': dwb_roles
		}
		return identity


	'''
	def session_is_valid(self, username, token):
		key = self.__get_key_from_token(token)
		salt = self.__get_salt_from_token(token)
		hashed_key = self.hash_secret(key, salt)
		if self.get_encrypted_passwd(username, hashed_key) is not None:
			self.update_expiration_time(hashed_key)
			return True
		else:
			return False
	'''


	'''
	def get_dwb_roles(self, hashed_token):
		query = """
		SELECT `users_dwb_role`
		FROM `dwb_roles` r
		INNER JOIN `sessions` s
		ON r.session_id = s.session_id
		WHERE `hashed_token` = %s
		"""
		self.cur.execute(query, hashed_token)
		rows = self.cur.fetchall()
		projects = []
		for row in rows:
			projects.append(row[0])
		return projects


	def get_projects(self, hashed_token):
		query = """
		SELECT `users_dwb_project_id`, `project_name`
		FROM `dwb_projects` p
		INNER JOIN `sessions` s
		ON p.session_id = s.session_id
		WHERE `hashed_token` = %s
		"""
		self.cur.execute(query, hashed_token)
		rows = self.cur.fetchall()
		projects = []
		for row in rows:
			projects.append([row[0], row[1]])
		return projects


	# needed to get the projects for a certain user
	# e.g. in CollTableLookup. does it break the security, as i can get information without a token?
	def get_projects_by_username(self, username):
		query = """
		SELECT `users_dwb_project_id`, `project_name`
		FROM `dwb_projects` p
		INNER JOIN `sessions` s
		ON p.session_id = s.session_id
		WHERE `username` = %s
		"""
		self.cur.execute(query, username)
		rows = self.cur.fetchall()
		projects = []
		for row in rows:
			projects.append([row[0], row[1]])
		return projects


	def get_username(self, hashed_token):
		query = """
		SELECT `username`
		FROM `sessions`
		WHERE `hashed_token` = %s
		"""
		self.cur.execute(query, hashed_token)
		row = self.cur.fetchone()
		if row is None:
			return None
		else:
			return row[0]
	'''

