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
		self.dwb_servers = DWB_Servers().get_available_dwb_cons()
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
		INNER JOIN `session_has_connector` shc ON shc.`db_connector_id` = mc.`db_connector_id`
		INNER JOIN `sessions` s ON s.`session_id` = shc.`session_id` 
		WHERE s.`hashed_token` = %s
		;"""
		
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


	def get_mssql_connectionparams(self):
		conparams = {}
		for dwb_server in self.dwb_servers:
			conparams[dwb_server['db_id']] = dwb_server
		
		return conparams


	def set_session(self, username, password):
		# pudb.set_trace()
		
		connector_ids = []
		roles = {}
		projects = {}
		
		dwb_servers = self.get_mssql_connectionparams()
		
		for connector_id in dwb_servers:
			conparams = dwb_servers[connector_id]
			conparams['username'] = username
			conparams['password'] = password
			
			dwb_account = DWB_Account(conparams)
			
			if dwb_account.isValid() is True:
				roles[connector_id] = dwb_account.server_roles
				projects[connector_id] = dwb_account.users_projects
				
				connector_ids.append(connector_id)
		
		if len(connector_ids) <= 0 or len(roles) <= 0:
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
		(hashed_token, encrypted_passwd, `username`, expiration_time)
		VALUES(%s, %s, %s, NOW() + INTERVAL {0} MINUTE)
		;""".format(self.expiration_interval)
		
		self.cur.execute(query, [hashed_token, encrypted_pw, username])
		self.con.commit()
		
		self.__set_session_id(hashed_token)
		
		self.insert_session_has_connector(connector_ids)
		for connector_id in connector_ids:
			self.insert_dwb_roles(connector_id, roles[connector_id])
			self.insert_projects(connector_id, projects[connector_id])
		
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


	def get_mssql_connector_id(self, server, port, database):
		query = """
		SELECT db_connector_id
		FROM `mssql_connectors`
		WHERE `server` = %s AND `port` = %s AND `database` = %s
		;"""
		self.cur.execute(query, [server, port, database])
		row = self.cur.fetchone()
		if row is not None:
			return row[0]
		return None


	def insert_dwb_roles(self, connector_id, roles = []):
		if len(roles)> 0:
			values = []
			placeholders = []
			for role in roles:
				values.extend([role, self.session_id, connector_id])
				placeholders.append('(%s, %s, %s)')
			placeholderstring = ', '.join(placeholders)
			query = """
			INSERT INTO `dwb_roles`
			(`users_dwb_role`, `session_id`, `db_connector_id`)
			VALUES {0}
			;""".format(placeholderstring)
			
			self.cur.execute(query, values)
			self.con.commit()
		return


	def insert_projects(self, connector_id, projects = []):
		if len(projects)> 0:
			values = []
			placeholders = []
			for project in projects:
				values.extend([project[0], project[1], self.session_id, connector_id])
				placeholders.append('(%s, %s, %s, %s)')
			placeholderstring = ', '.join(placeholders)
			query = """
			INSERT INTO `dwb_projects`
			(`users_dwb_project_id`, `project_name`, `session_id`, `db_connector_id`)
			VALUES {0}
			;""".format(placeholderstring)
			
			self.cur.execute(query, values)
			self.con.commit()
		return


	def insert_session_has_connector(self, connector_ids):
		values = []
		placeholders = ['(%s, %s)' for connector_id in connector_ids]
		valuelists = [[self.session_id, connector_id] for connector_id in connector_ids]
		for valuelist in valuelists:
			values.extend(valuelist)
		
		query = """
		INSERT INTO `session_has_connector`
		(session_id, db_connector_id)
		VALUES {0}
		;""".format(', '.join(placeholders))
		
		self.cur.execute(query, values)
		self.con.commit()


	def get_user_roles_projects(self, hashed_token):
		
		# ht_string = hashed_token.decode('utf8')
		
		user = None
		roles = {}
		projects = []
		
		query = """
		SELECT s.`username`
		FROM `sessions` s
		WHERE s.`hashed_token` = %s
		;"""
		
		self.cur.execute(query, [hashed_token])
		row = self.cur.fetchone()
		
		if row is not None:
			user = row[0]
		
		query = """
		SELECT shc.`db_connector_id`, r.`users_dwb_role`
		FROM `sessions` s
		INNER JOIN `session_has_connector` shc
		ON s.`session_id` = shc.`session_id`
		LEFT JOIN `dwb_roles` r
		ON r.session_id = shc.session_id AND r.`db_connector_id` = shc.`db_connector_id`
		WHERE s.`hashed_token` = %s
		"""
		self.cur.execute(query, [hashed_token])
		rows = self.cur.fetchall()
		
		for row in rows:
			if row[0] not in roles:
				roles[row[0]] = []
			roles[row[0]].append(row[1])
		
		query = """
		SELECT shc.`db_connector_id`, p.`users_dwb_project_id`, p.`project_name`
		FROM `sessions` s
		INNER JOIN `session_has_connector` shc
		ON s.`session_id` = shc.`session_id`
		LEFT JOIN `dwb_projects` p
		ON p.session_id = shc.session_id AND p.`db_connector_id` = shc.`db_connector_id`
		WHERE s.`hashed_token` = %s
		"""
		self.cur.execute(query, [hashed_token])
		rows = self.cur.fetchall()
		
		for row in rows:
			projects.append(['{0}_{1}'.format(row[0], row[1]), row[2]])
		
		return user, roles, projects


	def update_expiration_time(self, hashed_token):
		query = """
		UPDATE sessions
		SET expiration_time = NOW() + INTERVAL {0} MINUTE
		WHERE hashed_token = %s
		;""".format(self.expiration_interval)
		self.cur.execute(query, [hashed_token])
		self.con.commit()
	
	
	def _delete_sessions(self):
		
		query = """
		DELETE r 
		FROM `dwb_roles` r
		INNER JOIN `session_has_connector` shc
		ON r.`session_id` = shc.`session_id` AND r.`db_connector_id` = shc.`db_connector_id`
		INNER JOIN sessions s
		ON (shc.session_id = s.session_id)
		INNER JOIN sessions_to_delete sd
		ON sd.`hashed_token` = s.`hashed_token`
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DELETE p
		FROM `dwb_projects` p
		INNER JOIN `session_has_connector` shc
		ON p.`session_id` = shc.`session_id` AND p.`db_connector_id` = shc.`db_connector_id`
		INNER JOIN sessions s
		ON (shc.session_id = s.session_id)
		INNER JOIN sessions_to_delete sd
		ON sd.`hashed_token` = s.`hashed_token`
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DELETE shc
		FROM `session_has_connector` shc 
		INNER JOIN `sessions` s
		ON s.`session_id` = shc.`session_id`
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
		
		self._delete_sessions()


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
		
		self._delete_sessions()


	def get_credentials_from_session(self, token):
		if token is None:
			return None
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

