import pudb

from configparser import ConfigParser
config = ConfigParser(allow_no_value=True)
config.read('./config.ini')

from dwb_authentication.mysql_connect import mysql_connect

class SessionDBSetup():
	def __init__(self):
		self.con, self.cur = mysql_connect()
		
		self.delete_tables()
		self.create_tables()
		self.set_connectors()


	def delete_tables(self):
		
		query = """
		DROP TABLE IF EXISTS `dwb_projects`
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DROP TABLE IF EXISTS `dwb_roles`
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DROP TABLE IF EXISTS `session_has_connector`
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DROP TABLE IF EXISTS `sessions`
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DROP TABLE IF EXISTS `mssql_connectors`
		;"""
		
		self.cur.execute(query)
		self.con.commit()


	def create_tables(self):
		
		query = """
		CREATE TABLE `mssql_connectors` (
			`db_connector_id` VARCHAR(50),
			`accronym` varchar(50) NOT NULL,
			`server` varchar(255) NOT NULL,
			`port` INT DEFAULT NULL,
			`database` varchar(255) DEFAULT NULL,
			`driver` varchar(255) DEFAULT NULL,
			`trust_certificate` VARCHAR(10) DEFAULT NULL,
			`trusted_connection` VARCHAR(10) DEFAULT NULL, 
			`encrypt` VARCHAR(10) DEFAULT NULL, 
			`collation` VARCHAR(50) DEFAULT NULL,
			PRIMARY KEY (`db_connector_id`),
			UNIQUE KEY `idx_accronym` (`accronym`),
			UNIQUE KEY `idx_connector` (`server`,`port`,`database`),
			KEY (`server`),
			KEY (`database`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		CREATE TABLE `sessions` (
			`session_id` INT NOT NULL AUTO_INCREMENT,
			`hashed_token` varchar(255) NOT NULL,
			`encrypted_passwd` varchar(255),
			`expiration_time` datetime NOT NULL,
			`username` VARCHAR(50) NOT NULL,
			PRIMARY KEY (`session_id`),
			UNIQUE KEY `hashed_token` (`hashed_token`),
			KEY (`username`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE `session_has_connector` (
			`session_id` INT NOT NULL,
			`db_connector_id` VARCHAR(50) NOT NULL,
			UNIQUE KEY `session_has_connector_idx` (`session_id`, `db_connector_id`),
			FOREIGN KEY (`session_id`) REFERENCES `sessions` (`session_id`),
			FOREIGN KEY (`db_connector_id`) REFERENCES `mssql_connectors` (`db_connector_id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		CREATE TABLE `dwb_roles` (
			`users_dwb_role` VARCHAR(255),
			`session_id` INT NOT NULL,
			`db_connector_id` VARCHAR(50),
			PRIMARY KEY(`session_id`, `db_connector_id`, `users_dwb_role`),
			FOREIGN KEY(`session_id`, `db_connector_id`) REFERENCES `session_has_connector` (`session_id`, `db_connector_id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;
		"""
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		CREATE TABLE `dwb_projects` (
			`users_dwb_project_id` VARCHAR(50),
			`project_name` VARCHAR(255),
			`session_id` INT NOT NULL,
			`db_connector_id` VARCHAR(50),
			PRIMARY KEY(`session_id`, `db_connector_id`, `users_dwb_project_id`),
			FOREIGN KEY(`session_id`, `db_connector_id`) REFERENCES `session_has_connector` (`session_id`, `db_connector_id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;
		"""
		self.cur.execute(query)
		self.con.commit()


	def set_connectors(self):
		
		self.read_connectors()
		
		if len(self.connectors) > 0:
			placeholders = []
			values = []
			for valuelist in self.connectors:
				placeholders.append('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')
				values.extend(valuelist)
			placeholderstring = ', '.join(placeholders)
			
			query = """
			INSERT INTO `mssql_connectors`
			(`db_connector_id`, `accronym`, `server`, `port`, `database`, `driver`, `trust_certificate`, `trusted_connection`, `encrypt`, `collation`)
			VALUES {0}
			;""".format(placeholderstring)
			
			
			self.cur.execute(query, values)
			self.con.commit()


	def read_connectors(self):
		# read the default connection parameters without any credentials!
		self.connectors = []
		
		sections = config.sections()
		for section in sections:
			if section[:6]=='dc_db_' and section!='dc_db_test' and len(section[6:]) > 0:
				database_id = section[6:]
				accronym = config.get(section, 'accronym', fallback = section[6:])
				server = config.get(section, 'server', fallback = None)
				port = config.get(section, 'port', fallback = None)
				database = config.get(section, 'database', fallback = None)
				driver = config.get(section, 'driver', fallback = None)
				trust_certificate = config.get(section, 'trust_certificate', fallback = None)
				trusted_connection = config.get(section, 'trusted_connection', fallback = 'YES')
				encrypt = config.get(section, 'encrypt', fallback = 'YES')
				collation = config.get(section, 'collation', fallback = 'Latin1_General_CI_AS')
				
				if server is not None and port is not None and database is not None:
					self.connectors.append([database_id, accronym, server, port, database, driver, trust_certificate, trusted_connection, encrypt, collation])
		
		return


if __name__ == "__main__":
	db_setup = SessionDBSetup()


