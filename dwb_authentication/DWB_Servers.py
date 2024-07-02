import pudb

from dwb_authentication.mysql_connect import mysql_connect


class DWB_Servers():
	def __init__(self):
		
		self.con, self.cur = mysql_connect()
		self.set_available_servers()
	
	
	def set_available_servers(self):
		self.servers = []
		
		query = """
		SELECT `db_connector_id`, `accronym`, `server`, `port`, `database`, `driver`, `trust_certificate`, `trusted_connection`, `encrypt`, `collation`
		FROM `mssql_connectors`
		;"""
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			server = {}
			server['db_id'] = row[0]
			server['accronym'] = row[1]
			server['server'] = row[2]
			server['port'] = row[3]
			server['database'] = row[4]
			server['driver'] = row[5]
			server['trust_certificate'] = row[6]
			server['trusted_connection'] = row[7]
			server['encrypt'] = row[8]
			server['collation'] = row[9]
			self.servers.append(server)
		return
	
	
	def get_available_dwb_cons(self):
		return self.servers
	
	
	def get_dwb_con_by_accronym(self, accronym):
		dwb_con = None
		for server in self.servers:
			if accronym == server['accronym']:
				dwb_con = server
		
		return dwb_con


	def get_dwb_con_by_db_id(self, db_id):
		dwb_con = None
		for server in self.servers:
			if db_id == server['db_id']:
				dwb_con = server
		
		return dwb_con

