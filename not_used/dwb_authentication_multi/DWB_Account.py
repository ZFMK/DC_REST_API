import pudb


from DBConnectors.MSSQLConnector import MSSQLConnector


class DWB_Account():
	def __init__(self, conparams):
		self.conparams = conparams
		self.username = conparams['username']


	def isValid(self):
		try:
			dwb_con = MSSQLConnector(config = self.conparams)
			self.server_roles = self.getServerRoles(dwb_con)
			self.users_projects = self.getUsersProjects(dwb_con)
			dwb_con.closeConnection()
			return True
		
		except:
			# TODO: find out the exceptions
			return False


	def getServerRoles(self, dwb_con):
		con = dwb_con.getConnection()
		cur = dwb_con.getCursor()
		
		query = """
		SELECT p1.name AS [Role], p2.name AS [user] FROM sys.database_role_members r
		INNER JOIN sys.database_principals p1
		ON r.role_principal_id = p1.principal_id
		INNER JOIN sys.database_principals p2
		ON r.member_principal_id = p2.principal_id
		WHERE p2.name = ?
		;"""
		
		cur.execute(query, self.username)
		rows = cur.fetchall()
		
		roles = []
		for row in rows:
			roles.append(row[0])
		return roles


	def getUsersProjects(self, dwb_con):
		con = dwb_con.getConnection()
		cur = dwb_con.getCursor()
		
		query = """
		SELECT pu.ProjectID, pp.Project FROM ProjectUser pu
		INNER JOIN ProjectProxy pp
		ON pp.ProjectID = pu.ProjectID
		WHERE LoginName = ?
		;"""
		
		cur.execute(query, self.username)
		rows = cur.fetchall()
		
		projects = []
		for row in rows:
			projects.append([row[0], row[1]])
		return projects
