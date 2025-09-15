import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class ProjectMatcher():
	def __init__(self, dc_db, temptable):
		self.dc_db = dc_db
		self.temptable = temptable
		
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.prefiltered_temptable = '#prefiltered_projects'


	def matchExistingProjects(self):
		self.__createPrefilteredTempTable()
		self.__matchIntoPrefiltered()
		self.addProjectSHA(self.prefiltered_temptable)
		
		self.__matchPrefilteredToTempTable()


	def __createPrefilteredTempTable(self):
		
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.prefiltered_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[ProjectID] INT,
		[Project] NVARCHAR(50) COLLATE {1} NOT NULL,
		[ProjectURI] VARCHAR(255) COLLATE {1},
		 -- [StableIdentifierBase] VARCHAR(500) COLLATE {1},
		 -- [StableIdentifierTypeID] INT,
		[RowGUID] UNIQUEIDENTIFIER NOT NULL,
		[project_sha] VARCHAR(64) COLLATE {1},
		INDEX [project_sha_idx] ([project_sha])
		)
		;""".format(self.prefiltered_temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __matchIntoPrefiltered(self):
		# first match all existing Projects by Project and ProjectURI
		
		query = """
		INSERT INTO [{0}] (
			[ProjectID],
			[Project],
			[ProjectURI],
			 -- [StableIdentifierBase],
			 -- [StableIdentifierTypeID],
			[RowGUID]
		)
		SELECT 
			pp.[ProjectID],
			pp.[Project],
			pp.[ProjectURI],
			 -- pp.[StableIdentifierBase],
			 -- pp.[StableIdentifierTypeID],
			pp.[RowGUID]
		FROM [ProjectProxy] pp
		INNER JOIN [{1}] p_temp ON
			p_temp.[Project] = pp.[Project]
			AND ((p_temp.[ProjectURI] = pp.[ProjectURI]) OR (p_temp.[ProjectURI] IS NULL AND pp.[ProjectURI] IS NULL))
		 -- WHERE p_temp.[ProjectID] IS NULL 
		;""".format(self.prefiltered_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def addProjectSHA(self, tablename):
		query = """
		UPDATE t
		SET [project_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[Project],
			[ProjectURI] -- ,
			 -- [StableIdentifierBase],
			 -- [StableIdentifierTypeID]
		)), 2)
		FROM [{0}] t
		;""".format(tablename)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __matchPrefilteredToTempTable(self):
		query = """
		UPDATE p_temp
		SET p_temp.[ProjectID] = pf.[ProjectID],
		p_temp.[RowGUID] = pf.[RowGUID]
		FROM [{0}] p_temp
		INNER JOIN [{1}] pf
		ON pf.[project_sha] = p_temp.[project_sha]
		;""".format(self.temptable, self.prefiltered_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
