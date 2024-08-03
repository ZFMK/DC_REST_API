import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Matchers.ProjectMatcher import ProjectMatcher


class ProjectInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#project_temptable'
		self.unique_projects_temptable = '#unique_p_temptable'
		
		# set a minimum ProjectID for new projects to insert
		self.min_project_id = 271176
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID'},
			{'colname': 'ProjectID'},
			{'colname': 'Project', 'None allowed': False},
			{'colname': 'ProjectURI'},
			#{'colname': 'StableIdentifierBase'},
			#{'colname': 'StableIdentifierTypeID'}
		]
		
		self.json2temp = JSON2TempTable(self.dc_db, self.schema)


	def insertProjectData(self):
		
		self.__createProjectTempTable()
		
		self.json2temp.set_datadicts(self.project_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__addProjectSHA()
		
		self.project_matcher = ProjectMatcher(self.dc_db, self.temptable)
		self.project_matcher.matchExistingProjects()
		
		num_new_projects = self.__getNumberOfUnmatchedProjects()
		if num_new_projects > 0:
			if 'Administrator' in self.users_roles:
				self.createNewProjects()
			else:
				self.messages.append('You do not have the rights to insert new projects')
		
		self.__updateProjectDicts()
		
		return


	def setProjectDicts(self, json_dicts = []):
		self.project_dicts = []
		p_count = 1
		for project_dict in json_dicts:
			project_dict['entry_num'] = p_count
			p_count += 1
			self.project_dicts.append(project_dict)
		return



	def __createProjectTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[entry_num] INT NOT NULL,
		[CollectionSpecimenID] INT DEFAULT NULL,
		[ProjectID] INT DEFAULT NULL,
		[Project] NVARCHAR(50) COLLATE {1} NOT NULL,
		[ProjectURI] VARCHAR(255) COLLATE {1},
		 -- [StableIdentifierBase] VARCHAR(500) COLLATE {1},
		 -- [StableIdentifierTypeID] INT,
		[RowGUID] UNIQUEIDENTIFIER,
		[project_sha] VARCHAR(64) COLLATE {1},
		PRIMARY KEY ([entry_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [ProjectID_idx] ([ProjectID]),
		INDEX [Project_idx] ([Project]),
		 -- INDEX [StableIdentifierBase_idx] ([StableIdentifierBase])
		INDEX [project_sha_idx] ([project_sha])
		)
		;""".format(self.temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __addProjectSHA(self):
		query = """
		UPDATE p_temp
		SET [project_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[Project],
			[ProjectURI]
			 -- , [StableIdentifierBase]
		)), 2)
		FROM [{0}] p_temp
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateProjectIDsInTempTable(self):
		query = """
		UPDATE p_temp
		SET p_temp.ProjectID = pp.ProjectID
		FROM [{0}] p_temp
		INNER JOIN [{1}] ue_temp
		ON p_temp.[project_sha] = ue_temp.[project_sha]
		INNER JOIN [ProjectProxy] pp
		ON ue_temp.[RowGUID] = pp.[RowGUID]
		;""".format(self.temptable, self.unique_projects_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def createNewProjects(self):
		# insert only one version of each project when the same project occurres multiple times in json data
		self.__setUniqueProjectssTempTable()
		self.__insertNewProjects()
		self.__updateProjectIDsInTempTable()
		
		self.__insertCollectionProjects()
		return


	def __setUniqueProjectsTempTable(self):
		"""
		create a table that contains only one version of each project to be inserted
		"""
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.unique_projects_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
			[Project] NVARCHAR(50) COLLATE {1} NOT NULL,
			[ProjectURI] VARCHAR(255) COLLATE {1},
			 -- [StableIdentifierBase] VARCHAR(500) COLLATE {1},
			 -- [StableIdentifierTypeID] INT,
			[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
			 -- 
			[project_sha] VARCHAR(64) COLLATE {1},
			INDEX [project_sha_idx] ([project_sha]),
			INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.unique_collections_temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [{0}] (
			[project_sha]
		)
		SELECT DISTINCT
			[project_sha]
		FROM [{1}] p_temp
		WHERE p_temp.[ProjectID] IS NULL
		;""".format(self.unique_projects_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE ue_temp
		SET 
			[Project] = p_temp.[Project],
			[ProjectURI] = p_temp.[ProjectURI]
			 -- ,[StableIdentifierBase] = p_temp.[StableIdentifierBase],
			 -- ,[StableIdentifierTypeID] = p_temp.[StableIdentifierTypeID]
		FROM [{0}] ue_temp
		INNER JOIN [{1}] p_temp
		ON ue_temp.[project_sha] = p_temp.[project_sha]
		;""".format(self.unique_collections_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return



	def __insertNewProjects(self):
		query = """
		INSERT INTO [ProjectProxy] (
			[ProjectID],
			[Project],
			 -- [StableIdentifierBase],
			 -- [StableIdentifierTypeID],
			[RowGUID]
		)
		SELECT
			ue_temp.[ProjectID],
			ue_temp.[Project],
			ue_temp.[StableIdentifierBase],
			ue_temp.[StableIdentifierTypeID],
			ue_temp.[RowGUID]
		FROM [{0}] ue_temp
		;""".format(self.unique_projects_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE ue_temp
		SET ue_temp.[ProjectID] = pp.[ProjectID]
		FROM [{0}] ue_temp
		INNER JOIN [ProjectProxy] pp
		ON pp.[RowGUID] = ue_temp.[RowGUID]
		;""".format(self.unique_projects_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __insertCollectionProjects(self):
		query = """
		INSERT INTO [CollectionProject]
		(
			[CollectionSpecimenID],
			[ProjectID]
		)
		SELECT 
			p_temp.[CollectionSpecimenID],
			p_temp.[ProjectID]
		FROM [{0}] p_temp
		LEFT JOIN [CollectionProject] cp ON
			p_temp.[ProjectID] = cp.[ProjectID] AND p_temp.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE cp.[ProjectID] IS NULL AND p_temp.[ProjectID] IS NOT NULL AND p_temp.[CollectionSpecimenID] IS NOT NULL
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateProjectDicts(self):
		p_ids = self.getIDsForProjectDicts()
		for p_dict in self.project_dicts:
			entry_num = p_dict['entry_num']
			p_dict['ProjectID'] = p_ids[entry_num]['ProjectID']
			p_dict['RowGUID'] = p_ids[entry_num]['RowGUID']
		return


	def getIDsForCollectionDicts(self):
		query = """
		SELECT 
			p_temp.[entry_num],
			pp.[ProjectID],
			pp.[RowGUID]
		FROM [ProjectProxy] pp
		INNER JOIN [{0}] p_temp
		ON p_temp.[RowGUID] = pp.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		p_ids = {}
		for row in rows:
			if not row[0] in p_ids:
				p_ids[row[0]] = {}
			p_ids[row[0]]['ProjectID'] = row[1]
			p_ids[row[0]]['RowGUID'] = row[2]
		
		return c_ids






########################################


	def __setExistingProjects(self):
		query = """
		UPDATE p_temp
		SET p_temp.[ProjectID] = pp.[ProjectID],
		FROM [{0}] p_temp
		INNER JOIN ProjectProxy pp ON 
			p_temp.[Project] = pp.[Project]
			AND ((p_temp.[ProjectURI] = pp.[ProjectURI]) OR (p_temp.[ProjectURI] IS NULL AND pp.[ProjectURI] IS NULL))
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return







