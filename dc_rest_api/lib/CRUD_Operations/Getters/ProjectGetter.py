import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter

class ProjectGetter(DataGetter):
	def __init__(self, dc_db):
		DataGetter.__init__(self, dc_db)
		self.collation = self.dc_db.collation
		self.get_temptable = '#get_p_temptable'


	def getByPrimaryKeys(self, p_ids):
		self.createGetTempTable()
		
		batchsize = 1000
		while len(p_ids) > 0:
			cached_ids = p_ids[:batchsize]
			del p_ids[:batchsize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			DROP TABLE IF EXISTS [#p_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#p_pks_to_get_temptable] (
				[ProjectID] INT NOT NULL,
				INDEX [ProjectID_idx] ([ProjectID])
			)
			;"""
			
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#p_pks_to_get_temptable] (
				[ProjectID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [ProjectProxy] pp
			INNER JOIN [#p_pks_to_get_temptable] pks
			ON pks.[ProjectID] = pp.[ProjectID]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		projects = self.getData()
		
		return projects


	def getByProjectNames(self, projects):
		self.createGetTempTable()
		
		batchsize = 1000
		while len(projects) > 0:
			cached_projects = projects[:batchsize]
			del projects[:batchsize]
			placeholders = ['(?)' for _ in cached_projects]
			values = [value for value in cached_projects]
			
			query = """
			DROP TABLE IF EXISTS [#p_names_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#p_names_to_get_temptable] (
				[Project] NVARCHAR(50) COLLATE {0} NOT NULL,
				INDEX [Project_idx] ([Project])
			)
			;""".format(self.collation)
			
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#p_names_to_get_temptable] (
				[Project]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT DISTINCT [RowGUID] FROM [ProjectProxy] pp
			INNER JOIN [#p_names_to_get_temptable] pn
			ON pn.[Project] = pp.[Project] COLLATE {1}
			;""".format(self.get_temptable, self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		projects = self.getData()
		
		return projects


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		projects = self.getData()
		
		return projects



	def getData(self):
		
		query = """
		SELECT DISTINCT
		p_temp.[rowguid_to_get] AS [RowGUID],
		pp.[ProjectID],
		pp.[Project],
		pp.[ProjectURI],
		pp.[StableIdentifierBase],
		pp.[StableIdentifierTypeID]
		FROM [{0}] p_temp
		INNER JOIN [ProjectProxy] pp
		ON pp.[RowGUID] = p_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.results_rows = self.cur.fetchall()
		self.rows2list()
		
		return self.results_list


	def list2dict(self):
		self.results_dict = {}
		for element in self.results_list:
			if element['ProjectID'] not in self.results_dict:
				self.results_dict[element['ProjectID']] = {}
				
			self.results_dict[element['ProjectID']] = element
		
		return











