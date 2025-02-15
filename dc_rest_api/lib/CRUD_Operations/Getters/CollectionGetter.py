import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter

class CollectionGetter(DataGetter):
	def __init__(self, dc_db):
		DataGetter.__init__(self, dc_db)
		
		self.get_temptable = '#get_c_temptable'



	def getByPrimaryKeys(self, c_ids):
		self.createGetTempTable()
		
		batchsize = 1000
		while len(c_ids) > 0:
			cached_ids = c_ids[:batchsize]
			del c_ids[:batchsize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			DROP TABLE IF EXISTS [#c_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#c_pks_to_get_temptable] (
				[CollectionID] INT NOT NULL,
				INDEX [CollectionID_idx] ([CollectionID])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#c_pks_to_get_temptable] (
				[CollectionID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [Collection] c
			INNER JOIN [#c_pks_to_get_temptable] pks
			ON pks.[CollectionID] = c.[CollectionID]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		collections = self.getData()
		
		return collections


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		collections = self.getData()
		
		return collections



	def getData(self):
		self.setDatabaseURN()
		
		query = """
		SELECT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		c.[CollectionID],
		c.[CollectionName],
		c.[CollectionAcronym],
		c.[AdministrativeContactName],
		c.[AdministrativeContactAgentURI],
		c.[Description],
		c.[Location],
		c.[LocationParentID],
		c.[LocationPlan],
		c.[LocationPlanWidth],
		c.[LocationPlanDate],
		c.[LocationGeometry].STAsText() AS [LocationGeometry],
		c.[LocationHeight],
		c.[CollectionOwner]
		FROM [{0}] g_temp
		INNER JOIN [Collection] c
		ON c.[RowGUID] = g_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.results_rows = self.cur.fetchall()
		self.rows2list()
		
		return self.results_list


	def list2dict(self):
		self.results_dict = {}
		for element in self.results_list:
			if element['CollectionID'] not in self.results_dict:
				self.results_dict[element['CollectionID']] = {}
				
			self.results_dict[element['CollectionID']] = element 














