import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter

class CollectionExternalDatasourceGetter(DataGetter):
	def __init__(self, dc_db):
		DataGetter.__init__(self, dc_db)
		
		self.get_temptable = '#get_ed_temptable'



	def getByPrimaryKeys(self, ed_ids):
		self.createGetTempTable()
		
		batchsize = 1000
		while len(ed_ids) > 0:
			cached_ids = ed_ids[:batchsize]
			del ed_ids[:batchsize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			DROP TABLE IF EXISTS [#ed_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#ed_pks_to_get_temptable] (
				[ExternalDatasourceID] INT NOT NULL,
				INDEX [ExternalDatasourceID_idx] ([ExternalDatasourceID])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#ed_pks_to_get_temptable] (
				[ExternalDatasourceID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [CollectionExternalDatasource] ed
			INNER JOIN [#ed_pks_to_get_temptable] pks
			ON pks.[ExternalDatasourceID] = ed.[ExternalDatasourceID]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		externaldatasources = self.getData()
		
		return externaldatasources


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		externaldatasources = self.getData()
		
		return externaldatasources



	def getData(self):
		self.setDatabaseURN()
		
		query = """
		SELECT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		ed.ExternalDatasourceID,
		ed.ExternalDatasourceName,
		ed.ExternalDatasourceVersion,
		ed.[Rights],
		ed.ExternalDatasourceAuthors,
		ed.ExternalDatasourceURI,
		ed.ExternalDatasourceInstitution,
		ed.ExternalAttribute_NameID,
		ed.InternalNotes,
		ed.PreferredSequence,
		ed.[Disabled]
		FROM [{0}] g_temp
		INNER JOIN [CollectionExternalDatasource] ed
		ON ed.[RowGUID] = g_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.results_rows = self.cur.fetchall()
		self.rows2list()
		
		return self.results_list


	def list2dict(self):
		self.results_dict = {}
		for element in self.results_list:
			if element['ExternalDatasourceID'] not in self.results_dict:
				self.results_dict[element['ExternalDatasourceID']] = {}
				
			self.results_dict[element['ExternalDatasourceID']] = element 














