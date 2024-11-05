import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter

from DBConnectors.MSSQLConnector import MSSQLConnector

class CollectionAgentGetter(DataGetter):
	def __init__(self, dc_config, users_project_ids = []):
		self.dc_config = dc_config
		self.dc_db = MSSQLConnector(config = self.dc_config)
		
		DataGetter.__init__(self, self.dc_db)
		
		self.withholded = []
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_ca_temptable'



	def getByPrimaryKeys(self, ca_ids):
		self.createGetTempTable()
		
		batchsize = 1000
		while len(ca_ids) > 0:
			cached_ids = ca_ids[:batchsize]
			del ca_ids[:batchsize]
			placeholders = ['(?, ?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			DROP TABLE IF EXISTS [#ca_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#ca_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[CollectorsName] VARCHAR(255) COLLATE {0} NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [CollectorsName_idx] ([CollectorsName])
			)
			;""".format(self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#ca_pks_to_get_temptable] (
			[CollectionSpecimenID],
			[CollectorsName]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [CollectionAgent] ca
			INNER JOIN [#ca_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = ca.[CollectionSpecimenID]
			AND pks.[CollectorsName] = ca.[CollectorsName]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		collectionagents = self.getData()
		
		#self.getChildCollections()
		
		return collectionagents


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		collectionagents = self.getData()
		
		return collectionagents



	def getData(self):
		self.setDatabaseURN()
		self.withholded = self.filterAllowedRowGUIDs()
		
		query = """
		SELECT DISTINCT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		ca.[CollectionSpecimenID],
		ca.[CollectorsName],
		ca.[CollectorsSequence],
		ca.[CollectorsNumber],
		ca.[Notes],
		ca.[DataWithholdingReason]
		FROM [{0}] g_temp
		INNER JOIN [CollectionAgent] ca
		ON ca.[RowGUID] = g_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.ca_rows = self.cur.fetchall()
		self.rows2list()
		
		return self.ca_list


	def rows2list(self):
		self.ca_list = []
		for row in self.ca_rows:
			self.ca_list.append(dict(zip(self.columns, row)))
		
		return


	def list2dict(self):
		self.ca_dict = {}
		for element in self.ca_list:
			if element['CollectionSpecimenID'] not in self.ca_dict:
				self.ca_dict[element['CollectionSpecimenID']] = {}
				
			self.ca_dict[element['CollectionSpecimenID']][element['CollectorsName']] = element 


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		# the withholded variable keeps the IDs and RowGUIDs of the withholded rows
		withholded = []
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
		query = """
		SELECT ca.[CollectionSpecimenID], ca.[CollectorsName], ca.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [CollectionAgent] ca
		ON ca.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [CollectionSpecimen] cs ON ca.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		{1}
		WHERE (ca.[DataWithholdingReason] IS NOT NULL AND ca.[DataWithholdingReason] != '')
		OR (cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '')
		{2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		rows = self.cur.fetchall()
		for row in rows:
			withholded.append((row[0], row[1], row[2]))
		
		query = """
		DELETE g_temp
		FROM [{0}] g_temp
		INNER JOIN [CollectionAgent] ca
		ON ca.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [CollectionSpecimen] cs ON ca.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		{1}
		WHERE (ca.[DataWithholdingReason] IS NOT NULL AND ca.[DataWithholdingReason] != '')
		OR (cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '')
		{2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return withholded











