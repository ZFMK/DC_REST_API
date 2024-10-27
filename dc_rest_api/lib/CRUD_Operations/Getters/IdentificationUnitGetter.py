import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
#from dc_rest_api.lib.CRUD_Operations.Getters.IdentificationUnitAnalysisGetter import IdentificationUnitAnalysisGetter
from dc_rest_api.lib.CRUD_Operations.Getters.IdentificationGetter import IdentificationGetter

class IdentificationUnitGetter(DataGetter):
	def __init__(self, dc_db, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
		self.withholded = []
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_iu_temptable'



	def getByPrimaryKeys(self, cs_iu_ids):
		self.createGetTempTable()
		
		batchsize = 1000
		while len(cs_iu_ids) > 0:
			cached_ids = cs_iu_ids[:batchsize]
			del cs_iu_ids[:batchsize]
			placeholders = ['(?, ?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			DROP TABLE IF EXISTS [#iu_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#iu_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[IdentificationUnitID] INT NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#iu_pks_to_get_temptable] (
			[CollectionSpecimenID],
			[IdentificationUnitID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [IdentificationUnit] iu
			INNER JOIN [#iu_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = iu.[CollectionSpecimenID]
			AND pks.[IdentificationUnitID] = iu.[IdentificationUnitID]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		identificationunits = self.getData()
		
		return identificationunits


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		identificationunits = self.getData()
		
		return identificationunits



	def getData(self):
		self.setDatabaseURN()
		self.withholded = self.filterAllowedRowGUIDs()
		
		query = """
		SELECT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		iu.[CollectionSpecimenID],
		iu.[IdentificationUnitID],
		iu.[RowGUID],
		iu.[LastIdentificationCache],
		iu.[TaxonomicGroup],
		iu.[DisplayOrder],
		iu.[LifeStage],
		iu.[Gender],
		iu.[NumberOfUnits],
		iu.[NumberOfUnitsModifier],
		iu.[UnitIdentifier],
		iu.[UnitDescription],
		iu.[Notes],
		iu.[DataWithholdingReason]
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnit] iu
		ON iu.[RowGUID] = g_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.iu_rows = self.cur.fetchall()
		self.rows2list()
		
		self.setChildIdentifications()
		
		return self.iu_list


	def rows2list(self):
		self.iu_list = []
		for row in self.iu_rows:
			self.iu_list.append(dict(zip(self.columns, row)))
		return


	def list2dict(self):
		self.iu_dict = {}
		for element in self.iu_list:
			if element['CollectionSpecimenID'] not in self.iu_dict:
				self.iu_dict[element['CollectionSpecimenID']] = {}
				
			self.iu_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']] = element 
		
		return


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		# the withholded variable keeps the IDs and RowGUIDs of the withholded rows
		withholded = []
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
		query = """
		SELECT iu.[CollectionSpecimenID], iu.[IdentificationUnitID], iu.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnit] iu
		ON iu.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [CollectionSpecimen] cs ON iu.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		{1}
		WHERE (iu.[DataWithholdingReason] IS NOT NULL AND iu.[DataWithholdingReason] != '') 
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
		INNER JOIN [IdentificationUnit] iu
		ON iu.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [CollectionSpecimen] cs ON iu.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		{1}
		WHERE (iu.[DataWithholdingReason] IS NOT NULL AND iu.[DataWithholdingReason] != '') 
		OR (cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '')
		{2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return withholded


	def setChildIdentifications(self):
		
		i_getter = IdentificationGetter(self.dc_db, self.users_project_ids)
		i_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT i.[RowGUID]
		FROM [IdentificationUnit] iu
		INNER JOIN [Identification] i
		ON iu.[CollectionSpecimenID] = i.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = i.[IdentificationUnitID]
		INNER JOIN [{1}] rg_temp
		ON iu.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(i_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		i_getter.getData()
		i_getter.list2dict()
		
		for iu in self.iu_list:
			if iu['CollectionSpecimenID'] in i_getter.i_dict and iu['IdentificationUnitID'] in i_getter.i_dict[iu['CollectionSpecimenID']]:
				if 'Identifications' not in iu:
					iu['Identifications'] = []
				for i_id in i_getter.i_dict[iu['CollectionSpecimenID']][iu['IdentificationUnitID']]:
					iu['Identifications'].append(i_getter.i_dict[iu['CollectionSpecimenID']][iu['IdentificationUnitID']][i_id])
		
		return











