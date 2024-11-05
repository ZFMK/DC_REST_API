import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from threading import Thread, Lock

from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
from dc_rest_api.lib.CRUD_Operations.Getters.IdentificationUnitAnalysisGetter import IdentificationUnitAnalysisGetter
from dc_rest_api.lib.CRUD_Operations.Getters.IdentificationGetter import IdentificationGetter

from DBConnectors.MSSQLConnector import MSSQLConnector

class IdentificationUnitGetter(DataGetter):
	def __init__(self, dc_config, users_project_ids = []):
		self.dc_config = dc_config
		self.dc_db = MSSQLConnector(config = self.dc_config)
		
		DataGetter.__init__(self, self.dc_db)
		
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
		self.lock = Lock()
		
		iu_getter_thread = Thread(target = self.getIUData)
		i_getter_thread = Thread(target = self.getChildIdentifications)
		iua_getter_thread = Thread(target = self.getChildIUAnalyses)
		
		iu_getter_thread.start()
		i_getter_thread.start()
		iua_getter_thread.start()
		
		iu_getter_thread.join()
		i_getter_thread.join()
		iua_getter_thread.join()
		
		self.insertIdentificationDict()
		self.insertIUADict()
		return self.iu_list



	def getIUData(self):
		self.lock.acquire()
		self.setDatabaseURN()
		self.withholded = self.filterAllowedRowGUIDs()
		
		query = """
		SELECT DISTINCT
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
		columns = [column[0] for column in self.cur.description]
		
		iu_rows = self.cur.fetchall()
		self.lock.release()
		
		self.iu_list = []
		for row in iu_rows:
			self.iu_list.append(dict(zip(columns, row)))
		
		return self.iu_list


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


	def getChildIUAnalyses(self):
		self.iua_dict = {}
		for fieldname in ['Barcodes', 'FOGS', 'MAM_Measurements']:
			self.lock.acquire()
			
			query = """
			SELECT iua.[RowGUID]
			FROM [IdentificationUnit] iu
			INNER JOIN [IdentificationUnitAnalysis] iua
			ON iu.[CollectionSpecimenID] = iua.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = iua.[IdentificationUnitID]
			INNER JOIN [{0}] rg_temp
			ON iu.[RowGUID] = rg_temp.[rowguid_to_get]
			;""".format(self.get_temptable)
			
			querylog.info(query)
			self.cur.execute(query)
			
			rows = self.cur.fetchall()
			self.lock.release()
			
			row_guids = [row[0] for row in rows]
			iua_getter = IdentificationUnitAnalysisGetter(self.dc_config, fieldname, self.users_project_ids, withhold_set_before = True)
			iua_getter.getByRowGUIDs(row_guids)
			iua_getter.list2dict()
			
			self.lock.acquire()
			self.iua_dict[fieldname] = iua_getter.iua_dict
			self.lock.release()
		return


	def insertIUADict(self):
		for fieldname in self.iua_dict:
			for iu in self.iu_list:
				if iu['CollectionSpecimenID'] in self.iua_dict[fieldname] and iu['IdentificationUnitID'] in self.iua_dict[fieldname][iu['CollectionSpecimenID']]:
					if 'IdentificationUnitAnalyses' not in iu:
						iu['IdentificationUnitAnalyses'] = {}
					if fieldname not in iu['IdentificationUnitAnalyses']:
						iu['IdentificationUnitAnalyses'][fieldname] = []
					for iua_id in self.iua_dict[fieldname][iu['CollectionSpecimenID']][iu['IdentificationUnitID']]:
						iu['IdentificationUnitAnalyses'][fieldname].append(self.iua_dict[fieldname][iu['CollectionSpecimenID']][iu['IdentificationUnitID']][iua_id])
		
		return


	def getChildIdentifications(self):
		self.lock.acquire()
		
		query = """
		SELECT i.[RowGUID]
		FROM [IdentificationUnit] iu
		INNER JOIN [Identification] i
		ON iu.[CollectionSpecimenID] = i.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = i.[IdentificationUnitID]
		INNER JOIN [{0}] rg_temp
		ON iu.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		
		rows = self.cur.fetchall()
		row_guids = [row[0] for row in rows]
		self.lock.release()
		
		i_getter = IdentificationGetter(self.dc_config, self.users_project_ids)
		i_getter.getByRowGUIDs(row_guids)
		i_getter.list2dict()
		
		self.i_dict = i_getter.i_dict
	
		return


	def insertIdentificationDict(self):
		for iu in self.iu_list:
			if iu['CollectionSpecimenID'] in self.i_dict and iu['IdentificationUnitID'] in self.i_dict[iu['CollectionSpecimenID']]:
				if 'Identifications' not in iu:
					iu['Identifications'] = []
				for i_id in self.i_dict[iu['CollectionSpecimenID']][iu['IdentificationUnitID']]:
					iu['Identifications'].append(self.i_dict[iu['CollectionSpecimenID']][iu['IdentificationUnitID']][i_id])
		
		return











