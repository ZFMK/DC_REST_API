import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from threading import Thread, Lock

from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
from dc_rest_api.lib.CRUD_Operations.Getters.AnalysisMethodParameterFilter import AnalysisMethodParameterFilter

#from dc_rest_api.lib.CRUD_Operations.Getters.IdentificationUnitAnalysisMethodParameterGetter import IdentificationUnitAnalysisMethodParameterGetter

from DBConnectors.MSSQLConnector import MSSQLConnector


class IdentificationUnitAnalysisMethodGetter(DataGetter):
	"""
	IdentificationUnitAnalysisGetter needs to filter the IDs of the wanted methods and parameters, otherwise the mass of data belonging to an analysis might overwelm the 
	servers capacity
	The filtering is configured in AnalysisMethodParameterFilter. AnalysisMethodParameterFilter also generates a temporary table with the IDs that is used here
	to join against the Method and Parameter tables.
	Analyses, Methods and Parameters are requested separately because otherwise with a big join data like AnalysisResult are selected in multiple rows
	and thus cause a large overhead of data transfer
	"""
	
	def __init__(self, dc_config, fieldname, users_project_ids = [], amp_filter_temptable = None, withhold_set_before = False):
		self.dc_config = dc_config
		self.dc_db = MSSQLConnector(config = self.dc_config)
		
		DataGetter.__init__(self, self.dc_db)
		
		self.fieldname = fieldname
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_iuam_temptable'
		
		self.amp_filter_temptable = amp_filter_temptable
		if self.amp_filter_temptable is None:
			amp_filters = AnalysisMethodParameterFilter(self.dc_db, self.fieldname)
			self.amp_filter_temptable = amp_filters.amp_filter_temptable
		
		self.withhold_set_before = withhold_set_before
		
		self.withholded = []


	def getByPrimaryKeys(self, iuam_ids):
		# does this make sense here when the number of IDs is rapidly increasing with every sub table of IdentificationUnitAnalysis?
		
		self.createGetTempTable()
		
		batchsize = 400
		while len(iuam_ids) > 0:
			cached_ids = iuam_ids[:batchsize]
			del iuam_ids[:batchsize]
			placeholders = ['(?, ?, ?, ?, ?, ?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			DROP TABLE IF EXISTS [#iuam_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#iuam_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[IdentificationUnitID] INT NOT NULL,
				[AnalysisID] INT NOT NULL,
				[AnalysisNumber] NVARCHAR(50) NOT NULL COLLATE {0},
				[MethodID] INT NOT NULL,
				[MethodMarker] NVARCHAR(50) NOT NULL COLLATE {0},
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
				INDEX [AnalysisID_idx] ([AnalysisID]),
				INDEX [AnalysisNumber_idx] ([AnalysisNumber]),
				INDEX [MethodID_idx] ([MethodID]),
				INDEX [MethodMarker_idx] ([MethodMarker])
			)
			;""".format(self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#iuam_pks_to_get_temptable] (
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber],
			[MethodID],
			[MethodMarker]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [IdentificationUnitAnalysis] iua
			INNER JOIN [#iua_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = iua.[CollectionSpecimenID]
			AND pks.[IdentificationUnitID] = iua.[IdentificationUnitID]
			AND pks.[AnalysisID] = iua.[AnalysisID]
			AND pks.[AnalysisNumber] = iua.[AnalysisNumber] COLLATE {1}
			AND pks.[MethodID] = iuamp.[MethodID]
			AND pks.[MethodMarker] = iuamp.[MethodMarker] COLLATE {1}
			;""".format(self.get_temptable, self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		iuams = self.getData()
		
		return iuams


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		iuams = self.getData()
		
		return iuams


	def getData(self):
		self.lock = Lock()
		
		iuam_getter_thread = Thread(target = self.getIUAMData)
		#iuamp_getter_thread = Thread(target = self.getChildParameters)
		
		iuam_getter_thread.start()
		#iuamp_getter_thread.start()
		
		iuam_getter_thread.join()
		#iuamp_getter_thread.join()
		
		#self.insertParameterDicts()
		
		return self.iuam_list


	def getIUAMData(self):
		self.lock.acquire()
		
		self.setDatabaseURN()
		if self.withhold_set_before is not True:
			self.withholded = self.filterAllowedRowGUIDs()
		
		query = """
		SELECT DISTINCT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		iuam.[CollectionSpecimenID],
		iuam.[IdentificationUnitID],
		iuam.[AnalysisID],
		iuam.[AnalysisNumber],
		iuam.MethodID,
		iuam.MethodMarker,
		m.DisplayText AS MethodDisplay,
		m.Description AS MethodDescription,
		m.Notes AS MethodTypeNotes
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
			ON g_temp.[rowguid_to_get] = iuam.[RowGUID]
		INNER JOIN [MethodForAnalysis] mfa
			ON iuam.MethodID = mfa.MethodID AND iuam.AnalysisID = mfa.AnalysisID
		INNER JOIN [Method] m
			ON mfa.MethodID = m.MethodID
		INNER JOIN [{1}] amp_filter
			ON amp_filter.AnalysisID = iuam.AnalysisID AND amp_filter.MethodID = iuam.MethodID
		;""".format(self.get_temptable, self.amp_filter_temptable)
		self.cur.execute(query)
		columns = [column[0] for column in self.cur.description]
		
		iuam_rows = self.cur.fetchall()
		self.lock.release()
		
		self.iuam_list = []
		for row in iuam_rows:
			self.iuam_list.append(dict(zip(columns, row)))
		
		return self.iuam_list


	def list2dict(self):
		self.iuam_dict = {}
		for element in self.iuam_list:
			if element['CollectionSpecimenID'] not in self.iuam_dict:
				self.iuam_dict[element['CollectionSpecimenID']] = {}
			if element['IdentificationUnitID'] not in self.iuam_dict[element['CollectionSpecimenID']]:
				self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']] = {}
			if element['AnalysisID'] not in self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']]:
				self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']] = {}
			if element['AnalysisNumber'] not in self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']]:
				self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']] = {}
			if element['MethodID'] not in self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']]:
				self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']] = {}
			if element['MethodMarker'] not in self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']]:
				self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']][element['MethodMarker']] = {}
				
			self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']][element['MethodMarker']] = element 
		
		return


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		# the withholded list keeps the IDs and RowGUIDs of the withholded rows
		withholded = []
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
		query = """
		SELECT iuam.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON iuam.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [IdentificationUnit] iu
		ON iu.[CollectionSpecimenID] = iuam.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = iuam.[IdentificationUnitID]
		INNER JOIN [CollectionSpecimen] cs ON iuam.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
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
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON iuam.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [IdentificationUnit] iu
		ON iu.[CollectionSpecimenID] = iuam.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = iuam.[IdentificationUnitID]
		INNER JOIN [CollectionSpecimen] cs ON iuam.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		{1}
		WHERE (iu.[DataWithholdingReason] IS NOT NULL AND iu.[DataWithholdingReason] != '')
		OR (cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '')
		{2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return withholded


	def setChildParameters(self):
		self.lock.acquire()
		
		query = """
		SELECT iuamp.[RowGUID]
		FROM [IdentificationUnitAnalysisMethod] iuam
		INNER JOIN [IdentificationUnitAnalysisMethodParameter] iuamp
		ON iuam.[CollectionSpecimenID] = iuamp.[CollectionSpecimenID] AND iuam.[IdentificationUnitID] = iuamp.[IdentificationUnitID]
		AND iuam.[AnalysisID] = iuamp.[AnalysisID] AND iuam.[AnalysisNumber] = iuamp.[AnalysisNumber]
		AND iuam.[MethodID] = iuamp.[MethodID] AND iuam.[MethodMarker] = iuamp.[MethodMarker]
		INNER JOIN [{0}] amp_filter
			ON amp_filter.AnalysisID = iuam.AnalysisID AND amp_filter.MethodID = iuam.MethodID
		INNER JOIN [{1}] rg_temp
		ON iuam.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(self.amp_filter_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		self.lock.release()
		
		row_guids = [row[0] for row in rows]
		iuamp_getter = IdentificationUnitAnalysisMethodParameterGetter(self.dc_config, self.fieldname, self.users_project_ids)
		iuamp_getter.list2dict()
		self.iuamp_dict = iuamp_getter.iuamp_dict
		
		return


	def insertIUAMPDict(self):
		for iuam in self.iuam_list:
			if (iuam['CollectionSpecimenID'] in self.iuamp_dict 
			and iuam['IdentificationUnitID'] in self.iuamp_dict[iuam['CollectionSpecimenID']] 
			and iuam['AnalysisID'] in self.iuamp_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']]
			and iuam['AnalysisNumber'] in self.iuamp_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']][iuam['AnalysisID']]
			and iuam['MethodID'] in self.iuamp_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']][iuam['AnalysisID']][iuam['AnalysisNumber']]
			and iuam['MethodMarker'] in self.iuamp_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']][iuam['AnalysisID']][iuam['AnalysisNumber']][iuam['MethodID']]):
				if 'Parameters' not in iuam:
					iuam['Parameters'] = []
				
				for parameter_id in self.iuamp_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']][iuam['AnalysisID']][iuam['AnalysisNumber']][iuam['MethodID']][iuam['MethodMarker']]:
					iuam['Parameters'].append(self.iuamp_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']][iuam['AnalysisID']][iuam['AnalysisNumber']][iuam['MethodID']][iuam['MethodMarker']][parameter_id])
		
		return
		
		
		
		
		for iu in self.iu_list:
			if iu['CollectionSpecimenID'] in i_getter.i_dict and iu['IdentificationUnitID'] in i_getter.i_dict[iu['CollectionSpecimenID']]:
				if 'Identifications' not in iu:
					iu['Identifications'] = []
				for i_id in i_getter.i_dict[iu['CollectionSpecimenID']][iu['IdentificationUnitID']]:
					iu['Identifications'].append(i_getter.i_dict[iu['CollectionSpecimenID']][iu['IdentificationUnitID']][i_id])
		
		return











