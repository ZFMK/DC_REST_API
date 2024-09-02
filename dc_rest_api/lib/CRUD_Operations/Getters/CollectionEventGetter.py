import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter

class CollectionEventGetter(DataGetter):
	def __init__(self, dc_db, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
		self.withholded = []
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_ce_temptable'



	def getByPrimaryKeys(self, ce_ids):
		self.createGetTempTable()
		
		batchsize = 1000
		while len(ce_ids) > 0:
			cached_ids = ce_ids[:batchsize]
			del ce_ids[:batchsize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			DROP TABLE IF EXISTS [#ce_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#ce_pks_to_get_temptable] (
				[CollectionEventID] INT NOT NULL,
				INDEX [CollectionEventID_idx] ([CollectionEventID])
			)
			;""".format(self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#ce_pks_to_get_temptable] (
				[CollectionEventID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [CollectionEvent] ce
			INNER JOIN [#ce_pks_to_get_temptable] pks
			ON pks.[CollectionEventID] = ce.[CollectionEventID]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		collectionevents = self.getData()
		
		return collectionevents


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		collectionevents = self.getData()
		
		return collectionevents



	def getData(self):
		
		self.withholded = self.filterAllowedRowGUIDs()
		
		query = """
		SELECT
		g_temp.[row_num],
		g_temp.[rowguid_to_get] AS [RowGUID],
		ce.[CollectionEventID],
		ce.[CollectorsEventNumber],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionDate], NULL) AS [CollectionDate],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionDay], NULL) AS [CollectionDay],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionMonth], NULL) AS [CollectionMonth],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionYear], NULL) AS [CollectionYear],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionEndDay], NULL) AS [CollectionEndDay],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionEndMonth], NULL) AS [CollectionEndMonth],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionEndYear], NULL) AS [CollectionEndYear],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionDateSupplement], NULL) AS [CollectionDateSupplement],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionDateCategory], NULL) AS [CollectionDateCategory],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionTime], NULL) AS [CollectionTime],
		IIF(g_temp.[WithholdDate] IS NULL, ce.[CollectionTimeSpan], NULL) AS [CollectionTimeSpan],
		ce.[ReferenceTitle],
		ce.[ReferenceURI],
		 -- e.[ReferenceDetails],
		ce.[CountryCache],
		 -- 
		alt.Location1 AS [Altitude],
		alt.LocationAccuracy AS [Altitude_Accuracy],
		 --
		wgs.Location2 AS [WGS84_Lat],
		wgs.Location1 AS [WGS84_Lon],
		wgs.LocationAccuracy AS [WGS84_Accuracy],
		wgs.RecordingMethod AS [WGS84_RecordingMethod],
		 -- 
		d.Location1 AS [Depth_min_m],
		d.Location2 AS [Depth_max_m],
		d.LocationAccuracy AS [Depth_Accuracy_m],
		d.RecordingMethod AS [Depth_RecordingMethod_m],
		 -- 
		h.Location1 AS [Height_min_m],
		h.Location2 AS [Height_max_m],
		h.LocationAccuracy AS [Height_Accuracy_m],
		h.RecordingMethod AS [Height_RecordingMethod_m]
		 --
		FROM [{0}] g_temp
		INNER JOIN [CollectionEvent] ce
		ON ce.[RowGUID] = g_temp.[rowguid_to_get]
		LEFT JOIN [CollectionEventLocalisation] alt
		ON alt.CollectionEventID = ce.CollectionEventID AND alt.LocalisationSystemID = 4
		LEFT JOIN [CollectionEventLocalisation] wgs
		ON wgs.CollectionEventID = ce.CollectionEventID AND wgs.LocalisationSystemID = 8
		LEFT JOIN [CollectionEventLocalisation] d
		ON d.CollectionEventID = ce.CollectionEventID AND d.LocalisationSystemID = 14
		LEFT JOIN [CollectionEventLocalisation] h
		ON h.CollectionEventID = ce.CollectionEventID AND h.LocalisationSystemID = 15
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.ce_rows = self.cur.fetchall()
		self.rows2list()
		
		return self.ce_list


	def rows2list(self):
		self.ce_list = []
		for row in self.ce_rows:
			self.ce_list.append(dict(zip(self.columns, row)))
		
		return


	def list2dict(self):
		self.ce_dict = {}
		for element in self.ce_list:
			if element['CollectionEventID'] not in self.ce_dict:
				self.ce_dict[element['CollectionEventID']] = {}
				
			self.ce_dict[element['CollectionEventID']] = element 


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		# the withholded variable keeps the IDs and RowGUIDs of the withholded rows
		withholded = []
		
		projectclause = self.getProjectClause()
		
		query = """
		SELECT ce.[CollectionEventID], ce.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [CollectionEvent] ce
		ON ce.RowGUID = g_temp.[rowguid_to_get]
		LEFT JOIN [CollectionSpecimen] cs 
		ON cs.[CollectionEventID] = ce.[CollectionEventID]
		LEFT JOIN [CollectionProject] cp
		ON cs.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE ce.[DataWithholdingReason] IS NOT NULL AND ce.[DataWithholdingReason] != '' {1}
		;""".format(self.get_temptable, projectclause)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		rows = self.cur.fetchall()
		for row in rows:
			withholded.append((row[0], row[1]))
		
		query = """
		DELETE g_temp
		FROM [{0}] g_temp
		INNER JOIN [CollectionEvent] ce
		ON ce.RowGUID = g_temp.[rowguid_to_get]
		LEFT JOIN [CollectionSpecimen] cs 
		ON cs.[CollectionEventID] = ce.[CollectionEventID]
		LEFT JOIN [CollectionProject] cp
		ON cs.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE ce.[DataWithholdingReason] IS NOT NULL AND ce.[DataWithholdingReason] != '' {1}
		;""".format(self.get_temptable, projectclause)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		# withhold the collection date when DataWithholdingReasonDate is set
		query = """
		ALTER TABLE [{0}]
		ADD [WithholdDate] BIT DEFAULT NULL
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		UPDATE g_temp
		SET 
			g_temp.[WithholdDate] = 1
		FROM [{0}] g_temp
		INNER JOIN [CollectionEvent] ce
		ON ce.RowGUID = g_temp.[rowguid_to_get]
		LEFT JOIN [CollectionSpecimen] cs 
		ON cs.[CollectionEventID] = ce.[CollectionEventID]
		LEFT JOIN [CollectionProject] cp
		ON cs.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE ce.[DataWithholdingReasonDate] IS NOT NULL AND ce.[DataWithholdingReasonDate] != '' {1}
		;""".format(self.get_temptable, projectclause)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return withholded











