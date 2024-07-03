import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.JSON2TempTable import JSON2TempTable


class CollectionEventImporter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#event_temptable'
		
		self.schema = [
			{'colname': 'dataset_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID'},
			{'colname': 'CollectionEventID'},
			{'colname': 'CollectorsEventNumber'},
			{'colname': 'CollectionDay'},
			{'colname': 'CollectionMonth'},
			{'colname': 'CollectionYear'},
			{'colname': 'CollectionDateSupplement'},
			{'colname': 'CollectionEndDay'},
			{'colname': 'CollectionEndMonth'},
			{'colname': 'CollectionEndYear'},
			{'colname': 'LocalityDescription'},
			{'colname': 'LocalityVerbatim'},
			{'colname': 'HabitatDescription'},
			{'colname': 'CollectingMethod'},
			{'colname': 'Notes'},
			{'colname': 'Country'},
			{'colname': 'State'},
			{'colname': 'StateDistrict'},
			{'colname': 'County'},
			{'colname': 'Municipality'},
			{'colname': 'StreetHouseNumber'},
			{'colname': 'Altitude'},
			{'colname': 'Altitude_Accuracy'},
			{'colname': 'WGS84_Lat'},
			{'colname': 'WGS84_Lon'},
			{'colname': 'WGS84_Accuracy'},
			{'colname': 'WGS84_RecordingMethod'},
			{'colname': 'ce_DataWithholdingReason'}
		]
		
		self.json2temp = JSON2TempTable(self.dc_db, self.schema)


	def insertEventData(self, event_data_dicts = []):
		self.set_datadicts(event_data_dicts)
		self.__createEventTempTable()
		self.fill_temptable()
		self.__setExistingEvents()
		self.__createMissingEvents()
		self.__updateCollectionEvents()
		self.__insertEventLocalisationWGS84()
		self.__insertEventLocalisationAltitude()
		self.__insertEventLocalisationNamedArea()
		self.__updateImportTempTableEventIDs()
		self.__insertEventIDsInCollectionSpecimen()
		self.__deleteUnconnectedEvents()
		return


	def __createEventTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[dataset_num] INT NOT NULL,
		[CollectionSpecimenID] INT DEFAULT NULL,
		[CollectionEventID] INT DEFAULT NULL,
		[CollectorsEventNumber] VARCHAR(50) COLLATE {1},
		[CollectionDay] TINYINT,
		[CollectionMonth] TINYINT,
		[CollectionYear] SMALLINT,
		[CollectionEndDay] TINYINT,
		[CollectionEndMonth] TINYINT,
		[CollectionEndYear] SMALLINT,
		[CollectionDateSupplement] VARCHAR(100) COLLATE {1},
		[LocalityDescription] VARCHAR(MAX) COLLATE {1},
		[LocalityVerbatim] VARCHAR(MAX) COLLATE {1},
		[HabitatDescription] VARCHAR(MAX) COLLATE {1},
		[CollectingMethod] VARCHAR(MAX) COLLATE {1},
		[Notes] VARCHAR(MAX) COLLATE {1},
		[Country] VARCHAR(255) COLLATE {1},
		[State] VARCHAR(255) COLLATE {1},
		[StateDistrict] VARCHAR(255) COLLATE {1},
		[County] VARCHAR(255) COLLATE {1},
		[Municipality] VARCHAR(255) COLLATE {1},
		[StreetHouseNumber] VARCHAR(255) COLLATE {1},
		[Altitude] VARCHAR(255) COLLATE {1},
		[Altitude_Accuracy] VARCHAR(255) COLLATE {1},
		[WGS84_Lat] VARCHAR(255) COLLATE {1},
		[WGS84_Lon] VARCHAR(255) COLLATE {1},
		[WGS84_Accuracy] VARCHAR(255) COLLATE {1},
		[WGS84_RecordingMethod] VARCHAR(500) COLLATE {1},
		[ce_DataWithholdingReason] VARCHAR(255) COLLATE {1},
		PRIMARY KEY ([dataset_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [CollectionEventID_idx] ([CollectionEventID]),
		INDEX [CollectorsEventNumber_idx] ([CollectorsEventNumber])
		)
		;""".format(self.temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __setExistingEvents(self):
		query = """
		UPDATE ce_temp
		SET ce_temp.[CollectionEventID] = CollectionEvent.[CollectionEventID]
		FROM [{0}] ce_temp
		INNER JOIN (CollectionEvent
			LEFT JOIN (CollectionEventLocalisation as height
				INNER JOIN LocalisationSystem lh
				ON (height.LocalisationSystemID = lh.LocalisationSystemID AND lh.LocalisationSystemName = 'Altitude (mNN)'))
			ON (CollectionEvent.CollectionEventID = height.CollectionEventID)
			LEFT JOIN (CollectionEventLocalisation as coord
				INNER JOIN LocalisationSystem lcoord
				ON (coord.LocalisationSystemID = lcoord.LocalisationSystemID AND lcoord.LocalisationSystemName = 'Coordinates WGS84'))
			ON (CollectionEvent.CollectionEventID = coord.CollectionEventID))
		ON (
			((ce_temp.[LocalityDescription] = [CollectionEvent].[LocalityDescription]) OR (ce_temp.[LocalityDescription] IS NULL AND [CollectionEvent].[LocalityDescription] IS NULL))
			AND ((ce_temp.[LocalityVerbatim] = [CollectionEvent].[LocalityVerbatim]) OR (ce_temp.[LocalityVerbatim] IS NULL AND [CollectionEvent].[LocalityVerbatim] IS NULL))
			AND ((ce_temp.[Country] = [CollectionEvent].[CountryCache]) OR (ce_temp.[Country] IS NULL AND [CollectionEvent].[CountryCache] IS NULL))
			AND ((ce_temp.[CollectionDay] = [CollectionEvent].[CollectionDay]) OR (ce_temp.[CollectionDay] IS NULL AND [CollectionEvent].[CollectionDay] IS NULL))
			AND ((ce_temp.[CollectionMonth] = [CollectionEvent].[CollectionMonth]) OR (ce_temp.[CollectionMonth] IS NULL AND [CollectionEvent].[CollectionMonth] IS NULL))
			AND ((ce_temp.[CollectionYear] = [CollectionEvent].[CollectionYear]) OR (ce_temp.[CollectionYear] IS NULL AND [CollectionEvent].[CollectionYear] IS NULL))
			AND ((ce_temp.[CollectionEndDay] = [CollectionEvent].[CollectionEndDay]) OR (ce_temp.[CollectionEndDay] IS NULL AND [CollectionEvent].[CollectionEndDay] IS NULL))
			AND ((ce_temp.[CollectionEndMonth] = [CollectionEvent].[CollectionEndMonth]) OR (ce_temp.[CollectionEndMonth] IS NULL AND [CollectionEvent].[CollectionEndMonth] IS NULL))
			AND ((ce_temp.[CollectionEndYear] = [CollectionEvent].[CollectionEndYear]) OR (ce_temp.[CollectionEndYear] IS NULL AND [CollectionEvent].[CollectionEndYear] IS NULL))
			AND ((ce_temp.[HabitatDescription] = [CollectionEvent].[HabitatDescription]) OR (ce_temp.[HabitatDescription] IS NULL AND [CollectionEvent].[HabitatDescription] IS NULL))
			AND ((ce_temp.[CollectingMethod] = [CollectionEvent].[CollectingMethod]) OR (ce_temp.[CollectingMethod] IS NULL AND [CollectionEvent].[CollectingMethod] IS NULL))
			AND ((ce_temp.[WGS84_Lat] = [coord].[Location2]) OR (ce_temp.[WGS84_Lat] IS NULL AND [coord].[Location2] IS NULL))
			AND ((ce_temp.[WGS84_Lon] = [coord].[Location1]) OR (ce_temp.[WGS84_Lon] IS NULL AND [coord].[Location1] IS NULL))
			AND ((ce_temp.[WGS84_Accuracy] = [coord].[LocationAccuracy]) OR (ce_temp.[WGS84_Accuracy] IS NULL AND [coord].[LocationAccuracy] IS NULL))
			AND ((ce_temp.[WGS84_RecordingMethod] = [coord].[RecordingMethod]) OR (ce_temp.[WGS84_RecordingMethod] IS NULL AND [coord].[RecordingMethod] IS NULL))
			AND ((ce_temp.[Altitude] = [height].[Location1]) OR (ce_temp.[Altitude] IS NULL AND [height].[Location1] IS NULL))
			AND ((ce_temp.[Altitude_Accuracy] = [height].[LocationAccuracy]) OR (ce_temp.[Altitude_Accuracy] IS NULL AND [height].[LocationAccuracy] IS NULL))
			)
		;
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateImportTempTableEventIDs(self):
		query = """
		UPDATE ids_temp
		SET ids_temp.[CollectionEventID] = ce_temp.[CollectionEventID]
		FROM [{0}] ids_temp
		INNER JOIN [{1}] ce_temp ON ids_temp.[dataset_num] = ce_temp.[dataset_num]
		WHERE ce_temp.[CollectionEventID] IS NOT NULL
		;""".format(self.ids_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return

	'''
	def __deleteExistingEvents(self):
		query = """
		DELETE FROM [{0}]
		WHERE [CollectionEventID] IS NOT NULL
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return
	'''

	def __createMissingEvents(self):
		query = """
		DROP TABLE IF EXISTS [#new_event_ids];
		"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#new_event_ids] (dataset_num INT NOT NULL UNIQUE, CollectionEventID INT NOT NULL UNIQUE);
		"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [CollectionEvent] 
		([LocalityDescription])
		OUTPUT INSERTED.[LocalityDescription], INSERTED.[CollectionEventID] INTO [#new_event_ids]
		SELECT ce_temp.[dataset_num]
		FROM [{0}] ce_temp
		WHERE ce_temp.[CollectionEventID] IS NULL
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		# update the CollectionEventIDs in event_temptable
		query = """
		UPDATE ce_temp
		SET ce_temp.CollectionEventID = nei.CollectionEventID
		FROM [{0}] ce_temp
		INNER JOIN [#new_event_ids] nei
		ON ce_temp.[dataset_num] = nei.[dataset_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateCollectionEvents(self):
		# update all newly inserted CollectionEvents with the values from event_temptable
		query = """
		UPDATE ce 
		SET
			[CollectorsEventNumber] = ce_temp.[CollectorsEventNumber],
			[CollectionDay] = ce_temp.[CollectionDay],
			[CollectionMonth] = ce_temp.[CollectionMonth],
			[CollectionYear] = ce_temp.[CollectionYear],
			[CollectionEndDay] = ce_temp.[CollectionEndDay],
			[CollectionEndMonth] = ce_temp.[CollectionEndMonth],
			[CollectionEndYear] = ce_temp.[CollectionEndYear],
			[CollectionDateSupplement] = ce_temp.[CollectionDateSupplement],
			[LocalityDescription] = ce_temp.[LocalityDescription],
			[LocalityVerbatim] = ce_temp.[LocalityVerbatim],
			[HabitatDescription] = ce_temp.[HabitatDescription],
			[CollectingMethod] = ce_temp.[CollectingMethod],
			[Notes] = ce_temp.[Notes],
			[CountryCache] = ce_temp.[Country]
		FROM [CollectionEvent] ce
		INNER JOIN [{0}] ce_temp
			ON (ce.[CollectionEventID] = ce_temp.[CollectionEventID])
		 -- 
		INNER JOIN [#new_event_ids] nei
			ON ce_temp.[dataset_num] = nei.[dataset_num]
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertEventLocalisationWGS84(self):
		query = """
		INSERT INTO CollectionEventLocalisation (
			[CollectionEventID],
			[Location2],
			[Location1],
			[LocationAccuracy],
			[RecordingMethod],
			[LocalisationSystemID]
		)
		SELECT 
			ce_temp.[CollectionEventID],
			ce_temp.[WGS84_Lat],
			ce_temp.[WGS84_Lon],
			ce_temp.[WGS84_Accuracy],
			ce_temp.[WGS84_RecordingMethod],
			ls.[LocalisationSystemID]
		FROM [{0}] ce_temp
		INNER JOIN [#new_event_ids] nei
			ON ce_temp.[dataset_num] = nei.[dataset_num]
		INNER JOIN [LocalisationSystem] ls
		ON ls.LocalisationSystemName = 'Coordinates WGS84'
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertEventLocalisationAltitude(self):
		query = """
		INSERT INTO CollectionEventLocalisation (
			[CollectionEventID],
			[Location1],
			[LocationAccuracy],
			[LocalisationSystemID]
		)
		SELECT 
			ce_temp.[CollectionEventID],
			ce_temp.[Altitude],
			ce_temp.[Altitude_Accuracy],
			ls.[LocalisationSystemID]
		FROM [{0}] ce_temp
		INNER JOIN [#new_event_ids] nei
			ON ce_temp.[dataset_num] = nei.[dataset_num]
		INNER JOIN [LocalisationSystem] ls
		ON ls.LocalisationSystemName = 'Altitude (mNN)'
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertEventLocalisationNamedArea(self):
		query = """
		INSERT INTO CollectionEventLocalisation (
			[CollectionEventID],
			[Location1],
			[LocalisationSystemID]
		)
		SELECT 
			ce_temp.[CollectionEventID],
			CONCAT_WS(', ', ce_temp.[Country], ce_temp.[State], ce_temp.[StateDistrict], ce_temp.[County], ce_temp.[Municipality], ce_temp.[StreetHouseNumber]),
			ls.[LocalisationSystemID]
		FROM [{0}] ce_temp
		INNER JOIN [#new_event_ids] nei
			ON ce_temp.[dataset_num] = nei.[dataset_num]
		INNER JOIN [LocalisationSystem] ls
		ON ls.LocalisationSystemName = 'Named area (DiversityGazetteer)'
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertEventIDsInCollectionSpecimen(self):
		query = """
		UPDATE cs
		SET cs.[CollectionEventID] = ids_temp.[CollectionEventID]
		FROM CollectionSpecimen cs
		INNER JOIN [{0}] ids_temp ON ids_temp.CollectionSpecimenID = cs.CollectionSpecimenID
		WHERE ids_temp.[CollectionEventID] IS NOT NULL AND ids_temp.CollectionSpecimenID IS NOT NULL
		""".format(self.ids_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return

	# is that needed when thinking of Events are the main table not CollectionSpecimen?
	def __deleteUnconnectedEvents(self):
		query = """
		DELETE cel
		FROM [CollectionEventLocalisation] cel
		INNER JOIN [CollectionEvent] ce ON cel.[CollectionEventID] = ce.[CollectionEventID]
		LEFT JOIN [CollectionSpecimen] cs ON cs.[CollectionEventID] = ce.[CollectionEventID]
		WHERE cs.[CollectionSpecimenID] IS NULL
		;"""
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DELETE ce
		FROM [CollectionEvent] ce
		LEFT JOIN [CollectionSpecimen] cs ON cs.[CollectionEventID] = ce.[CollectionEventID]
		WHERE cs.[CollectionSpecimenID] IS NULL
		;"""
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return






