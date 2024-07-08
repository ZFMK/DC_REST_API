import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.CollectionEventMatcher import CollectionEventMatcher


class CollectionEventInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = 'event_temptable'
		self.unique_events_temptable = 'unique_events_temptable'
		
		self.schema = [
			{'colname': 'event_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID'},
			{'colname': 'CollectionEventID'},
			{'colname': 'CollectorsEventNumber'},
			{'colname': 'CollectionDate'},
			{'colname': 'CollectionDay'},
			{'colname': 'CollectionMonth'},
			{'colname': 'CollectionYear'},
			{'colname': 'CollectionDateSupplement'},
			{'colname': 'CollectionDateCategory'},
			{'colname': 'CollectionEndDay'},
			{'colname': 'CollectionEndMonth'},
			{'colname': 'CollectionEndYear'},
			{'colname': 'LocalityDescription'},
			{'colname': 'LocalityDescription_sha', 'compute sha of': 'LocalityDescription'},
			{'colname': 'LocalityVerbatim'},
			{'colname': 'LocalityVerbatim_sha', 'compute sha of': 'LocalityVerbatim'},
			{'colname': 'HabitatDescription'},
			{'colname': 'HabitatDescription_sha', 'compute sha of': 'HabitatDescription'},
			{'colname': 'CollectingMethod'},
			{'colname': 'CollectingMethod_sha', 'compute sha of': 'CollectingMethod'},
			{'colname': 'ReferenceTitle'},
			{'colname': 'ReferenceURI'},
			# {'colname': 'ReferenceDetails'},
			{'colname': 'Notes'},
			{'colname': 'Country'},
			#################
			{'colname': 'State'},
			{'colname': 'StateDistrict'},
			{'colname': 'County'},
			{'colname': 'Municipality'},
			{'colname': 'StreetHouseNumber'},
			#################
			#  lsID = 4
			{'colname': 'Altitude'},
			{'colname': 'Altitude_Accuracy'},
			# lsID = 8
			{'colname': 'WGS84_Lat'},
			{'colname': 'WGS84_Lon'},
			{'colname': 'WGS84_Accuracy'},
			{'colname': 'WGS84_RecordingMethod'},
			# lsID = 14
			{'colname': 'Depth_min_m'},
			{'colname': 'Depth_max_m'},
			{'colname': 'Depth_Accuracy_m'},
			{'colname': 'Depth_RecordingMethod_m'},
			# lsID = 15
			{'colname': 'Height_min_m'},
			{'colname': 'Height_max_m'},
			{'colname': 'Height_Accuracy_m'},
			{'colname': 'Height_RecordingMethod_m'},
			{'colname': 'DataWithholdingReason'},
			{'colname': 'DataWithholdingReasonDate'}
		]
		
		self.json2temp = JSON2TempTable(self.dc_db, self.schema)


	def insertCollectionEventData(self, event_data_dicts = []):
		pudb.set_trace()
		self.__createEventTempTable()
		
		self.json2temp.set_datadicts(self.e_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__addEventSHA()
		
		self.event_matcher = CollectionEventMatcher(self.dc_db, self.temptable)
		self.event_matcher.matchExistingEvents()
		
		self.createNewEvents()
		
		
		
		
		
		self.__updateImportTempTableEventIDs()
		self.__insertEventIDsInCollectionSpecimen()
		self.__deleteUnconnectedEvents()
		return


	def setCollectionEventDicts(self, json_dicts = []):
		self.e_dicts = []
		e_count = 1
		for e_dict in json_dicts:
			e_dict['event_num'] = e_count
			e_count += 1
			self.e_dicts.append(e_dict)
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
		[event_num] INT NOT NULL,
		[CollectionSpecimenID] INT,
		[CollectionEventID] INT DEFAULT NULL,
		[RowGUID] UNIQUEIDENTIFIER,
		[CollectorsEventNumber] VARCHAR(50) COLLATE {1},
		[CollectionDate] DATETIME,
		[CollectionDay] TINYINT,
		[CollectionMonth] TINYINT,
		[CollectionYear] SMALLINT,
		[CollectionEndDay] TINYINT,
		[CollectionEndMonth] TINYINT,
		[CollectionEndYear] SMALLINT,
		[CollectionDateSupplement] VARCHAR(100) COLLATE {1},
		[CollectionDateCategory] NVARCHAR(50) COLLATE {1},
		[CollectionTime] VARCHAR(50) COLLATE {1},
		[CollectionTimeSpan] VARCHAR(50) COLLATE {1},
		[LocalityDescription] VARCHAR(MAX) COLLATE {1},
		[LocalityDescription_sha] VARCHAR(50),
		[LocalityVerbatim] VARCHAR(MAX) COLLATE {1},
		[LocalityVerbatim_sha] VARCHAR(50),
		[HabitatDescription] VARCHAR(MAX) COLLATE {1},
		[HabitatDescription_sha] VARCHAR(50),
		[ReferenceTitle] NVARCHAR(255),
		[ReferenceURI] VARCHAR(255),
		 -- [ReferenceDetails] NVARCHAR(50),
		[CollectingMethod] VARCHAR(MAX) COLLATE {1},
		[CollectingMethod_sha] VARCHAR(50),
		[Notes] VARCHAR(MAX) COLLATE {1},
		 -- 
		[Country] VARCHAR(255) COLLATE {1},
		[State] VARCHAR(255) COLLATE {1},
		[StateDistrict] VARCHAR(255) COLLATE {1},
		[County] VARCHAR(255) COLLATE {1},
		[Municipality] VARCHAR(255) COLLATE {1},
		[StreetHouseNumber] VARCHAR(255) COLLATE {1},
		 -- 
		[Altitude] VARCHAR(255) COLLATE {1},
		[Altitude_Accuracy] VARCHAR(255) COLLATE {1},
		 -- 
		[WGS84_Lat] VARCHAR(255) COLLATE {1},
		[WGS84_Lon] VARCHAR(255) COLLATE {1},
		[WGS84_Accuracy] VARCHAR(50) COLLATE {1},
		[WGS84_RecordingMethod] NVARCHAR(500) COLLATE {1},
		 -- 
		[Depth_min_m] VARCHAR(255) COLLATE {1},
		[Depth_max_m] VARCHAR(255) COLLATE {1},
		[Depth_Accuracy_m] VARCHAR(50) COLLATE {1},
		[Depth_RecordingMethod_m] VARCHAR(500) COLLATE {1},
		 -- 
		[Height_min_m] VARCHAR(255) COLLATE {1},
		[Height_max_m] VARCHAR(255) COLLATE {1},
		[Height_Accuracy_m] VARCHAR(50) COLLATE {1},
		[Height_RecordingMethod_m] VARCHAR(500) COLLATE {1},
		 -- 
		[DataWithholdingReason] NVARCHAR(255) COLLATE {1},
		[DataWithholdingReasonDate] NVARCHAR(50) COLLATE {1},
		 -- 
		[event_sha] VARCHAR(50),
		 -- 
		PRIMARY KEY ([event_num]),
		INDEX [event_sha_idx] ([event_sha]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [CollectionEventID_idx] ([CollectionEventID]),
		INDEX [CollectorsEventNumber_idx] ([CollectorsEventNumber])
		)
		;""".format(self.temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __addEventSHA(self):
		query = """
		UPDATE ce_temp
		SET [event_sha] = CONVERT(VARCHAR(50), HASHBYTES('sha2_256', CONCAT(
			[CollectorsEventNumber],
			[CollectionDate],
			[CollectionDay],
			[CollectionMonth],
			[CollectionYear],
			[CollectionEndDay],
			[CollectionEndMonth],
			[CollectionEndYear],
			[CollectionDateSupplement],
			[CollectionDateCategory],
			[CollectionTime],
			[CollectionTimeSpan],
			[LocalityDescription_sha],
			[LocalityVerbatim_sha],
			[HabitatDescription_sha],
			[ReferenceTitle],
			[ReferenceURI],
			 -- e.[ReferenceDetails],
			[CollectingMethod_sha],
			[Country],
			 -- 
			[Altitude],
			[Altitude_Accuracy],
			 --
			[WGS84_Lat],
			[WGS84_Lon],
			[WGS84_Accuracy],
			[WGS84_RecordingMethod],
			 -- 
			[Depth_min_m],
			[Depth_max_m],
			[Depth_Accuracy_m],
			[Depth_RecordingMethod_m],
			 -- 
			[Height_min_m],
			[Height_max_m],
			[Height_Accuracy_m],
			[Height_RecordingMethod_m]
		)), 2)
		FROM [{0}] ce_temp
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __createEventNumbersTempTable(self):
		"""
		a table that holds the event_numbers and event IDs
		"""
		
		self.event_num_match_table = 'event_num_matches_event'
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.event_num_match_table)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
			[event_num] INT NOT NULL,
			[CollectionEventID] INT,
			[RowGUID] UNIQUEIDENTIFIER,
			PRIMARY KEY ([event_num]),
			INDEX [CollectionEventID_idx] ([CollectionEventID]),
			INDEX [RowGUID_idx] (RowGUID)
		)
		""".format(self.event_num_match_table)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [{0}] ([event_num])
		SELECT [event_num]
		FROM [{1}]
		;""".format(self.event_num_match_table, self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return



	def __createLocalisationSystemsTable(self):
		query = """
		CREATE TABLE localisations_temp (
			[CollectionEventID] INT NOT NULL,
			
		)
		
		
		"""
		






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

	def createNewEvents(self):
		# insert only one version of each event when the same event occurres multiple times in json data
		self.__setUniqueEventsTempTable()
		self.__insertNewCollectionEvents()
		
		self.__insertEventLocalisationWGS84()
		self.__insertEventLocalisationAltitude()
		self.__insertEventLocalisationDepth()
		self.__insertEventLocalisationHeight()
		
		#self.__insertEventLocalisationNamedArea()
		
		self.__updateEventIDs()
		return


	def __setUniqueEventsTempTable(self):
		query = """
		CREAT TABLE [{0}] (
			[CollectionEventID] INT,
			[CollectorsEventNumber] NVARCHAR(50),
			[CollectionDate] DATETIME,
			[CollectionDay] TINYINT,
			[CollectionMonth] TINYINT,
			[CollectionYear] SMALLINT,
			[CollectionEndDay] TINYINT,
			[CollectionEndMonth] TINYINT,
			[CollectionEndYear] SMALLINT,
			[CollectionDateSupplement] NVARCHAR(100),
			[CollectionDateCategory] NVARCHAR(50),
			[CollectionTime] VARCHAR(50),
			[CollectionTimeSpan] VARCHAR(50),
			[LocalityDescription_sha] VARCHAR(50),
			[LocalityVerbatim_sha] VARCHAR(50),
			[HabitatDescription_sha] VARCHAR(50),
			[ReferenceTitle] NVARCHAR(255),
			[ReferenceURI] VARCHAR(255),
			 -- [ReferenceDetails] NVARCHAR(50),
			[CollectingMethod_sha] VARCHAR(50),
			 -- [Notes_sha] VARCHAR(50),
			[CountryCache] NVARCHAR(50),
			[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
			 -- 
			[event_sha] VARCHAR(50),
			 -- 
			[Altitude] VARCHAR(255) COLLATE {1},
			[Altitude_Accuracy] VARCHAR(255) COLLATE {1},
			 --
			[WGS84_Lat] VARCHAR(255) COLLATE {1},
			[WGS84_Lon] VARCHAR(255) COLLATE {1},
			[WGS84_Accuracy] VARCHAR(50) COLLATE {1},
			[WGS84_RecordingMethod] NVARCHAR(500) COLLATE {1},
			 -- 
			[Depth_min_m] VARCHAR(255) COLLATE {1},
			[Depth_max_m] VARCHAR(255) COLLATE {1},
			[Depth_Accuracy_m] VARCHAR(50) COLLATE {1},
			[Depth_RecordingMethod_m] VARCHAR(500) COLLATE {1},
			 -- 
			[Height_min_m] VARCHAR(255) COLLATE {1},
			[Height_max_m] VARCHAR(255) COLLATE {1},
			[Height_Accuracy_m] VARCHAR(50) COLLATE {1},
			[Height_RecordingMethod_m] VARCHAR(500) COLLATE {1},
			INDEX [event_sha_idx] ([event_sha]),
			INDEX [RowGUID_idx] ([RowGUID])
			)
		"""
		
		query = """
		SELECT DISTINCT
			ce_temp.[CollectorsEventNumber],
			ce_temp.[CollectionDate],
			ce_temp.[CollectionDay],
			ce_temp.[CollectionMonth],
			ce_temp.[CollectionYear],
			ce_temp.[CollectionEndDay],
			ce_temp.[CollectionEndMonth], 
			ce_temp.[CollectionEndYear],
			ce_temp.[CollectionDateSupplement],
			ce_temp.[CollectionDateCategory],
			ce_temp.[CollectionTime],
			ce_temp.[CollectionTimeSpan],
			ce_temp.[LocalityDescription],
			ce_temp.[HabitatDescription], 
			ce_temp.[ReferenceTitle],
			ce_temp.[ReferenceURI],
			 -- ce_temp.[ReferenceDetails],
			ce_temp.[LocalityVerbatim],
			ce_temp.[CollectingMethod],
			ce_temp.[Notes],
			ce_temp.[Country],
			 -- 
			ce_temp.[event_sha]
			 -- 
			ce_temp.[Altitude],
			ce_temp.[Altitude_Accuracy],
			 --
			ce_temp.[WGS84_Lat],
			ce_temp.[WGS84_Lon],
			ce_temp.[WGS84_Accuracy],
			ce_temp.[WGS84_RecordingMethod],
			 -- 
			ce_temp.[Depth_min_m],
			ce_temp.[Depth_max_m],
			ce_temp.[Depth_Accuracy_m],
			ce_temp.[Depth_RecordingMethod_m],
			 -- 
			ce_temp.[Height_min_m],
			ce_temp.[Height_max_m],
			ce_temp.[Height_Accuracy_m],
			ce_temp.[Height_RecordingMethod_m]
		INTO [{0}]
		FROM [{1}] ce_temp
		WHERE ce_temp.[CollectionEventID] IS NULL
		;""".format(self.unique_events_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return




	def __insertNewCollectionEvents(self):
		query = """
		INSERT INTO [CollectionEvent] (
			[CollectorsEventNumber],
			[CollectionDate],
			[CollectionDay],
			[CollectionMonth],
			[CollectionYear],
			[CollectionEndDay],
			[CollectionEndMonth],
			[CollectionEndYear],
			[CollectionDateSupplement],
			[CollectionDateCategory],
			[CollectionTime],
			[CollectionTimeSpan],
			[LocalityDescription],
			[LocalityVerbatim],
			[HabitatDescription],
			[ReferenceTitle],
			[ReferenceURI],
			 -- [ReferenceDetails],
			[CollectingMethod],
			[Notes],
			[CountryCache],
			[RowGUID]
		)
		OUTPUT INSERTED.[CollectionEventID], INSERTED.[RowGUID] INTO [#new_event_ids]
		SELECT DISTINCT -- insert only one version of each event when the same event occurres multiple times in json data
			ue_temp.[CollectorsEventNumber],
			ue_temp.[CollectionDate],
			ue_temp.[CollectionDay],
			ue_temp.[CollectionMonth],
			ue_temp.[CollectionYear],
			ue_temp.[CollectionEndDay],
			ue_temp.[CollectionEndMonth], 
			ue_temp.[CollectionEndYear],
			ue_temp.[CollectionDateSupplement],
			ue_temp.[CollectionDateCategory],
			ue_temp.[CollectionTime],
			ue_temp.[CollectionTimeSpan],
			ue_temp.[LocalityDescription],
			ue_temp.[HabitatDescription], 
			ue_temp.[ReferenceTitle],
			ue_temp.[ReferenceURI],
			 -- ue_temp.[ReferenceDetails],
			ue_temp.[LocalityVerbatim],
			ue_temp.[CollectingMethod],
			ue_temp.[Notes],
			ue_temp.[Country],
			ue_temp.[RowGUID]
		FROM [{0}] ue_temp
		;""".format(self.unique_events_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE ue_temp
		SET ue_temp.[CollectionEventID] = ce.[CollectionEventID]
		FROM [{0}]
		INNER JOIN [CollectionEvent] ce
		ON ce.[RowGUID] = ue_temp.[RowGUID]
		;""".format(self.unique_events_temptable)
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
			ue_temp.[CollectionEventID],
			ue_temp.[WGS84_Lat],
			ue_temp.[WGS84_Lon],
			ue_temp.[WGS84_Accuracy],
			ue_temp.[WGS84_RecordingMethod],
			ls.[LocalisationSystemID]
		FROM [{0}] ue_temp
		INNER JOIN [LocalisationSystem] ls
		ON ls.LocalisationSystemName = 'Coordinates WGS84'
		;""".format(self.unique_events_temptable)
		
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
			ue_temp.[CollectionEventID],
			ue_temp.[Altitude],
			ue_temp.[Altitude_Accuracy],
			ls.[LocalisationSystemID]
		FROM [{0}] ue_temp
		INNER JOIN [LocalisationSystem] ls
		ON ls.LocalisationSystemName = 'Altitude (mNN)'
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertEventLocalisationDepth(self):
		query = """
		INSERT INTO CollectionEventLocalisation (
			[CollectionEventID],
			[Location1],
			[LocationAccuracy],
			[RecordingMethod],
			[LocalisationSystemID]
		)
		SELECT 
			ue_temp.[CollectionEventID],
			ue_temp.[Depth_min_m],
			ue_temp.[Depth_max_m],
			ue_temp.[Depth_Accuracy_m],
			ue_temp.[Depth_RecordingMethod_m],
			ls.[LocalisationSystemID]
		FROM [{0}] ue_temp
		INNER JOIN [LocalisationSystem] ls
		ON ls.LocalisationSystemName = 'Depth'
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertEventLocalisationHeight(self):
		query = """
		INSERT INTO CollectionEventLocalisation (
			[CollectionEventID],
			[Location1],
			[LocationAccuracy],
			[RecordingMethod],
			[LocalisationSystemID]
		)
		SELECT 
			ue_temp.[CollectionEventID],
			ue_temp.[Height_min_m],
			ue_temp.[Height_max_m],
			ue_temp.[Height_Accuracy_m],
			ue_temp.[Height_RecordingMethod_m],
			ls.[LocalisationSystemID]
		FROM [{0}] ue_temp
		INNER JOIN [LocalisationSystem] ls
		ON ls.LocalisationSystemName = 'Height'
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	'''
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
		INNER JOIN [LocalisationSystem] ls
		ON ls.LocalisationSystemName = 'Named area (DiversityGazetteer)'
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return
	'''


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






