import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Matchers.CollectionEventMatcher import CollectionEventMatcher


class CollectionEventInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#event_temptable'
		self.unique_events_temptable = '#unique_events_temptable'
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
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


	def insertCollectionEventData(self):
		self.__createEventTempTable()
		
		self.json2temp.set_datadicts(self.ce_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__updateCollectionDate()
		self.__addEventSHA()
		
		self.event_matcher = CollectionEventMatcher(self.dc_db, self.temptable)
		self.event_matcher.matchExistingEvents()
		
		self.createNewEvents()
		
		self.__insertEventIDsInCollectionSpecimen()
		#self.__deleteUnconnectedEvents()
		
		self.__updateCEDicts()
		return


	def setCollectionEventDicts(self, json_dicts = []):
		self.ce_dicts = []
		ce_count = 1
		for ce_dict in json_dicts:
			ce_dict['entry_num'] = ce_count
			ce_count += 1
			self.ce_dicts.append(ce_dict)
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
		[entry_num] INT NOT NULL,
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
		[LocalityDescription_sha] VARCHAR(64),
		[LocalityVerbatim] VARCHAR(MAX) COLLATE {1},
		[LocalityVerbatim_sha] VARCHAR(64),
		[HabitatDescription] VARCHAR(MAX) COLLATE {1},
		[HabitatDescription_sha] VARCHAR(64),
		[ReferenceTitle] NVARCHAR(255),
		[ReferenceURI] VARCHAR(255),
		 -- [ReferenceDetails] NVARCHAR(50),
		[CollectingMethod] VARCHAR(MAX) COLLATE {1},
		[CollectingMethod_sha] VARCHAR(64),
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
		[event_sha] VARCHAR(64),
		 -- 
		PRIMARY KEY ([entry_num]),
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


	def __updateCollectionDate(self):
		# because the CollectionEvent.trgInsCollectionEvent overwrites CollectionDate with NULL
		# when one of ColletcionDay, CollectionMonth, or CollectionYear is not given
		# copy them from CollectionDate when they are not there
		
		
		query = """
		UPDATE ce_temp 
		SET CollectionDay = DATEPART(day, CollectionDate),
		CollectionMonth = DATEPART(month, CollectionDate),
		CollectionYear = DATEPART(year, CollectionDate)
		FROM [{0}] ce_temp
		where ce_temp.CollectionDate IS NOT NULL AND (CollectionDay IS NULL OR CollectionMonth IS NULL OR CollectionYear IS NULL)
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		 -- taken from CollectionEvent.trgInsCollectionEvent
		UPDATE ce_temp 
		SET CollectionDate = 
		CASE WHEN ce_temp.CollectionMonth IS NULL OR ce_temp.CollectionDay IS NULL OR ce_temp.CollectionYear IS NULL
			THEN NULL
		ELSE CASE WHEN ISDATE(convert(varchar(40), cast(ce_temp.CollectionYear as varchar) + '-' 
			+ case when ce_temp.CollectionMonth < 10 then '0' else '' end + cast(ce_temp.CollectionMonth as varchar)  + '-' 
			+ case when ce_temp.CollectionDay < 10 then '0' else '' end + cast(ce_temp.CollectionDay as varchar) + 'T00:00:00.000Z', 127)) = 1
			AND ce_temp.CollectionYear > 1760
			AND ce_temp.CollectionMonth between 1 and 12
			AND ce_temp.CollectionDay between 1 and 31
		then cast(convert(varchar(40), cast(ce_temp.CollectionYear as varchar) + '-' 
			+ case when ce_temp.CollectionMonth < 10 then '0' else '' end + cast(ce_temp.CollectionMonth as varchar)  + '-' 
			+ case when ce_temp.CollectionDay < 10 then '0' else '' end + cast(ce_temp.CollectionDay as varchar) + 'T00:00:00.000Z', 127) as datetime)
		else null end end 
		FROM [{0}] ce_temp
		where ce_temp.CollectionDate IS NULL
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __addEventSHA(self):
		query = """
		UPDATE ce_temp
		SET [event_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
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


	def createNewEvents(self):
		# insert only one version of each event when the same event occurres multiple times in json data
		self.__setUniqueEventsTempTable()
		self.__insertNewCollectionEvents()
		
		self.__insertEventLocalisationWGS84()
		self.__insertEventLocalisationAltitude()
		self.__insertEventLocalisationDepth()
		self.__insertEventLocalisationHeight()
		#self.__insertEventLocalisationNamedArea()
		
		self.__updateEventIDsInTempTable()
		return


	def __setUniqueEventsTempTable(self):
		"""
		create a table that contains only one version of each event to be inserted
		"""
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.unique_events_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
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
			
			[LocalityDescription] VARCHAR(MAX) COLLATE {1},
			[LocalityVerbatim] VARCHAR(MAX) COLLATE {1},
			[HabitatDescription] VARCHAR(MAX) COLLATE {1},
			[ReferenceTitle] NVARCHAR(255),
			[ReferenceURI] VARCHAR(255),
			 -- [ReferenceDetails] NVARCHAR(50),
			[CollectingMethod] VARCHAR(MAX) COLLATE {1},
			[Notes] VARCHAR(MAX) COLLATE {1},
			[CountryCache] NVARCHAR(50),
			[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
			 -- 
			[event_sha] VARCHAR(64),
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
		;""".format(self.unique_events_temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [{0}] (
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
			 -- 
			[event_sha],
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
		)
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
			ce_temp.[LocalityVerbatim],
			ce_temp.[HabitatDescription], 
			ce_temp.[ReferenceTitle],
			ce_temp.[ReferenceURI],
			 -- ce_temp.[ReferenceDetails],
			ce_temp.[CollectingMethod],
			ce_temp.[Notes],
			ce_temp.[Country],
			 -- 
			ce_temp.[event_sha],
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
		SELECT DISTINCT -- the uniqueness is set by ue_temp.[RowGUID] which was set before in __setUniqueEventsTemptable when the CollectionEvent data is the same but Localisations differ
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
			ue_temp.[LocalityVerbatim],
			ue_temp.[HabitatDescription], 
			ue_temp.[ReferenceTitle],
			ue_temp.[ReferenceURI],
			 -- ue_temp.[ReferenceDetails],
			ue_temp.[CollectingMethod],
			ue_temp.[Notes],
			ue_temp.[CountryCache],
			ue_temp.[RowGUID]
		FROM [{0}] ue_temp
		;""".format(self.unique_events_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE ue_temp
		SET ue_temp.[CollectionEventID] = ce.[CollectionEventID]
		FROM [{0}] ue_temp
		INNER JOIN [CollectionEvent] ce
		ON ce.[RowGUID] = ue_temp.[RowGUID]
		;""".format(self.unique_events_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateEventIDsInTempTable(self):
		query = """
		UPDATE ce_temp
		SET ce_temp.[CollectionEventID] = ue_temp.[CollectionEventID],
		ce_temp.[RowGUID] = ue_temp.[RowGUID]
		FROM [{0}] ce_temp
		INNER JOIN [{1}] ue_temp 
		ON ce_temp.[event_sha] = ue_temp.[event_sha]
		 -- WHERE ce_temp.[CollectionEventID] IS NULL
		;""".format(self.temptable, self.unique_events_temptable)
		
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
		;""".format(self.unique_events_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertEventLocalisationDepth(self):
		query = """
		INSERT INTO CollectionEventLocalisation (
			[CollectionEventID],
			[Location1],
			[Location2],
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
		;""".format(self.unique_events_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertEventLocalisationHeight(self):
		query = """
		INSERT INTO CollectionEventLocalisation (
			[CollectionEventID],
			[Location1],
			[Location2],
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
		;""".format(self.unique_events_temptable)
		
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
		SET cs.[CollectionEventID] = ce_temp.[CollectionEventID]
		FROM CollectionSpecimen cs
		INNER JOIN [{0}] ce_temp 
		ON ce_temp.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		WHERE ce_temp.[CollectionEventID] IS NOT NULL
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateCEDicts(self):
		ce_ids = self.getIDsForCEDicts()
		for ce_dict in self.ce_dicts:
			entry_num = ce_dict['entry_num']
			ce_dict['CollectionEventID'] = ce_ids[entry_num]['CollectionEventID']
			ce_dict['RowGUID'] = ce_ids[entry_num]['RowGUID']
			ce_dict['CollectionSpecimenID'] = ce_ids[entry_num]['CollectionSpecimenID']
		return


	def getIDsForCEDicts(self):
		query = """
		SELECT ce_temp.[entry_num], ce.CollectionEventID, ce.[RowGUID], ce_temp.[CollectionSpecimenID]
		FROM [CollectionEvent] ce
		INNER JOIN [{0}] ce_temp
		ON ce_temp.[RowGUID] = ce.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		ce_ids = {}
		for row in rows:
			if not row[0] in ce_ids:
				ce_ids[row[0]] = {}
			ce_ids[row[0]]['CollectionEventID'] = row[1]
			ce_ids[row[0]]['RowGUID'] = row[2]
			ce_ids[row[0]]['CollectionSpecimenID'] = row[3]
		
		return ce_ids


	'''
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
	'''






