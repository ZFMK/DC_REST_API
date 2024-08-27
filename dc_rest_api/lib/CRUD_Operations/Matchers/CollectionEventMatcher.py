import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class CollectionEventMatcher():
	def __init__(self, dc_db, event_temptable):
		self.dc_db = dc_db
		self.temptable = event_temptable
		
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.prefiltered_temptable = '#prefiltered_events'


	def matchExistingEvents(self):
		
		self.__createPrefilteredTempTable()
		self.__matchIntoPrefiltered()
		self.__addEventSHAOnPrefiltered()
		
		self.__matchPrefilteredToEventTempTable()


	def __createPrefilteredTempTable(self):
		# create a temptable that contains all events that
		# match in CollectionEvent table columns
		# use this prefiltered table to check the matching of EventLocalisations
		
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.prefiltered_temptable)
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
		[LocalityDescription_sha] VARCHAR(64),
		[LocalityVerbatim_sha] VARCHAR(64),
		[HabitatDescription_sha] VARCHAR(64),
		[ReferenceTitle] NVARCHAR(255),
		[ReferenceURI] VARCHAR(255),
		 -- [ReferenceDetails] NVARCHAR(50),
		[CollectingMethod_sha] VARCHAR(64),
		 -- [Notes_sha] VARCHAR(64),
		[CountryCache] NVARCHAR(50),
		[RowGUID] UNIQUEIDENTIFIER NOT NULL,
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
		 -- 
		[DataWithholdingReason] NVARCHAR(255) COLLATE {1},
		[DataWithholdingReasonDate] NVARCHAR(50) COLLATE {1},
		 -- 
		INDEX [event_sha_idx] ([event_sha])
		)
		;""".format(self.prefiltered_temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __matchIntoPrefiltered(self):
		# first match all existing events by the columns in CollectionEvent
		
		query = """
		INSERT INTO [{0}] (
			[CollectionEventID],
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
			 -- [ReferenceDetails],
			[CollectingMethod_sha],
			[CountryCache],
			[RowGUID],
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
			[Height_RecordingMethod_m],
			 -- 
			[DataWithholdingReason],
			[DataWithholdingReasonDate]
		)
		SELECT 
			e.[CollectionEventID],
			e.[CollectorsEventNumber],
			e.[CollectionDate],
			e.[CollectionDay],
			e.[CollectionMonth],
			e.[CollectionYear],
			e.[CollectionEndDay],
			e.[CollectionEndMonth],
			e.[CollectionEndYear],
			e.[CollectionDateSupplement],
			e.[CollectionDateCategory],
			e.[CollectionTime],
			e.[CollectionTimeSpan],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', e.[LocalityDescription]), 2) AS [LocalityDescription_sha],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', e.[LocalityVerbatim]), 2) AS [LocalityVerbatim_sha],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', e.[HabitatDescription]), 2) AS [HabitatDescription_sha],
			e.[ReferenceTitle],
			e.[ReferenceURI],
			 -- e.[ReferenceDetails],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', e.[CollectingMethod]), 2) AS [CollectingMethod_sha],
			e.[CountryCache],
			e.[RowGUID],
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
			h.RecordingMethod AS [Height_RecordingMethod_m],
			 -- 
			e.[DataWithholdingReason],
			e.[DataWithholdingReasonDate]
			 -- 
		FROM [CollectionEvent] e
		INNER JOIN [{1}] ce_temp
		ON 
			(
				(e.[CollectionDate] = ce_temp.[CollectionDate] OR (e.[CollectionDate] IS NULL AND ce_temp.[CollectionDate] IS NULL))
				OR
				(
					(e.[CollectionDay] = ce_temp.[CollectionDay] OR (e.[CollectionDay] IS NULL AND ce_temp.[CollectionDay] IS NULL))
					AND (e.[CollectionMonth] = ce_temp.[CollectionMonth] OR (e.[CollectionMonth] IS NULL AND ce_temp.[CollectionMonth] IS NULL))
					AND (e.[CollectionYear] = ce_temp.[CollectionYear] OR (e.[CollectionYear] IS NULL AND ce_temp.[CollectionYear] IS NULL))
				)
			)
			AND ((e.[CollectionEndDay] = ce_temp.[CollectionEndDay]) OR (e.[CollectionEndDay] IS NULL AND ce_temp.[CollectionEndDay] IS NULL))
			AND ((e.[CollectionEndMonth] = ce_temp.[CollectionEndMonth]) OR (e.[CollectionEndMonth] IS NULL AND ce_temp.[CollectionEndMonth] IS NULL))
			AND ((e.[CollectionEndYear] = ce_temp.[CollectionEndYear]) OR (e.[CollectionEndYear] IS NULL AND ce_temp.[CollectionEndYear] IS NULL))
			AND ((e.[CollectionDateSupplement] = ce_temp.[CollectionDateSupplement]) OR (e.[CollectionDateSupplement] IS NULL AND ce_temp.[CollectionDateSupplement] IS NULL))
			AND ((e.[CollectionDateCategory] = ce_temp.[CollectionDateCategory]) OR (e.[CollectionDateCategory] IS NULL AND ce_temp.[CollectionDateCategory] IS NULL))
			AND ((e.[CollectionTime] = ce_temp.[CollectionTime]) OR (e.[CollectionTime] IS NULL AND ce_temp.[CollectionTime] IS NULL))
			AND ((e.[CollectionTimeSpan] = ce_temp.[CollectionTimeSpan]) OR (e.[CollectionTimeSpan] IS NULL AND ce_temp.[CollectionTimeSpan] IS NULL))
			AND ((e.[CountryCache] = ce_temp.[Country]) OR (e.[CountryCache] IS NULL AND ce_temp.[Country] IS NULL))
			AND ((e.[DataWithholdingReason] = ce_temp.[DataWithholdingReason]) OR (e.[DataWithholdingReason] IS NULL AND ce_temp.[DataWithholdingReason] IS NULL))
			AND ((e.[DataWithholdingReasonDate] = ce_temp.[DataWithholdingReasonDate]) OR (e.[DataWithholdingReasonDate] IS NULL AND ce_temp.[DataWithholdingReasonDate] IS NULL))
		LEFT JOIN [CollectionEventLocalisation] alt
		ON alt.CollectionEventID = e.CollectionEventID AND alt.LocalisationSystemID = 4
		LEFT JOIN [CollectionEventLocalisation] wgs
		ON wgs.CollectionEventID = e.CollectionEventID AND wgs.LocalisationSystemID = 8
		LEFT JOIN [CollectionEventLocalisation] d
		ON d.CollectionEventID = e.CollectionEventID AND d.LocalisationSystemID = 14
		LEFT JOIN [CollectionEventLocalisation] h
		ON h.CollectionEventID = e.CollectionEventID AND h.LocalisationSystemID = 15
		;""".format(self.prefiltered_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __addEventSHAOnPrefiltered(self):
		query = """
		UPDATE pf
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
			[CountryCache],
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
			[Height_RecordingMethod_m],
			 -- 
			[DataWithholdingReason],
			[DataWithholdingReasonDate]
		)), 2)
		FROM [{0}] pf
		;""".format(self.prefiltered_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __matchPrefilteredToEventTempTable(self):
		query = """
		UPDATE ce_temp
		SET ce_temp.[CollectionEventID] = pf.[CollectionEventID],
		ce_temp.[RowGUID] = pf.[RowGUID]
		FROM [{0}] ce_temp
		INNER JOIN [{1}] pf
		ON pf.[event_sha] = ce_temp.[event_sha]
		;""".format(self.temptable, self.prefiltered_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return



	'''
	def __matchPrefilteredToEventTempTable(self):
		query = """
		UPDATE ce_temp
		SET ce_temp.[CollectionEventID] = pf.[CollectionEventID],
		ce_temp.[RowGUID] = pf.[RowGUID]
		FROM [{0}] ce_temp
		INNER JOIN [{1}] pf
		ON 
			(
				(pf.[CollectionDate] = ce_temp.[CollectionDate] OR (pf.[CollectionDate] IS NULL AND ce_temp.[CollectionDate] IS NULL))
				OR
				(
					(pf.[CollectionDay] = ce_temp.[CollectionDay] OR (pf.[CollectionDay] IS NULL AND ce_temp.[CollectionDay] IS NULL))
					AND (pf.[CollectionMonth] = ce_temp.[CollectionMonth] OR (pf.[CollectionMonth] IS NULL AND ce_temp.[CollectionMonth] IS NULL))
					AND (pf.[CollectionYear] = ce_temp.[CollectionYear] OR (pf.[CollectionYear] IS NULL AND ce_temp.[CollectionYear] IS NULL))
				)
			)
			AND (pf.[CollectionEndDay] = ce_temp.[CollectionEndDay] OR (pf.[CollectionEndDay] IS NULL AND ce_temp.[CollectionEndDay] IS NULL))
			AND (pf.[CollectionEndMonth] = ce_temp.[CollectionEndMonth] OR (pf.[CollectionEndMonth] IS NULL AND ce_temp.[CollectionEndMonth] IS NULL))
			AND (pf.[CollectionEndYear] = ce_temp.[CollectionEndYear] OR (pf.[CollectionEndYear] IS NULL AND ce_temp.[CollectionEndYear] IS NULL))
			AND (pf.[CollectionDateSupplement] = ce_temp.[CollectionDateSupplement] OR (pf.[CollectionDateSupplement] IS NULL AND ce_temp.[CollectionDateSupplement] IS NULL))
			AND (pf.[CollectionDateCategory] = ce_temp.[CollectionDateCategory] OR (pf.[CollectionDateCategory] IS NULL AND ce_temp.[CollectionDateCategory] IS NULL))
			AND (pf.[CollectionTime] = ce_temp.[CollectionTime] OR (pf.[CollectionTime] IS NULL AND ce_temp.[CollectionTime] IS NULL))
			AND (pf.[CollectionTimeSpan] = ce_temp.[CollectionTimeSpan] OR (pf.[CollectionTimeSpan] IS NULL AND ce_temp.[CollectionTimeSpan] IS NULL))
			AND (pf.[CountryCache] = ce_temp.[Country] OR (pf.[CountryCache] IS NULL AND ce_temp.[Country] IS NULL))
			 -- 
			AND (pf.[LocalityDescription_sha] = ce_temp.[LocalityDescription_sha] OR (pf.[LocalityDescription_sha] IS NULL AND ce_temp.[LocalityDescription_sha] IS NULL))
			AND (pf.[LocalityVerbatim_sha] = ce_temp.[LocalityVerbatim_sha] OR (pf.[LocalityVerbatim_sha] IS NULL AND ce_temp.[LocalityVerbatim_sha] IS NULL))
			AND (pf.[HabitatDescription_sha] = ce_temp.[HabitatDescription_sha] OR (pf.[HabitatDescription_sha] IS NULL AND ce_temp.[HabitatDescription_sha] IS NULL))
			AND (pf.[CollectingMethod_sha] = ce_temp.[CollectingMethod_sha] OR (pf.[CollectingMethod_sha] IS NULL AND ce_temp.[CollectingMethod_sha] IS NULL))
			 -- 
			AND (pf.[Altitude] = ce_temp.[Altitude] OR (pf.[Altitude] IS NULL AND ce_temp.[Altitude] IS NULL))
			AND (pf.[Altitude_Accuracy] = ce_temp.[Altitude_Accuracy] OR (pf.[Altitude_Accuracy] IS NULL AND ce_temp.[Altitude_Accuracy] IS NULL))
			 -- 
			AND (pf.[WGS84_Lat] = ce_temp.[WGS84_Lat] OR (pf.[WGS84_Lat] IS NULL AND ce_temp.[WGS84_Lat] IS NULL))
			AND (pf.[WGS84_Lon] = ce_temp.[WGS84_Lon] OR (pf.[WGS84_Lon] IS NULL AND ce_temp.[WGS84_Lon] IS NULL))
			AND (pf.[WGS84_Accuracy] = ce_temp.[WGS84_Accuracy] OR (pf.[WGS84_Accuracy] IS NULL AND ce_temp.[WGS84_Accuracy] IS NULL))
			AND (pf.[WGS84_RecordingMethod] = ce_temp.[WGS84_RecordingMethod] OR (pf.[WGS84_RecordingMethod] IS NULL AND ce_temp.[WGS84_RecordingMethod] IS NULL))
			 -- 
			AND (pf.[Depth_min_m] = ce_temp.[Depth_min_m] OR (pf.[Depth_min_m] IS NULL AND ce_temp.[Depth_min_m] IS NULL))
			AND (pf.[Depth_max_m] = ce_temp.[Depth_max_m] OR (pf.[Depth_max_m] IS NULL AND ce_temp.[Depth_max_m] IS NULL))
			AND (pf.[Depth_Accuracy_m] = ce_temp.[Depth_Accuracy_m] OR (pf.[Depth_Accuracy_m] IS NULL AND ce_temp.[Depth_Accuracy_m] IS NULL))
			AND (pf.[Depth_RecordingMethod_m] = ce_temp.[Depth_RecordingMethod_m] OR (pf.[Depth_RecordingMethod_m] IS NULL AND ce_temp.[Depth_RecordingMethod_m] IS NULL))
			 -- 
			AND (pf.[Height_min_m] = ce_temp.[Height_min_m] OR (pf.[Height_min_m] IS NULL AND ce_temp.[Height_min_m] IS NULL))
			AND (pf.[Height_max_m] = ce_temp.[Height_max_m] OR (pf.[Height_max_m] IS NULL AND ce_temp.[Height_max_m] IS NULL))
			AND (pf.[Height_Accuracy_m] = ce_temp.[Height_Accuracy_m] OR (pf.[Height_Accuracy_m] IS NULL AND ce_temp.[Height_Accuracy_m] IS NULL))
			AND (pf.[Height_RecordingMethod_m] = ce_temp.[Height_RecordingMethod_m] OR (pf.[Height_RecordingMethod_m] IS NULL AND ce_temp.[Height_RecordingMethod_m] IS NULL))
		;""".format(self.temptable, self.prefiltered_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
		'''



