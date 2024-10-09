import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('./dc_importer/config.ini')

import logging, logging.config
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable


class ExternalDatasourceInserter():
	def __init__(self, dc_db, users_roles = []):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.users_roles = users_roles
		self.messages = []
		
		self.temptable = '#datasource_temptable'
		self.unique_ed_temptable = '#unique_ed_temptable'
		
		self.schema = [
			{'colname': '@id', 'None allowed': False},
			#{'colname': 'CollectionSpecimenID'},
			# do not add ExternalDatasourceID as it should be set by comparison
			#{'colname': 'ExternalDatasourceID'},
			{'colname': 'DatabaseURN'},
			{'colname': 'ExternalDatasourceName', 'None allowed': False},
			{'colname': 'ExternalDatasourceVersion'},
			{'colname': 'ExternalDatasourceURI'},
			{'colname': 'ExternalDatasourceInstitution'},
			{'colname': 'InternalNotes'},
			{'colname': 'ExternalAttribute_NameID'}
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)
		self.messages = []


	def insertExternalDatasourceData(self, json_dicts = []):
		self.ed_dicts = json_dicts
		
		self.__createTempTable()
		
		self.json2temp.set_datadicts(self.ed_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__setExistingDatasources()
		num_datasources = self.__getNumberOfUnmatchedDatasources()
		if num_datasources > 0:
			if 'Administrator' in self.users_roles or 'DataManager' in self.users_roles:
				self.__setUniqueNewDatasourcesTempTable()
				self.__insertNewDatasources()
			else:
				self.messages.append('You do not have the rights to insert external datasources')
				raise ValueError()
		
		#self.__setExternalDatasourceIDsInCollectionSpecimen()
		
		#if 'Administrator' in self.users_roles or 'DataManager' in self.users_roles:
		#	self.__deleteUnconnectedExternalDatasources()
		
		self.__updateEDDicts()
		
		return


	def __createTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[@id] VARCHAR(100) COLLATE {1} NOT NULL,
		[CollectionSpecimenID] INT DEFAULT NULL,
		[ExternalDatasourceID] INT DEFAULT NULL,
		[RowGUID] UNIQUEIDENTIFIER,
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		[ExternalDatasourceName] VARCHAR(255) COLLATE {1} NOT NULL,
		[ExternalDatasourceVersion] VARCHAR(255) COLLATE {1},
		[ExternalDatasourceURI] VARCHAR(300) COLLATE {1},
		[ExternalDatasourceInstitution] VARCHAR(300) COLLATE {1},
		[InternalNotes] VARCHAR(1500) COLLATE {1},
		[ExternalAttribute_NameID] VARCHAR(255) COLLATE {1},
		PRIMARY KEY ([@id]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [ExternalDatasourceID_idx] ([ExternalDatasourceID]),
		INDEX [ExternalDatasourceName_idx] ([ExternalDatasourceName]),
		INDEX [ExternalDatasourceVersion_idx] ([ExternalDatasourceVersion]),
		INDEX [ExternalDatasourceURI_idx] ([ExternalDatasourceURI]),
		INDEX [ExternalAttribute_NameID_idx] ([ExternalAttribute_NameID]),
		INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __setExistingDatasources(self):
		query = """
		UPDATE ed_temp
		SET ed_temp.[ExternalDatasourceID] = eds.[ExternalDatasourceID],
		ed_temp.[RowGUID] = eds.[RowGUID]
		FROM [{0}] ed_temp
		INNER JOIN CollectionExternalDatasource eds
		ON (
				eds.[ExternalDatasourceName] = ed_temp.[ExternalDatasourceName] 
				AND ((eds.[ExternalDatasourceVersion] = ed_temp.[ExternalDatasourceVersion]) OR (eds.[ExternalDatasourceVersion] IS NULL AND ed_temp.[ExternalDatasourceVersion] IS NULL))
				AND ((eds.[ExternalDatasourceURI] = ed_temp.[ExternalDatasourceURI]) OR (eds.[ExternalDatasourceURI] IS NULL AND ed_temp.[ExternalDatasourceURI] IS NULL))
				AND ((eds.[ExternalDatasourceInstitution] = ed_temp.[ExternalDatasourceInstitution]) OR (eds.[ExternalDatasourceInstitution] IS NULL AND ed_temp.[ExternalDatasourceInstitution] IS NULL))
				AND ((eds.[ExternalAttribute_NameID] = ed_temp.[ExternalAttribute_NameID]) OR (eds.[ExternalAttribute_NameID] IS NULL AND ed_temp.[ExternalAttribute_NameID] IS NULL))
			)
		WHERE ed_temp.[ExternalDatasourceID] IS NULL
		;
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __setUniqueNewDatasourcesTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.unique_ed_temptable)
		
		query = """
		CREATE TABLE [{0}] (
			[ExternalDatasourceID] INT DEFAULT NULL,
			[ExternalDatasourceName] VARCHAR(255) COLLATE {1} NOT NULL,
			[ExternalDatasourceVersion] VARCHAR(255) COLLATE {1},
			[ExternalDatasourceURI] VARCHAR(300) COLLATE {1},
			[ExternalDatasourceInstitution] VARCHAR(300) COLLATE {1},
			[InternalNotes] VARCHAR(1500) COLLATE {1},
			[ExternalAttribute_NameID] VARCHAR(255) COLLATE {1},
			[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
			INDEX [ExternalDatasourceID_idx] ([ExternalDatasourceID]),
			INDEX [ExternalDatasourceName_idx] ([ExternalDatasourceName]),
			INDEX [ExternalDatasourceVersion_idx] ([ExternalDatasourceVersion]),
			INDEX [ExternalDatasourceURI_idx] ([ExternalDatasourceURI]),
			INDEX [ExternalAttribute_NameID_idx] ([ExternalAttribute_NameID])
		)
		;""".format(self.unique_ed_temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [{0}] (
			[ExternalDatasourceID],
			[ExternalDatasourceName],
			[ExternalDatasourceVersion],
			[ExternalDatasourceURI],
			[ExternalDatasourceInstitution],
			[InternalNotes],
			[ExternalAttribute_NameID]
		)
		SELECT DISTINCT 
			[ExternalDatasourceID],
			[ExternalDatasourceName],
			[ExternalDatasourceVersion],
			[ExternalDatasourceURI],
			[ExternalDatasourceInstitution],
			[InternalNotes],
			[ExternalAttribute_NameID]
		FROM [{1}]
		WHERE [ExternalDatasourceID] IS NULL
		;""".format(self.unique_ed_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __getNumberOfUnmatchedDatasources(self):
		query = """
		SELECT COUNT([@id])
		FROM [{0}] ed_temp
		WHERE ed_temp.[ExternalDatasourceID] IS NULL
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row is not None:
			num = int(row[0])
			return num
		else:
			return 0


	def __insertNewDatasources(self):
		
		query = """
		INSERT INTO [CollectionExternalDatasource] 
		(
		[ExternalDatasourceName],
		[ExternalDatasourceVersion],
		[ExternalDatasourceURI],
		[ExternalDatasourceInstitution],
		[InternalNotes],
		[ExternalAttribute_NameID],
		[RowGUID]
		)
		SELECT 
		ue_temp.[ExternalDatasourceName],
		ue_temp.[ExternalDatasourceVersion],
		ue_temp.[ExternalDatasourceURI],
		ue_temp.[ExternalDatasourceInstitution],
		ue_temp.[InternalNotes],
		ue_temp.[ExternalAttribute_NameID],
		ue_temp.[RowGUID]
		FROM [{0}] ue_temp
		;""".format(self.unique_ed_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE ue_temp
		SET ue_temp.[ExternalDatasourceID] = ed.[ExternalDatasourceID]
		FROM [{0}] ue_temp
		INNER JOIN [CollectionExternalDatasource] ed
		ON ed.[RowGUID] = ue_temp.[RowGUID]
		;""".format(self.unique_ed_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		# update the ExternalDatasourceIDs in temptable
		self.__setExistingDatasources()
		
		return


	'''
	def __setExternalDatasourceIDsInCollectionSpecimen(self):
		query = """
		UPDATE cs
		SET cs.[ExternalDatasourceID] = ed_temp.[ExternalDatasourceID]
		FROM CollectionSpecimen cs
		INNER JOIN [{0}] ed_temp
		ON ed_temp.CollectionSpecimenID = cs.CollectionSpecimenID
		WHERE ed_temp.[ExternalDatasourceID] IS NOT NULL AND ed_temp.CollectionSpecimenID IS NOT NULL
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return
	'''


	def __deleteUnconnectedExternalDatasources(self):
		query = """
		DELETE eds
		FROM [CollectionExternalDatasource] eds
		LEFT JOIN [CollectionSpecimen] cs ON cs.[ExternalDatasourceID] = eds.[ExternalDatasourceID]
		WHERE cs.[CollectionSpecimenID] IS NULL
		;"""
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateEDDicts(self):
		ed_ids = self.getIDsForEDDicts()
		for dict_id in self.ed_dicts:
			ed_dict = self.ed_dicts[dict_id]
			ed_dict['ExternalDatasourceID'] = ed_ids[dict_id]['ExternalDatasourceID']
			ed_dict['RowGUID'] = ed_ids[dict_id]['RowGUID']
			ed_dict['CollectionSpecimenID'] = ed_ids[dict_id]['CollectionSpecimenID']
		return


	def getIDsForEDDicts(self):
		query = """
		SELECT ed_temp.[@id], ed.[ExternalDatasourceID], ed.[RowGUID], ed_temp.[CollectionSpecimenID]
		FROM [CollectionExternalDatasource] ed
		INNER JOIN [{0}] ed_temp
		ON ed_temp.[RowGUID] = ed.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		ed_ids = {}
		for row in rows:
			if not row[0] in ed_ids:
				ed_ids[row[0]] = {}
			ed_ids[row[0]]['ExternalDatasourceID'] = row[1]
			ed_ids[row[0]]['RowGUID'] = row[2]
			ed_ids[row[0]]['CollectionSpecimenID'] = row[3]
		
		return ed_ids
