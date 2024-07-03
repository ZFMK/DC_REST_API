import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.JSON2TempTable import JSON2TempTable


"""
It appears to be impossible to update any IdentificationUnits as they do not have any identifying property or character. So the IdentificationUnits 
need to be updated by deleteing the existing and inserting the new ones?!
Therefore, here is no code to get and updating the existing IdentificationUnits. But this just moves the problem of outside the code here. Is that better?
"""


class SpecimenPartInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#csp_temptable'
		
		
		self.schema = [
			{'colname': 'specimenpart_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'SpecimenPartID'},
			{'colname': 'CollectionName', 'default': 'No collection', 'None allowed': False},
			{'colname': 'AccessionNumber'},
			{'colname': 'PartSublabel'},
			{'colname': 'PreparationMethod'},
			{'colname': 'MaterialCategory', 'None allowed': False},
			{'colname': 'StorageLocation'},
			{'colname': 'StorageContainer'},
			{'colname': 'Stock'},
			{'colname': 'StockUnit'},
			{'colname': 'ResponsibleName'},
			{'colname': 'ResponsibleAgentURI'},
			{'colname': 'Notes'},
			{'colname': 'DataWithholdingReason'}
		]
		
		self.json2temp = JSON2TempTable(self.dc_db, self.schema)


	def insertSpecimenPartData(self):
		
		self.__createSpecimenPartTempTable()
		
		self.json2temp.set_datadicts(self.csp_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__setCollectionIDs()
		self.__setMissingAccessionNumbers()
		self.__overwriteUnknownMaterialCategories()
		
		self.__insertSpecimenParts()
		
		self.__updateCSPTempTable()
		
		self.__updateCSPDicts()
		
		return


	def setSpecimenPartDicts(self, json_dicts = []):
		self.csp_dicts = []
		csp_count = 1
		for csp_dict in json_dicts:
			csp_dict['specimenpart_num'] = csp_count
			csp_count += 1
			self.csp_dicts.append(csp_dict)
		return


	def __createSpecimenPartTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[specimenpart_num] INT NOT NULL,
		[CollectionSpecimenID] INT NOT NULL,
		[SpecimenPartID] INT DEFAULT NULL,
		[CollectionID] INT DEFAULT NULL,
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		[CollectionName] NVARCHAR(255) COLLATE {1},
		[AccessionNumber] NVARCHAR(50) COLLATE {1},
		[PartSublabel] NVARCHAR(50) COLLATE {1},
		[PreparationMethod] NVARCHAR(255) COLLATE {1},
		[MaterialCategory] NVARCHAR(50) COLLATE {1},
		[StorageLocation] NVARCHAR(255) COLLATE {1},
		[StorageContainer] NVARCHAR(500) COLLATE {1},
		[Stock] FLOAT,
		[StockUnit] NVARCHAR(50) COLLATE {1},
		[ResponsibleName] NVARCHAR(255) COLLATE {1},
		[ResponsibleAgentURI] VARCHAR(255) COLLATE {1},
		[Notes] NVARCHAR(MAX) COLLATE {1},
		[DataWithholdingReason] NVARCHAR(255) COLLATE {1},
		PRIMARY KEY ([specimenpart_num]),
		INDEX [specimenpart_num_idx] ([specimenpart_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [SpecimenPartID_idx] (SpecimenPartID),
		INDEX [RowGUID_idx] (RowGUID)
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __setCollectionIDs(self):
		query = """
		UPDATE csp_temp
		SET csp_temp.[CollectionID] = c.CollectionID
		FROM [{0}] csp_temp
		INNER JOIN [Collection] c ON c.[CollectionName] = csp_temp.[CollectionName]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		# set the CollectionNames that can not be found to 'No collection' and the according CollectionID in Collection table
		query = """
		UPDATE csp_temp
		SET csp_temp.[CollectionID] = c.CollectionID,
			csp_temp.[CollectionName] = c.CollectionName
		FROM [{0}] csp_temp
		INNER JOIN [Collection] c ON c.[CollectionName] = 'No collection'
		WHERE csp_temp.[CollectionID] IS NULL
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		# TODO: how to deall with Collections that do not exist in database?
		'''
		query = """
		SELECT [dataset_num], [specimenpart_num], [AccessionNumber], [CollectionName]
		FROM [{0}] csp_temp
		WHERE CollectionID IS NULL
		;""".format(self.temptable)
		
		querylog.debug(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		if len(rows) > 0:
			#raise FailedDataImportError()
			self.failed_specimenparts = []
			for row in rows:
				part_dict = {
					'Message': 'Failed SpecimenPart import: CollectionName can not be found in database!',
					'CollectionName': row[3],
					'Dataset': row[0],
					'Specimenpart number': row[1],
					'Part AccessionNumber': row[2],
				}
				self.failed_specimenparts.append(part_dict)
				raise ValueError()
		'''
		
		return


	def __setMissingAccessionNumbers(self):
		query = """
		UPDATE csp_temp
		SET csp_temp.[AccessionNumber] = csp_temp2.[PartAccessionNumber]
		FROM [{0}] csp_temp
		INNER JOIN (
			SELECT 
				[specimenpart_num], 
				CONCAT_WS('_', cs.[AccessionNumber], ROW_NUMBER() OVER(PARTITION BY csp_temp.[CollectionSpecimenID] ORDER BY csp_temp.[specimenpart_num] ASC)) AS [PartAccessionNumber]
				FROM [{0}] csp_temp
				INNER JOIN [CollectionSpecimen] cs ON cs.[CollectionSpecimenID] = csp_temp.[CollectionSpecimenID]
		) AS csp_temp2
		ON csp_temp.[specimenpart_num] = csp_temp2.[specimenpart_num]
		WHERE csp_temp.[AccessionNumber] IS NULL
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __overwriteUnknownMaterialCategories(self):
		# set the MaterialCategories that can not be found in CollMaterialCategory_Enum to 'specimen'
		
		query = """
		UPDATE csp_temp
		SET csp_temp.[MaterialCategory] = 'specimen'
		FROM [{0}] csp_temp
		LEFT JOIN [CollMaterialCategory_Enum] mce ON mce.[code] = csp_temp.[MaterialCategory]
		WHERE mce.[code] IS NULL
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()


	def __insertSpecimenParts(self):
		
		# SpecimenPartID is an IDENTITY column
		
		query = """
		INSERT INTO [CollectionSpecimenPart] 
		(
			[CollectionSpecimenID],
			[CollectionID],
			[MaterialCategory],
			[RowGUID],
			[AccessionNumber],
			[PartSublabel],
			[PreparationMethod],
			[StorageLocation],
			[StorageContainer],
			[Stock],
			[StockUnit],
			[ResponsibleName],
			[ResponsibleAgentURI], 
			[Notes],
			[DataWithholdingReason]
		)
		SELECT 
			[CollectionSpecimenID],
			[CollectionID],
			[MaterialCategory],
			[RowGUID],
			[AccessionNumber],
			[PartSublabel],
			[PreparationMethod],
			[StorageLocation],
			[StorageContainer],
			[Stock],
			[StockUnit],
			[ResponsibleName],
			[ResponsibleAgentURI], 
			[Notes],
			[DataWithholdingReason]
		FROM [{0}] csp_temp
		ORDER BY csp_temp.[specimenpart_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateCSPTempTable(self):
		query = """
		UPDATE csp_temp
		SET csp_temp.SpecimenPartID = csp.SpecimenPartID
		FROM [{0}] csp_temp
		INNER JOIN [CollectionSpecimenPart] csp
		ON csp_temp.[RowGUID] = csp.[RowGUID]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateCSPDicts(self):
		
		csp_ids = self.getIDsForCSPDicts()
		for csp_dict in self.csp_dicts:
			specimenpart_num = csp_dict['specimenpart_num']
			csp_dict['AccessionNumber'] = csp_ids[specimenpart_num]['AccessionNumber']
			csp_dict['SpecimenPartID'] = csp_ids[specimenpart_num]['SpecimenPartID']
			csp_dict['CollectionID'] = csp_ids[specimenpart_num]['CollectionID']
			csp_dict['MaterialCategory'] = csp_ids[specimenpart_num]['MaterialCategory']
			csp_dict['RowGUID'] = csp_ids[specimenpart_num]['RowGUID']
		return


	def getIDsForCSPDicts(self):
		query = """
		SELECT 
			csp_temp.[specimenpart_num],
			csp.[AccessionNumber],
			csp.[SpecimenPartID],
			csp.[CollectionID],
			csp.[MaterialCategory],
			csp.[RowGUID]
		FROM [CollectionSpecimenPart] csp
		INNER JOIN [{0}] csp_temp
		ON csp_temp.[RowGUID] = csp.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		csp_ids = {}
		for row in rows:
			if not row[0] in csp_ids:
				csp_ids[row[0]] = {}
			csp_ids[row[0]]['AccessionNumber'] = row[1]
			csp_ids[row[0]]['SpecimenPartID'] = row[2]
			csp_ids[row[0]]['CollectionID'] = row[3]
			csp_ids[row[0]]['MaterialCategory'] = row[4]
			csp_ids[row[0]]['RowGUID'] = row[5]
		
		return csp_ids
