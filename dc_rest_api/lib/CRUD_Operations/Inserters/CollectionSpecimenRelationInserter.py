import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('./dc_importer/config.ini')

import logging, logging.config
querylog = logging.getLogger('query')
api_log = logging.getLogger('dc_api')


from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable


class CollectionSpecimenRelationInserter():
	def __init__(self, dc_db, users_roles = []):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.users_roles = users_roles
		self.messages = []
		
		self.temptable = '#specimenrelation_temptable'
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'RelatedSpecimenURI', 'None allowed': False},
			{'colname': 'RelatedSpecimenDisplayText', 'None allowed': False},
			{'colname': 'RelationType'},
			{'colname': 'RelatedSpecimenCollectionID'},
			{'colname': 'RelatedSpecimenDescription'},
			{'colname': 'IdentificationUnitID'},
			{'colname': 'SpecimenPartID'},
			{'colname': 'Notes'},
			{'colname': 'IsInternalRelationCache', 'None allowed': False, 'default': 1}
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)
		self.messages = []


	def setSpecimenRelationDicts(self, json_dicts = []):
		self.csrel_dicts = []
		csrel_count = 1
		for csrel_dict in json_dicts:
			csrel_dict['entry_num'] = csrel_count
			csrel_count += 1
			self.csrel_dicts.append(csrel_dict)
		return


	def insertSpecimenRelationData(self):
		self.__createTempTable()
		self.json2temp.set_datadicts(self.csrel_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__insertSpecimenRelations()
		
		self.__updateCSRelDicts()
		
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
		[entry_num] INT NOT NULL,
		[CollectionSpecimenID] INT NOT NULL,
		[RelatedSpecimenURI] VARCHAR(255) COLLATE {1} NOT NULL,
		[RelatedSpecimenDisplayText]  VARCHAR(255) COLLATE {1} NOT NULL,
		[RelationType] NVARCHAR(50) COLLATE {1},
		[RelatedSpecimenCollectionID] INT,
		[RelatedSpecimenDescription] NVARCHAR(MAX) COLLATE {1},
		[IdentificationUnitID] INT,
		[SpecimenPartID] INT,
		[Notes] NVARCHAR(MAX) COLLATE {1},
		[IsInternalRelationCache] BIT NOT NULL,
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		PRIMARY KEY ([entry_num]),
		INDEX [RowGUID_idx] ([RowGUID]),
		INDEX [CollectionSpecimenID_idx] (CollectionSpecimenID),
		INDEX [RelatedSpecimenCollectionID_idx] (RelatedSpecimenCollectionID),
		INDEX [RelationType_idx] (RelationType),
		INDEX [IdentificationUnitID_idx] (IdentificationUnitID),
		INDEX [SpecimenPartID_idx] (SpecimenPartID)
		)
		;""".format(self.temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __replaceInvalidRelationType(self):
		invalid_relation_types = 0
		query = """
		UPDATE csrel_temp
		SET csrel_temp.[RelationType] = NULL
		FROM [{0}] csrel_temp
		LEFT JOIN [CollSpecimenRelation_Enum] csrel_e
		ON csrel_e.[RelationType] = csrel_temp.[RelationType]
		WHERE csrel_temp.[RelationType] IS NOT NULL AND csrel_e.[RelationType] IS NULL
		;""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		invalid_relation_types = self.cur.rowcount
		if invalid_relation_types > 0:
			self.messages.append('Some CollectionSpecimenRelations contain invalid values for "RelationType". The RelationType of these entries have been set to NULL')
			api_log.warn('Some CollectionSpecimenRelations contain invalid values for "RelationType". The RelationType of these entries have been set to NULL')
		return

	'''
	def __validateCollectionID(self):
		query = """
		UPDATE csrel_temp
		SET csrel_temp.[CollectionID] = NULL
		FROM [{0}] csrel_temp
		LEFT JOIN [Collection] c
		ON c.CollectionID = csrel_temp.CollectionID
		LEFT JOIN (
			SELECT c.CollectionID, csp.SpecimenPartID, iuip.IdentificationUnitID, iuip.CollectionSpecimenID
			FROM IdentificationUnitInPart iuip
			INNER JOIN CollectionSpecimenPart csp
			ON csp.CollectionSpecimenID = iuip.CollectinSpecimenID
			AND csp.SpecimenPartID = iuip.SpecimenPartID
			LEFT JOIN [Collection] c
			ON c.CollectionID = csp.CollectionID
		) AS c_in_part
		ON c_in_part.CollectionSpecimenID = csrel_temp.CollectionSpecimenID
		AND c_in_part.IdentificationUnitID = csrel_temp.IdentificationUnitID
		AND (c_in_part.SpecimenPartID = csrel_temp.SpecimenPartID OR (csrel_temp.SpecimenPartID IS NULL))
		WHERE csrel_temp.CollectionID IS NOT NULL AND c.CollectionID IS NULL AND c_in_part.CollectionID IS NULL
		;""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		invalid_relation_types = self.cur.rowcount
		if invalid_relation_types > 0:
			self.messages.append('Some CollectionSpecimenRelations contain invalid values for "CollectionID". The CollectionID of these entries have been set to NULL')
			api_log.warn('Some CollectionSpecimenRelations contain invalid values for "CollectionID". The CollectionID of these entries have been set to NULL')
		return
	'''


	def __insertSpecimenRelations(self):
		
		query = """
		INSERT INTO [CollectionSpecimenRelation] (
			[CollectionSpecimenID],
			[RelatedSpecimenURI],
			[RelatedSpecimenDisplayText],
			[RelationType],
			[RelatedSpecimenCollectionID],
			[RelatedSpecimenDescription],
			[IdentificationUnitID],
			[SpecimenPartID],
			[Notes],
			[IsInternalRelationCache],
			[RowGUID]
		)
		SELECT 
		csrel_temp.[CollectionSpecimenID],
		csrel_temp.[RelatedSpecimenURI],
		csrel_temp.[RelatedSpecimenDisplayText],
		csrel_temp.[RelationType],
		csrel_temp.[RelatedSpecimenCollectionID],
		csrel_temp.[RelatedSpecimenDescription],
		csrel_temp.[IdentificationUnitID],
		csrel_temp.[SpecimenPartID],
		csrel_temp.[Notes],
		csrel_temp.[IsInternalRelationCache],
		csrel_temp.[RowGUID]
		FROM [{0}] csrel_temp
		ORDER BY csrel_temp.[entry_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateCSRelDicts(self):
		
		csrel_ids = self.getIDsForCSRelDicts()
		for csrel_dict in self.csrel_dicts:
			entry_num = csrel_dict['entry_num']
			csrel_dict['RowGUID'] = csrel_ids[entry_num]['RowGUID']
		return


	def getIDsForCSRelDicts(self):
		query = """
		SELECT csrel_temp.[entry_num],
		csrel_temp.[RowGUID]
		FROM [{0}] csrel_temp
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		csrel_ids = {}
		for row in rows:
			if not row[0] in csrel_ids:
				csrel_ids[row[0]] = {}
			csrel_ids[row[0]]['RowGUID'] = row[1]
		
		return csrel_ids

