import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('./dc_importer/config.ini')

import logging, logging.config
querylog = logging.getLogger('query')
api_log = logging.getLogger('dc_api')


from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Matchers.CollectionSpecimenRelationMatcher import CollectionSpecimenRelationMatcher

class CollectionSpecimenRelationInserter():
	def __init__(self, dc_db, users_roles = []):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.users_roles = users_roles
		self.messages = []
		
		self.temptable = '#specimenrelation_temptable'
		self.unique_ed_temptable = '#unique_specimenrelation_temptable'
		
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


	def insertSpecimenRelationData(self, json_dicts = []):
		self.csr_dicts = json_dicts
		
		self.__createTempTable()
		
		self.json2temp.set_datadicts(self.csr_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__insertSpecimenRelations()
		self.__updateCSRTempTable()
		
		self.__updateCSRDicts()
		
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
		UPDATE csr_temp
		SET csr_temp.[RelationType] = NULL
		FROM [{0}] csr_temp
		LEFT JOIN [CollSpecimenRelation_Enum] csr_e
		ON csr_e.[RelationType] = csr_temp.[RelationType]
		WHERE csr_temp.[RelationType] IS NOT NULL AND csr_e.[RelationType] IS NULL
		;""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		invalid_relation_types = self.cur.rowcount
		if invalid_relation_types > 0:
			self.messages.append('Some CollectionSpecimenRelations contain invalid values for "RelationType". The RelationType of these entries have been set to NULL')
			api_log.warn('Some CollectionSpecimenRelations contain invalid values for "RelationType". The RelationType of these entries have been set to NULL')
		return


	def __validateCollectionID(self):
		query = """
		UPDATE csr_temp
		SET csr_temp.[CollectionID] = NULL
		FROM [{0}] csr_temp
		LEFT JOIN [Collection] c
		ON c.CollectionID = csr_temp.CollectionID
		LEFT JOIN (
			SELECT c.CollectionID, csp.SpecimenPartID, iuip.IdentificationUnitID, iuip.CollectionSpecimenID
			FROM IdentificationUnitInPart iuip
			INNER JOIN CollectionSpecimenPart csp
			ON csp.CollectionSpecimenID = iuip.CollectinSpecimenID
			AND csp.SpecimenPartID = iuip.SpecimenPartID
			LEFT JOIN [Collection] c
			ON c.CollectionID = csp.CollectionID
		) AS c_in_part
		ON c_in_part.CollectionSpecimenID = csr_temp.CollectionSpecimenID
		AND c_in_part.IdentificationUnitID = csr_temp.IdentificationUnitID
		AND (c_in_part.SpecimenPartID = csr_temp.SpecimenPartID OR (csr_temp.SpecimenPartID IS NULL))
		WHERE csr.temp.CollectionID IS NOT NULL AND c.CollectionID IS NULL AND c_in_part.CollectionID IS NULL
		;""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		invalid_relation_types = self.cur.rowcount
		if invalid_relation_types > 0:
			self.messages.append('Some CollectionSpecimenRelations contain invalid values for "CollectionID". The CollectionID of these entries have been set to NULL')
			api_log.warn('Some CollectionSpecimenRelations contain invalid values for "CollectionID". The CollectionID of these entries have been set to NULL')
		return


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
		csr_temp.[CollectionSpecimenID],
		csr_temp.[RelatedSpecimenURI],
		csr_temp.[RelatedSpecimenDisplayText],
		csr_temp.[RelationType],
		csr_temp.[RelatedSpecimenCollectionID],
		csr_temp.[RelatedSpecimenDescription],
		csr_temp.[IdentificationUnitID],
		csr_temp.[SpecimenPartID],
		csr_temp.[Notes],
		csr_temp.[IsInternalRelationCache],
		csr_temp.[RowGUID]
		FROM [{0}] csr_temp
		ORDER BY csr_temp.[entry_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateCSRDicts(self):
		
		csr_ids = self.getIDsForCSRDicts()
		for csr_dict in self.csr_dicts:
			entry_num = csr_dict['entry_num']
			csr_dict['RowGUID'] = csr_ids[entry_num]['RowGUID']
		return


	def getIDsForIDicts(self):
		query = """
		SELECT csr_temp.[entry_num],
		csr_temp.[RowGUID]
		FROM [{0}] csr_temp
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		csr_ids = {}
		for row in rows:
			if not row[0] in csr_ids:
				csr_ids[row[0]] = {}
			csr_ids[row[0]]['RowGUID'] = row[1]
		
		return i_ids

