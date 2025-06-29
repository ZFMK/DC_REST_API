import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable

from dc_rest_api.lib.CRUD_Operations.Inserters.IdentificationInserter import IdentificationInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.SpecimenPartInserter import SpecimenPartInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.IdentificationUnitInPartSetter import IdentificationUnitInPartSetter
from dc_rest_api.lib.CRUD_Operations.Inserters.IdentificationUnitAnalysisInserter import IdentificationUnitAnalysisInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.CollectionSpecimenRelationInserter import CollectionSpecimenRelationInserter


class IdentificationUnitInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#iu_temptable'
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'IdentificationUnitID'},
			{'colname': 'DatabaseURN'},
			{'colname': 'LastIdentificationCache', 'default': 'unknown', 'None allowed': False},
			{'colname': 'TaxonomicGroup', 'default': 'organism', 'None allowed': False},
			{'colname': 'OrderCache'},
			{'colname': 'FamilyCache'},
			{'colname': 'HierarchyCache'},
			{'colname': 'LifeStage'},
			{'colname': 'Gender'},
			{'colname': 'NumberOfUnits'},
			{'colname': 'NumberOfUnitsModifier'},
			{'colname': 'UnitIdentifier'},
			{'colname': 'UnitDescription'},
			{'colname': 'DisplayOrder'},
			{'colname': 'Notes'},
			{'colname': 'DataWithholdingReason'},
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)


	def insertIdentificationUnitData(self):
		self.__createIdentificationUnitTempTable()
		
		self.json2temp.set_datadicts(self.iu_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__insertIdentificationUnits()
		self.__updateIUTempTable()
		
		self.__updateIUDicts()
		
		identifications = []
		iuanalyses = []
		specimenparts = []
		specimenrelations = []
		
		for iu_dict in self.iu_dicts:
			if 'Identifications' in iu_dict:
				for i_dict in iu_dict['Identifications']:
					i_dict['CollectionSpecimenID'] = iu_dict['CollectionSpecimenID']
					i_dict['IdentificationUnitID'] = iu_dict['IdentificationUnitID']
					identifications.append(i_dict)
			
			if 'IdentificationUnitAnalyses' in iu_dict:
				for iua_dict in iu_dict['IdentificationUnitAnalyses']:
					iua_dict['CollectionSpecimenID'] = iu_dict['CollectionSpecimenID']
					iua_dict['IdentificationUnitID'] = iu_dict['IdentificationUnitID']
					iuanalyses.append(iua_dict)
			
			if 'CollectionSpecimenParts' in iu_dict:
				for csp_dict in iu_dict['CollectionSpecimenParts']:
					csp_dict['CollectionSpecimenID'] = iu_dict['CollectionSpecimenID']
					csp_dict['IdentificationUnitID'] = iu_dict['IdentificationUnitID']
					specimenparts.append(csp_dict)
			
			if 'CollectionSpecimenRelations' in iu_dict:
				for csrel_dict in iu_dict['CollectionSpecimenRelations']:
					csrel_dict['CollectionSpecimenID'] = iu_dict['CollectionSpecimenID']
					csrel_dict['IdentificationUnitID'] = iu_dict['IdentificationUnitID']
					specimenrelations.append(csrel_dict)
		
		if len(identifications) > 0:
			i_inserter = IdentificationInserter(self.dc_db)
			i_inserter.setIdentificationDicts(identifications)
			i_inserter.insertIdentificationData()
		
		if len(iuanalyses) > 0:
			iua_inserter = IdentificationUnitAnalysisInserter(self.dc_db)
			iua_inserter.setIdentificationUnitAnalysisDicts(iuanalyses)
			iua_inserter.insertIdentificationUnitAnalysisData()
		
		if len(specimenparts) > 0:
			csp_inserter = SpecimenPartInserter(self.dc_db)
			csp_inserter.setSpecimenPartDicts(specimenparts)
			csp_inserter.insertSpecimenPartData()
		
		if len(specimenrelations) > 0:
			csrel_inserter = CollectionSpecimenRelationInserter(self.dc_db)
			csrel_inserter.setSpecimenRelationDicts(specimenrelations)
			csrel_inserter.insertSpecimenRelationData()
		
		self.setIdentificationUnitInPart()
		
		return


	def setIdentificationUnitDicts(self, json_dicts = []):
		self.iu_dicts = []
		iu_count = 1
		for iu_dict in json_dicts:
			iu_dict['entry_num'] = iu_count
			iu_count += 1
			self.iu_dicts.append(iu_dict)
		return


	def __createIdentificationUnitTempTable(self):
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
		[IdentificationUnitID] INT DEFAULT NULL,
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		[LastIdentificationCache] VARCHAR(255) DEFAULT 'unknown' COLLATE {1} NOT NULL,
		[TaxonomicGroup] VARCHAR(50) DEFAULT 'unknown' COLLATE {1} NOT NULL,
		[DisplayOrder] SMALLINT,
		[OrderCache] VARCHAR(255) COLLATE {1},
		[FamilyCache] VARCHAR(255) COLLATE {1},
		[HierarchyCache] VARCHAR(500) COLLATE {1},
		[LifeStage] VARCHAR(255) COLLATE {1},
		[Gender] VARCHAR(50) COLLATE {1},
		[NumberOfUnits] SMALLINT,
		[NumberOfUnitsModifier] VARCHAR(50) COLLATE {1},
		[UnitIdentifier] VARCHAR(50) COLLATE {1},
		[UnitDescription] VARCHAR(50) COLLATE {1},
		[Notes] VARCHAR(MAX) COLLATE {1},
		[DataWithholdingReason] VARCHAR(255) COLLATE {1},
		PRIMARY KEY ([entry_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
		INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertIdentificationUnits(self):
		
		query = """
		INSERT INTO [IdentificationUnit] 
		(
			[CollectionSpecimenID],
			[RowGUID],
			[LastIdentificationCache],
			[TaxonomicGroup],
			[DisplayOrder],
			[OrderCache],
			[FamilyCache],
			[HierarchyCache],
			[LifeStage],
			[Gender],
			[NumberOfUnits],
			[NumberOfUnitsModifier],
			[UnitIdentifier],
			[UnitDescription],
			[Notes],
			[DataWithholdingReason]
		)
		SELECT 
			[CollectionSpecimenID],
			[RowGUID],
			[LastIdentificationCache],
			ISNULL(tg_enum.[Code], 'unknown')
			ISNULL ([DisplayOrder], ROW_NUMBER() OVER(PARTITION BY [CollectionSpecimenID] ORDER BY [entry_num] ASC)) AS [DisplayOrder],
			[OrderCache],
			[FamilyCache],
			[HierarchyCache],
			[LifeStage],
			[Gender],
			[NumberOfUnits],
			[NumberOfUnitsModifier],
			[UnitIdentifier],
			[UnitDescription],
			[Notes],
			[DataWithholdingReason]
		FROM [{0}] iu_temp
		LEFT JOIN CollTaxonomicGroup_Enum tg_enum
		ON tg_enum.code = iu_temp.[TaxonomicGroup]
		ORDER BY iu_temp.[entry_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateIUTempTable(self):
		# update the IdentificationUnitIDs in iu_temptable
		query = """
		UPDATE iu_temp
		SET iu_temp.IdentificationUnitID = iu.IdentificationUnitID,
		iu_temp.[DisplayOrder] = iu.[DisplayOrder]
		FROM [{0}] iu_temp
		INNER JOIN [IdentificationUnit] iu
		ON iu_temp.[RowGUID] = iu.[RowGUID]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return

	'''
	def __updateIdentificationUnits(self):
		# update all newly inserted IdentificationUnits with the values from iu_temptable
		query = """
		UPDATE iu
		SET
			iu.[LastIdentificationCache] = iu_temp.[LastIdentificationCache],
			iu.[OrderCache] = iu_temp.[OrderCache],
			iu.[FamilyCache] = iu_temp.[FamilyCache],
			iu.[HierarchyCache] = iu_temp.[HierarchyCache],
			iu.[LifeStage] = iu_temp.[LifeStage],
			iu.[Gender] = iu_temp.[Gender],
			iu.[NumberOfUnits] = iu_temp.[NumberOfUnits],
			iu.[NumberOfUnitsModifier] = iu_temp.[NumberOfUnitsModifier],
			iu.[UnitIdentifier] = iu_temp.[UnitIdentifier],
			iu.[UnitDescription] = iu_temp.[UnitDescription],
			iu.[TaxonomicGroup] = iu_temp.[TaxonomicGroup],
			iu.[DataWithholdingReason] = iu_temp.[DataWithholdingReason]
		FROM [IdentificationUnit] iu
		INNER JOIN [{0}] iu_temp
			ON (iu.[RowGUID] = iu_temp.[RowGUID])
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return
	'''


	def __updateIUDicts(self):
		iu_ids = self.getIDsForIUDicts()
		for iu_dict in self.iu_dicts:
			entry_num = iu_dict['entry_num']
			iu_dict['RowGUID'] = iu_ids[entry_num]['RowGUID']
			iu_dict['IdentificationUnitID'] = iu_ids[entry_num]['IdentificationUnitID']
			iu_dict['DisplayOrder'] = iu_ids[entry_num]['DisplayOrder']
		return


	def getIDsForIUDicts(self):
		query = """
		SELECT iu_temp.[entry_num], iu.IdentificationUnitID, iu.[RowGUID], iu.[DisplayOrder]
		FROM [IdentificationUnit] iu
		INNER JOIN [{0}] iu_temp
		ON iu_temp.[RowGUID] = iu.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		iu_ids = {}
		for row in rows:
			if not row[0] in iu_ids:
				iu_ids[row[0]] = {}
			iu_ids[row[0]]['IdentificationUnitID'] = row[1]
			iu_ids[row[0]]['RowGUID'] = row[2]
			iu_ids[row[0]]['DisplayOrder'] = row[3]
		
		return iu_ids


	def setIdentificationUnitInPart(self):
		iuip_dicts = []
		for iu_dict in self.iu_dicts:
			if 'CollectionSpecimenParts' in iu_dict:
				for csp_dict in iu_dict['CollectionSpecimenParts']:
					iuip_dict = {
						'CollectionSpecimenID': csp_dict['CollectionSpecimenID'],
						'IdentificationUnitID': csp_dict['IdentificationUnitID'],
						'SpecimenPartID': csp_dict['SpecimenPartID']
					}
					iuip_dicts.append(iuip_dict)
		iuip_setter = IdentificationUnitInPartSetter(self.dc_db)
		iuip_setter. setIdentificationUnitInPartDicts(iuip_dicts)
		iuip_setter.setIdentificationUnitInPartData()
		
		return
