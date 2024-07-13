import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.IUPReader import IUPReader 
from dc_rest_api.lib.CRUD_Operations.JSON2TempTable import JSON2TempTable

class IdentificationUnitInPartSetter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#iup_temptable'
		
		self.schema = [
			{'colname': 'iup_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'IdentificationUnitID', 'None allowed': False},
			{'colname': 'SpecimenPartID', 'None allowed': False}
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)
		


	def setIdentificationUnitInPartData(self):
		self.__createIdentificationUnitInPartTempTable()
		
		self.json2temp.set_datadicts(self.iup_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__insertIdentificationUnits()
		self.__updateIUTempTable()
		
		self.__updateIUDicts()
		
		return


	def setIdentificationUnitInPartDicts(self, json_dicts = []):
		self.iup_dicts = []
		iup_count = 1
		for iup_dict in json_dicts:
			iup_dict['iup_num'] = iup_count
			iup_count += 1
			self.iup_dicts.append(iup_dict)
		return


	def __createIdentificationUnitInPartTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[identificationunit_num] INT NOT NULL,
		[CollectionSpecimenID] INT NOT NULL,
		[IdentificationUnitID] INT DEFAULT NULL,
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		[LastIdentificationCache] VARCHAR(255) DEFAULT 'unknown' COLLATE {1} NOT NULL,
		[TaxonomicGroup] VARCHAR(50) DEFAULT 'unknown' COLLATE {1} NOT NULL,
		[DisplayOrder] SMALLINT,
		[LifeStage] VARCHAR(255) COLLATE {1},
		[Gender] VARCHAR(50) COLLATE {1},
		[NumberOfUnits] SMALLINT,
		[NumberOfUnitsModifier] VARCHAR(50) COLLATE {1},
		[UnitIdentifier] VARCHAR(50) COLLATE {1},
		[UnitDescription] VARCHAR(50) COLLATE {1},
		[Notes] VARCHAR(MAX) COLLATE {1},
		[DataWithholdingReason] VARCHAR(255) COLLATE {1},
		PRIMARY KEY ([identificationunit_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
		INDEX [RowGUID_idx] ([RowGUID]),
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
			[TaxonomicGroup],
			ISNULL ([DisplayOrder], ROW_NUMBER() OVER(PARTITION BY [CollectionSpecimenID] ORDER BY [identificationunit_num] ASC)) AS [DisplayOrder],
			[LifeStage],
			[Gender],
			[NumberOfUnits],
			[NumberOfUnitsModifier],
			[UnitIdentifier],
			[UnitDescription],
			[Notes],
			[DataWithholdingReason]
		FROM [{0}] iu_temp
		ORDER BY iu_temp.[identificationunit_num]
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


	def __updateIdentificationUnits(self):
		# update all newly inserted IdentificationUnits with the values from iu_temptable
		query = """
		UPDATE iu
		SET
			iu.[LastIdentificationCache] = iu_temp.[LastIdentificationCache],
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
		 -- ensure that only inserted identificationunits are updated
		 -- not needed anymore, because the RowGUIDs are considered as UNIQUE
		 -- INNER JOIN [#new_iu_ids] nui
			 -- ON iu_temp.[RowGUID] = nui.[RowGUID] 
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateIUDicts(self):
		iu_ids = self.getIDsForIUDicts()
		for iu_dict in self.iu_dicts:
			identificationunit_num = iu_dict['identificationunit_num']
			iu_dict['RowGUID'] = iu_ids[identificationunit_num]['RowGUID']
			iu_dict['IdentificationUnitID'] = iu_ids[identificationunit_num]['IdentificationUnitID']
			iu_dict['DisplayOrder'] = iu_ids[identificationunit_num]['DisplayOrder']
		return


	def getIDsForIUDicts(self):
		query = """
		SELECT iu_temp.[identificationunit_num], iu.IdentificationUnitID, iu.[RowGUID], iu.[DisplayOrder]
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
