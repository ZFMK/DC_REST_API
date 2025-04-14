import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.IUPReader import IUPReader 
from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable

class IdentificationUnitInPartSetter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#iup_temptable'
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'IdentificationUnitID', 'None allowed': False},
			{'colname': 'SpecimenPartID', 'None allowed': False},
			{'colname': 'DisplayOrder'}
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)
		


	def setIdentificationUnitInPartData(self):
		self.__createIdentificationUnitInPartTempTable()
		
		self.json2temp.set_datadicts(self.iup_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__setIdentificationUnitInParts()
		
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
		[entry_num] INT NOT NULL,
		[CollectionSpecimenID] INT NOT NULL,
		[IdentificationUnitID] INT NOT NULL,
		[SpecimenPartID] INT NOT NULL,
		[DisplayOrder] SMALLINT,
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		PRIMARY KEY ([entry_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
		INDEX [SpecimenPartID_idx] ([SpecimenPartID]),
		INDEX [DisplayOrder_idx] ([DisplayOrder]),
		INDEX [RowGUID_idx] ([RowGUID]),
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __setIdentificationUnitInParts(self):
		
		query = """
		INSERT INTO [IdentificationUnitInPart] 
		(
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[SpecimenPartID],
			[DisplayOrder],
			[RowGUID]
		)
		SELECT 
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[SpecimenPartID],
			ISNULL ([DisplayOrder], ROW_NUMBER() OVER(PARTITION BY [CollectionSpecimenID], [IdentificationUnitID] ORDER BY [entry_num] ASC)) AS [DisplayOrder],
			[RowGUID]
		FROM [{0}] iu_temp
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
