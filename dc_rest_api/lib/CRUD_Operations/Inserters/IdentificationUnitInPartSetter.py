import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

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
		
		return


	def setIdentificationUnitInPartDicts(self, json_dicts = []):
		self.iup_dicts = []
		iup_count = 1
		for iup_dict in json_dicts:
			iup_dict['entry_num'] = iup_count
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
			iu_temp.[CollectionSpecimenID],
			iu_temp.[IdentificationUnitID],
			iu_temp.[SpecimenPartID],
			ISNULL (iu_temp.[DisplayOrder], ROW_NUMBER() OVER(PARTITION BY iu_temp.[CollectionSpecimenID], iu_temp.[IdentificationUnitID] ORDER BY iu_temp.[entry_num] ASC)) AS [DisplayOrder],
			iu_temp.[RowGUID]
		FROM [{0}] iu_temp
		 -- prevent that an existing part is entered again, what will also not work because the three keys form a primery key together
		LEFT JOIN [IdentificationUnitInPart] iuip
		ON iuip.[CollectionSpecimenID] = iu_temp.[CollectionSpecimenID]
		AND iuip.[IdentificationUnitID] = iu_temp.[IdentificationUnitID]
		AND iuip.[SpecimenPartID] = iu_temp.[SpecimenPartID]
		WHERE iuip.[SpecimenPartID] IS NULL
		ORDER BY iu_temp.[entry_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return

