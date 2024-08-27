import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.JSON2TempTable import JSON2TempTable


class IdentificationInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		
		self.temptable = '#identification_temptable'
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'IdentificationUnitID', 'None allowed': False},
			{'colname': 'IdentificationSequence'},
			{'colname': 'TaxonomicName', 'default': 'unknown', 'None allowed': False},
			{'colname': 'NameURI'},
			{'colname': 'VernacularTerm'},
			{'colname': 'IdentificationDay'},
			{'colname': 'IdentificationMonth'},
			{'colname': 'IdentificationYear'},
			{'colname': 'IdentificationDateSupplement'},
			{'colname': 'ResponsibleName'},
			{'colname': 'ResponsibleAgentURI'},
			{'colname': 'IdentificationCategory'},
			{'colname': 'IdentificationQualifier'},
			{'colname': 'TypeStatus'},
			{'colname': 'TypeNotes'},
			{'colname': 'ReferenceTitle'},
			{'colname': 'ReferenceURI'},
			{'colname': 'ReferenceDetails'},
			{'colname': 'Notes'}
		]
		
		self.json2temp = JSON2TempTable(self.dc_db, self.schema)


	def insertIdentificationData(self):
		self.__createIdentificationTempTable()
		
		self.json2temp.set_datadicts(self.i_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__insertIdentifications()
		self.__updateITempTable()
		
		self.__updateIDicts()
		
		return


	def setIdentificationDicts(self, json_dicts = []):
		self.i_dicts = []
		i_count = 1
		for i_dict in json_dicts:
			i_dict['entry_num'] = i_count
			i_count += 1
			self.i_dicts.append(i_dict)
		return




	def __createIdentificationTempTable(self):
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
		[IdentificationSequence] SMALLINT,
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		[TaxonomicName] NVARCHAR(255) COLLATE {1} NOT NULL,
		[NameURI] NVARCHAR(255) COLLATE {1},
		[VernacularTerm] NVARCHAR(255) COLLATE {1},
		[IdentificationDay] TINYINT,
		[IdentificationMonth] TINYINT,
		[IdentificationYear] SMALLINT,
		[IdentificationDateSupplement] NVARCHAR(255) COLLATE {1},
		[ResponsibleName] NVARCHAR(255) COLLATE {1},
		[ResponsibleAgentURI] VARCHAR(255) COLLATE {1},
		[IdentificationCategory] NVARCHAR(50) COLLATE {1},
		[IdentificationQualifier] NVARCHAR(50) COLLATE {1},
		[TypeStatus] NVARCHAR(50) COLLATE {1},
		[TypeNotes] NVARCHAR(MAX) COLLATE {1},
		[ReferenceTitle] NVARCHAR(255) COLLATE {1},
		[ReferenceURI] VARCHAR(255) COLLATE {1},
		[ReferenceDetails] NVARCHAR(50) COLLATE {1},
		[Notes] NVARCHAR(MAX) COLLATE {1},
		PRIMARY KEY ([entry_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
		INDEX [TaxonomicName_idx] ([TaxonomicName]),
		INDEX [IdentificationDay] ([IdentificationDay]),
		INDEX [IdentificationMonth] ([IdentificationMonth]),
		INDEX [IdentificationYear] ([IdentificationYear]),
		INDEX [ResponsibleName] ([ResponsibleName]),
		INDEX [TypeStatus] ([TypeStatus])
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertIdentifications(self):
		
		query = """
		INSERT INTO [Identification] (
			 -- TaxonomicName or VernacularTerm must not be NULL according to a CHECK Constraint for the Identification table
			[TaxonomicName],
			[VernacularTerm],
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[IdentificationSequence],
			[RowGUID],
			[NameURI],
			[IdentificationDay],
			[IdentificationMonth],
			[IdentificationYear],
			[IdentificationDateSupplement],
			[ResponsibleName],
			[ResponsibleAgentURI],
			[IdentificationCategory],
			[IdentificationQualifier],
			[TypeStatus],
			[TypeNotes],
			[ReferenceTitle],
			[ReferenceURI],
			[ReferenceDetails],
			[Notes]
		)
		SELECT 
			i_temp.[TaxonomicName],
			i_temp.[VernacularTerm],
			i_temp.[CollectionSpecimenID],
			i_temp.[IdentificationUnitID],
			ISNULL(i.[IdentificationSequence] + ROW_NUMBER() OVER(PARTITION BY i_temp.[IdentificationUnitID] ORDER BY i_temp.[entry_num] ASC), ROW_NUMBER() OVER(PARTITION BY i_temp.[IdentificationUnitID] ORDER BY i_temp.[entry_num] ASC)),
			i_temp.[RowGUID],
			i_temp.[NameURI],
			i_temp.[IdentificationDay],
			i_temp.[IdentificationMonth],
			i_temp.[IdentificationYear],
			i_temp.[IdentificationDateSupplement],
			i_temp.[ResponsibleName],
			i_temp.[ResponsibleAgentURI],
			i_temp.[IdentificationCategory],
			i_temp.[IdentificationQualifier],
			i_temp.[TypeStatus],
			i_temp.[TypeNotes],
			i_temp.[ReferenceTitle],
			i_temp.[ReferenceURI],
			i_temp.[ReferenceDetails],
			i_temp.[Notes]
		FROM [{0}] i_temp
		 -- left join for getting the IdentificationSequence number
		LEFT JOIN (
			SELECT MAX(IdentificationSequence) AS [IdentificationSequence], [CollectionSpecimenID], [IdentificationUnitID]
			FROM [Identification]
			GROUP BY [CollectionSpecimenID], [IdentificationUnitID]
		) AS i 
			ON i.[CollectionSpecimenID] = i_temp.[CollectionSpecimenID] AND i.[IdentificationUnitID] = i_temp.[IdentificationUnitID]
		ORDER BY i_temp.[entry_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateITempTable(self):
		# update the IdentificationIDs in i_temptable
		query = """
		UPDATE i_temp
		SET i_temp.[IdentificationSequence] = i.[IdentificationSequence]
		FROM [{0}] i_temp
		INNER JOIN [Identification] i
		ON i_temp.[RowGUID] = i.[RowGUID]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateIDicts(self):
		
		i_ids = self.getIDsForIDicts()
		for i_dict in self.i_dicts:
			entry_num = i_dict['entry_num']
			i_dict['RowGUID'] = i_ids[entry_num]['RowGUID']
			i_dict['IdentificationSequence'] = i_ids[entry_num]['IdentificationSequence']
		return


	def getIDsForIDicts(self):
		query = """
		SELECT i_temp.[entry_num],
		i.[IdentificationSequence], i.[RowGUID]
		FROM [Identification] i
		INNER JOIN [{0}] i_temp
		ON i_temp.[RowGUID] = i.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		i_ids = {}
		for row in rows:
			if not row[0] in i_ids:
				i_ids[row[0]] = {}
			i_ids[row[0]]['IdentificationSequence'] = row[1]
			i_ids[row[0]]['RowGUID'] = row[2]
		
		return i_ids
