import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('./config.ini')

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Inserters.IndependentTablesInsert import IndependentTablesInsert

from dc_rest_api.lib.CRUD_Operations.Inserters.IdentificationUnitInserter import IdentificationUnitInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.CollectionAgentInserter import CollectionAgentInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.SpecimenPartInserter import SpecimenPartInserter

class CollectionSpecimenInserter():
	
	def __init__(self, dc_db, uid, users_roles = []):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.uid = uid
		self.users_roles = users_roles
		
		self.temptable = '#specimen_temptable'
		self.accnr_prefix = config.get('option', 'AccNr_prefix', fallback = '')
		
		self.schema = [
			{'colname': '@id', 'None allowed': False},
			{'colname': 'CollectionSpecimenID'},
			{'colname': 'CollectionEventID'},
			{'colname': 'ExternalDatasourceID'},
			{'colname': 'CollectionID'},
			{'colname': 'DatabaseURN'},
			{'colname': 'AccessionNumber'},
			{'colname': 'DepositorsAccessionNumber'},
			{'colname': 'DepositorsName'},
			{'colname': 'ExternalIdentifier'},
			{'colname': 'OriginalNotes'},
			{'colname': 'AdditionalNotes'},
			{'colname': 'DataWithholdingReason'}
		]
		
		self.json2temp = JSON2TempTable(self.dc_db, self.schema)


	def insertSpecimenData(self, flattened_json):
		if 'CollectionSpecimens' in flattened_json:
			independent_tables = IndependentTablesInsert(self.dc_db, flattened_json, self.uid, self.users_roles)
			independent_tables.insertIndependentTables()
			
			self.specimen_dicts = flattened_json['CollectionSpecimens']
			
			specimen_list = [self.specimen_dicts[cs_id] for cs_id in self.specimen_dicts]
			
			independent_tables.setLinkedIDs(specimen_list)
			
			self.__createSpecimenTempTable()
			
			self.json2temp.set_datadicts(self.specimen_dicts)
			self.json2temp.fill_temptable(self.temptable)
			
			self.__insertSpecimen()
			self.__setMissingAccessionNumbers()
			self.__updateCSTempTable()
			self.__updateSpecimenDicts()
			
			independent_tables.insertCollectionProjects(specimen_list)
			
			identificationunits = []
			collectionagents = []
			specimenparts = []
			
			for dict_id in self.specimen_dicts:
				cs_dict = self.specimen_dicts[dict_id]
				
				if 'IdentificationUnits' in cs_dict:
					for iu_dict in cs_dict['IdentificationUnits']:
						iu_dict['CollectionSpecimenID'] = cs_dict['CollectionSpecimenID']
						identificationunits.append(iu_dict)
				
				if 'CollectionAgents' in cs_dict:
					for ca_dict in cs_dict['CollectionAgents']:
						ca_dict['CollectionSpecimenID'] = cs_dict['CollectionSpecimenID']
						collectionagents.append(ca_dict)
				
				if 'CollectionSpecimenParts' in cs_dict:
					for csp_dict in cs_dict['CollectionSpecimenParts']:
						csp_dict['CollectionSpecimenID'] = cs_dict['CollectionSpecimenID']
						specimenparts.append(csp_dict)
			
			iu_inserter = IdentificationUnitInserter(self.dc_db)
			iu_inserter.setIdentificationUnitDicts(identificationunits)
			iu_inserter.insertIdentificationUnitData()
			
			ca_inserter = CollectionAgentInserter(self.dc_db)
			ca_inserter.setCollectionAgentDicts(collectionagents)
			ca_inserter.insertCollectionAgentData()
			
			csp_inserter = SpecimenPartInserter(self.dc_db)
			csp_inserter.setSpecimenPartDicts(specimenparts)
			independent_tables.setLinkedIDs(specimenparts)
			csp_inserter.insertSpecimenPartData()
			
			return


	def __createSpecimenTempTable(self):
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
		[CollectionEventID] INT DEFAULT NULL,
		[CollectionID] INT DEFAULT NULL,
		[ExternalDatasourceID] INT,
		[AccessionNumber] VARCHAR(50) COLLATE {1},
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		[DepositorsAccessionNumber] VARCHAR(50) COLLATE {1},
		[DepositorsName] VARCHAR(255) COLLATE {1},
		[ExternalIdentifier] VARCHAR(100) COLLATE {1},
		[OriginalNotes] VARCHAR(MAX) COLLATE {1},
		[AdditionalNotes] VARCHAR(MAX) COLLATE {1},
		[DataWithholdingReason] VARCHAR(255) COLLATE {1},
		PRIMARY KEY ([@id]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [AccessionNumber_idx] ([AccessionNumber]),
		INDEX [RowGUID_idx] ([RowGUID]),
		INDEX [DepositorsAccessionNumber_idx] ([DepositorsAccessionNumber])
		)
		;""".format(self.temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertSpecimen(self):
		
		query = """
		INSERT INTO [CollectionSpecimen] (
			[AccessionNumber],
			[RowGUID],
			[CollectionEventID],
			[CollectionID],
			[ExternalDatasourceID],
			[DepositorsAccessionNumber],
			[DepositorsName],
			[ExternalIdentifier],
			[OriginalNotes],
			[AdditionalNotes],
			[DataWithholdingReason]
		)
		SELECT cs_temp.[AccessionNumber],
		cs_temp.[RowGUID],
		cs_temp.[CollectionEventID],
		cs_temp.[CollectionID],
		cs_temp.[ExternalDatasourceID],
		cs_temp.[DepositorsAccessionNumber],
		cs_temp.[DepositorsName],
		cs_temp.[ExternalIdentifier],
		cs_temp.[OriginalNotes],
		cs_temp.[AdditionalNotes],
		cs_temp.[DataWithholdingReason]
		FROM [{0}] cs_temp
		;
		""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __setMissingAccessionNumbers(self):
		# set missing AccessionNumbers in CollectionSpecimen table
		query = """
		UPDATE cs
		SET cs.[AccessionNumber] = CONCAT(?, cs.[CollectionSpecimenID])
		FROM [CollectionSpecimen] cs
		INNER JOIN [{0}] cs_temp
		ON cs_temp.[RowGUID] = cs.[RowGUID]
		WHERE cs.[AccessionNumber] IS NULL
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query, [self.accnr_prefix])
		self.con.commit()
		return


	def __updateCSTempTable(self):
		query = """
		UPDATE cs_temp
		SET cs_temp.[AccessionNumber] = cs.[AccessionNumber],
		cs_temp.CollectionSpecimenID = cs.[CollectionSpecimenID]
		FROM [{0}] cs_temp
		INNER JOIN [CollectionSpecimen] cs 
		ON cs.RowGUID = cs_temp.RowGUID
		;""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateSpecimenDicts(self):
		# write the inserted ids back to the specimen_dicts
		
		cs_ids = self.getIDsForSpecimenDicts()
		
		for dict_id in self.specimen_dicts:
			cs_dict = self.specimen_dicts[dict_id]
			cs_dict['CollectionSpecimenID'] = cs_ids[dict_id]['CollectionSpecimenID']
			cs_dict['RowGUID'] = cs_ids[dict_id]['RowGUID']
			
			# fill the ids for the next level...
			if 'IdentificationUnits' in cs_dict:
				for iu_dict in cs_dict['IdentificationUnits']:
					iu_dict['CollectionSpecimenID'] = cs_dict['CollectionSpecimenID']
			
		return


	def getIDsForSpecimenDicts(self):
		# select all CollectionSpecimenIDs and RowGUIDs
		query = """
		SELECT cs_temp.[@id], cs.CollectionSpecimenID, cs.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [{0}] cs_temp
		ON cs_temp.[RowGUID] = cs.[RowGUID]
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		cs_ids = {}
		for row in rows:
			if not row[0] in cs_ids:
				cs_ids[row[0]] = {}
			cs_ids[row[0]]['CollectionSpecimenID'] = row[1]
			cs_ids[row[0]]['RowGUID'] = row[2]
		
		return cs_ids


