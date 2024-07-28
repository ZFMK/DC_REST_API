import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('./config.ini')

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.JSON2TempTable import JSON2TempTable

from dc_rest_api.lib.CRUD_Operations.CollectionInserter import CollectionInserter
from dc_rest_api.lib.CRUD_Operations.CollectionEventInserter import CollectionEventInserter
from dc_rest_api.lib.CRUD_Operations.ExternalDatasourceInserter import ExternalDatasourceInserter
from dc_rest_api.lib.CRUD_Operations.IdentificationUnitInserter import IdentificationUnitInserter
from dc_rest_api.lib.CRUD_Operations.CollectionAgentInserter import CollectionAgentInserter
from dc_rest_api.lib.CRUD_Operations.SpecimenPartInserter import SpecimenPartInserter

class CollectionSpecimenInserter():
	
	def __init__(self, dc_db, users_roles = []):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.users_roles = users_roles
		
		self.temptable = '#specimen_temptable'
		self.accnr_prefix = config.get('option', 'AccNr_prefix', fallback = '')
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID'},
			{'colname': 'AccessionNumber'},
			{'colname': 'DepositorsAccessionNumber'},
			{'colname': 'DepositorsName'},
			{'colname': 'ExternalIdentifier'},
			{'colname': 'OriginalNotes'},
			{'colname': 'AdditionalNotes'},
			{'colname': 'DataWithholdingReason'}
		]
		
		self.json2temp = JSON2TempTable(self.dc_db, self.schema)


	def insertSpecimenData(self):
		self.__createSpecimenTempTable()
		self.json2temp.set_datadicts(self.specimen_dicts)
		self.json2temp.fill_temptable(self.temptable)
		self.__insertSpecimen()
		self.__setMissingAccessionNumbers()
		self.__updateCSTempTable()
		self.__updateSpecimenDicts()
		
		collections = []
		for cs_dict in self.specimen_dicts:
			if 'Collection' in cs_dict:
				c_dict = cs_dict['Collection']
				# add the CollectionSpecimenID here as the events should be inserted for specific Specimens
				c_dict['CollectionSpecimenID'] = cs_dict['CollectionSpecimenID']
				collections.append(c_dict)
		
		c_inserter = CollectionInserter(self.dc_db)
		c_inserter.setCollectionDicts(collections)
		c_inserter.insertCollectionData()
		
		events = []
		for cs_dict in self.specimen_dicts:
			if 'CollectionEvent' in cs_dict:
				e_dict = cs_dict['CollectionEvent']
				# add the CollectionSpecimenID here as the events should be inserted for specific Specimens
				e_dict['CollectionSpecimenID'] = cs_dict['CollectionSpecimenID']
				events.append(e_dict)
		
		e_inserter = CollectionEventInserter(self.dc_db)
		e_inserter.setCollectionEventDicts(events)
		e_inserter.insertCollectionEventData()
		
		externaldatasources = []
		for cs_dict in self.specimen_dicts:
			if 'CollectionExternalDatasource' in cs_dict:
				ed_dict = cs_dict['CollectionExternalDatasource']
				# add the CollectionSpecimenID here as the ExternalDatasorces should be inserted for specific Specimens
				ed_dict['CollectionSpecimenID'] = cs_dict['CollectionSpecimenID']
				externaldatasources.append(ed_dict)
		
		ed_inserter = ExternalDatasourceInserter(self.dc_db, users_roles = self.users_roles)
		ed_inserter.setExternalDatasourceDicts(externaldatasources)
		ed_inserter.insertExternalDatasourceData()
		
		
		identificationunits = []
		for cs_dict in self.specimen_dicts:
			if 'IdentificationUnits' in cs_dict:
				for iu_dict in cs_dict['IdentificationUnits']:
					iu_dict['CollectionSpecimenID'] = cs_dict['CollectionSpecimenID']
					identificationunits.append(iu_dict)
					
		iu_inserter = IdentificationUnitInserter(self.dc_db)
		iu_inserter.setIdentificationUnitDicts(identificationunits)
		iu_inserter.insertIdentificationUnitData()
		
		collectionagents = []
		for cs_dict in self.specimen_dicts:
			if 'CollectionAgents' in cs_dict:
				for ca_dict in cs_dict['CollectionAgents']:
					ca_dict['CollectionSpecimenID'] = cs_dict['CollectionSpecimenID']
					collectionagents.append(ca_dict)
		
		ca_inserter = CollectionAgentInserter(self.dc_db)
		ca_inserter.setCollectionAgentDicts(collectionagents)
		ca_inserter.insertCollectionAgentData()
		
		specimenparts = []
		for cs_dict in self.specimen_dicts:
			if 'CollectionSpecimenParts' in cs_dict:
				for csp_dict in cs_dict['CollectionSpecimenParts']:
					csp_dict['CollectionSpecimenID'] = cs_dict['CollectionSpecimenID']
					specimenparts.append(csp_dict)
		
		csp_inserter = SpecimenPartInserter(self.dc_db)
		csp_inserter.setSpecimenPartDicts(specimenparts)
		csp_inserter.insertSpecimenPartData()
		
		return


	def setSpecimenDicts(self, json_dicts = []):
		self.specimen_dicts = json_dicts
		cs_count = 1
		for cs_dict in self.specimen_dicts:
			cs_dict['entry_num'] = cs_count
			cs_count += 1
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
		[entry_num] INT NOT NULL,
		[CollectionSpecimenID] INT DEFAULT NULL,
		[AccessionNumber] VARCHAR(50) COLLATE {1},
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		[DepositorsAccessionNumber] VARCHAR(50) COLLATE {1},
		[DepositorsName] VARCHAR(255) COLLATE {1},
		[ExternalDatasourceID] INT,
		[ExternalIdentifier] VARCHAR(100) COLLATE {1},
		[OriginalNotes] VARCHAR(MAX) COLLATE {1},
		[AdditionalNotes] VARCHAR(MAX) COLLATE {1},
		[DataWithholdingReason] VARCHAR(255) COLLATE {1},
		PRIMARY KEY ([entry_num]),
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
			[DepositorsAccessionNumber],
			[DepositorsName],
			[ExternalDatasourceID],
			[ExternalIdentifier],
			[OriginalNotes],
			[AdditionalNotes],
			[DataWithholdingReason]
		)
		SELECT cs_temp.[AccessionNumber],
		cs_temp.[RowGUID],
		cs_temp.[DepositorsAccessionNumber],
		cs_temp.[DepositorsName],
		cs_temp.[ExternalDatasourceID],
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
		# write the inserted ids back to the json_dicts that where provided as payload
		
		cs_ids = self.getIDsForSpecimenDicts()
		
		for specimen_dict in self.specimen_dicts:
			entry_num = specimen_dict['entry_num']
			specimen_dict['CollectionSpecimenID'] = cs_ids[entry_num]['CollectionSpecimenID']
			specimen_dict['RowGUID'] = cs_ids[entry_num]['RowGUID']
			
			# fill the ids for the next level...
			if 'IdentificationUnits' in specimen_dict:
				for iu_dict in specimen_dict['IdentificationUnits']:
					iu_dict['CollectionSpecimenID'] = specimen_dict['CollectionSpecimenID']
			
		return


	def getIDsForSpecimenDicts(self):
		# select all CollectionSpecimenIDs and RowGUIDs
		query = """
		SELECT cs_temp.[entry_num], cs.CollectionSpecimenID, cs.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [{0}] cs_temp
		 -- ON cs_temp.CollectionSpecimenID = cs.CollectionSpecimenID
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


