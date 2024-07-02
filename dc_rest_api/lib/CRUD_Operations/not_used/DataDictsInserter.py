import pudb


from dc_importer.DCImporter.CollectionSpecimenImporter import CollectionSpecimenImporter
from dc_importer.DCImporter.CollectionEventImporter import CollectionEventImporter
from dc_importer.DCImporter.CollectionAgentImporter import CollectionAgentImporter
from dc_importer.DCImporter.IdentificationUnitImporter import IdentificationUnitImporter
from dc_importer.DCImporter.SpecimenPartImporter import SpecimenPartImporter
from dc_importer.DCImporter.ProjectImporter import ProjectImporter
from dc_importer.DCImporter.ExternalDatasourceImporter import ExternalDatasourceImporter

from DBConnectors.MSSQLConnector import MSSQLConnector


from configparser import ConfigParser
config = ConfigParser()
config.read('./config.ini')

import logging
import logging.config

logging.config.fileConfig('logging.conf')
log = logging.getLogger('dc_importer')


class JSON2DataDicts():
	def __init__(self, dc_db, users_roles, datadicts = []):
		self.datadicts = datadicts
		self.users_roles = users_roles
		
		self.dc_db = dc_db


	def setDataSets(self, datadicts = []):
		if len(datadicts) < 1:
			return
		
		dataset_num = 1
		for datadict in datadicts:
			datadict['dataset_num'] = dataset_num
			self.datadicts.append(datadict)
			dataset_num += 1
		return


	def setCollectionSpecimens(self):
		self.specimens = []
		cs_count = 1
		for datadict in self.datadicts:
			if 'CollectionSpecimens' in datadict:
				for cs_dict in datadict['CollectionSpecimens']:
					cs_dict['dataset_num'] = datadict['dataset_num']
					cs_dict['collectionspecimen_num'] = cs_count
					cs_count += 1
					self.specimens.append(dict(csdict))
		return




	def addProjectToDataDicts(self, project = {}):
		# add the project from submission form to datadict when no projects are given in excel file
		if 'Project' in project:
			for datadict in self.datadicts:
				if 'Projects' not in datadict: 
					datadict['Projects'] = []
				datadict['Projects'].append(project)
		return


	def insertDataDictsToDC(self):
		
		cs_importer = CollectionSpecimenImporter(self.dc_db)
		cs_importer.insertSpecimenData(self.datadicts)
		self.updateCollectionSpecimenIDs(cs_importer)
		
		self.setEvents()
		ce_importer = CollectionEventImporter(self.dc_db)
		ce_importer.insertEventData(self.events)
		
		self.setAgents()
		ca_importer = CollectionAgentImporter(self.dc_db)
		ca_importer.insertAgentData(self.agents)
		
		self.setIdentificationUnits()
		iu_importer = IdentificationUnitImporter(self.dc_db)
		iu_importer.insertIdentificationUnitData(self.identificationunits)
		self.updateIdentificationUnitIDs(iu_importer)
		
		self.setIdentifications()
		i_importer = IdentificationImporter(self.dc_db)
		i_importer.insertIdentificationData(self.identifications)
		self.updateIdentificationIDs(i_importer)
		
		self.setSpecimenParts()
		csp_importer = SpecimenPartImporter(self.dc_db)
		csp_importer.insertSpecimenPartData(self.specimenparts)
		self.updateSpecimenPartIDs(csp_importer)
		
		self.setProjects()
		p_importer = ProjectImporter(self.dc_db, self.users_roles)
		p_importer.insertProjectData(self.projects)
		
		self.setExternalDatasource()
		ds_importer = ExternalDatasourceImporter(self.dc_db, self.users_roles)
		ds_importer.insertExternalDatasourceData(self.datasources)
		
		return


	def updateCollectionSpecimenIDs(self, cs_importer):
		cs_ids = cs_importer.getIDsForCSDicts()
		for datadict in self.datadicts:
			dataset_num = datadict['dataset_num']
			if dataset_num in cs_ids:
				datadict['CollectionSpecimenID'] = cs_ids[dataset_num]['CollectionSpecimenID']
				datadict['RowGUID'] = cs_ids[dataset_num]['RowGUID']
		return


	def setEvents(self):
		self.events = []
		for datadict in self.datadicts:
			if 'CollectionEvent' in datadict:
				ce_dict = datadict['CollectionEvent']
				ce_dict['dataset_num'] = datadict['dataset_num']
				self.events.append(dict(ce_dict))
		return


	def setAgents(self):
		self.agents = []
		for datadict in self.datadicts:
			if 'CollectionAgents' in datadict:
				for ca_dict in datadict['CollectionAgents']:
					ca_dict['dataset_num'] = datadict['dataset_num']
					self.agents.append(dict(ca_dict))
		return


	def setIdentificationUnits(self):
		self.identificationunits = []
		iu_count = 1
		for datadict in self.datadicts:
			if 'IdentificationUnits' in datadict:
				for iu_dict in datadict['IdentificationUnits']:
					iu_dict['dataset_num'] = datadict['dataset_num']
					iu_dict['identificationunit_num'] = iu_count
					iu_count += 1
					self.identificationunits.append(dict(iu_dict))
		return


	def updateIdentificationUnitIDs(self, iu_importer):
		iu_ids = iu_importer.getIDsForIUDicts()
		for datadict in self.datadicts:
			if 'IdentificationUnits' in datadict:
				for iu_dict in datadict['IdentificationUnits']:
					dataset_num = iu_dict['dataset_num']
					identificationunit_num = iu_dict['identificationunit_num']
					iu_dict['CollectionSpecimenID'] = iu_ids[dataset_num][identificationunit_num]['CollectionSpecimenID']
					iu_dict['IdentificationUnitID'] = iu_ids[dataset_num][identificationunit_num]['IdentificationUnitID']
					iu_dict['RowGUID'] = iu_ids[dataset_num][identificationunit_num]['RowGUID']
		return


	def setIdentifications(self):
		self.identifications = []
		i_count = 1
		for datadict in self.datadicts:
			if 'IdentificationUnits' in datadict:
				for iu_dict in datadict['IdentificationUnits']:
					if 'Identifications' in iu_dict:
						for i_dict in iu_dict['Identifications']:
							i_dict['dataset_num'] = datadict['dataset_num']
							i_dict['identificationunit_num'] = iu_dict['identificationunit_num']
							i_dict['CollectionSpecimenID'] = iu_dict['CollectionSpecimenID']
							i_dict['IdentificationUnitID'] = iu_dict['IdentificationUnitID']
							i_dict['identification_num'] = i_count
							i_count += 1
							self.identifications.append(dict(i_dict))
		return


	def updateIdentificationIDs(self, i_importer):
		i_ids = i_importer.getIDsForIDicts()
		for datadict in self.datadicts:
			if 'IdentificationUnits' in datadict:
				for iu_dict in datadict['IdentificationUnits']:
					identificationunit_num = iu_dict['identificationunit_num']
					if 'Identifications' in iu_dict:
						for i_dict in iu_dict['Identifications']:
							dataset_num = i_dict['dataset_num']
							identification_num = i_dict['identification_num']
							i_dict['IdentificationSequence'] = i_ids[dataset_num][identificationunit_num][identification_num]['IdentificationSequence']
							i_dict['RowGUID'] = i_ids[dataset_num][identificationunit_num][identification_num]['RowGUID']
		return


	def setSpecimenParts(self):
		self.specimenparts = []
		csp_count = 1
		for datadict in self.datadicts:
			if 'CollectionSpecimenParts' in datadict:
				for csp_dict in datadict['CollectionSpecimenParts']:
					csp_dict['dataset_num'] = datadict['dataset_num']
					csp_dict['specimenpart_num'] = csp_count
					csp_count += 1
					self.specimenparts.append(dict(csp_dict))
		return


	def updateSpecimenPartIDs(self, csp_importer):
		csp_ids = csp_importer.getIDsForCSPDicts()
		for datadict in self.datadicts:
			if 'CollectionSpecimenParts' in datadict:
				for csp_dict in datadict['CollectionSpecimenParts']:
					specimenpart_num = csp_dict['specimenpart_num']
					dataset_num = csp_dict['dataset_num']
					csp_dict['CollectionSpecimenID'] = csp_ids[dataset_num][specimenpart_num]['CollectionSpecimenID']
					csp_dict['SpecimenPartID'] = csp_ids[dataset_num][specimenpart_num]['SpecimenPartID']
					csp_dict['AccessionNumber'] = csp_ids[dataset_num][specimenpart_num]['AccessionNumber']
					csp_dict['MaterialCategory'] = csp_ids[dataset_num][specimenpart_num]['MaterialCategory']
					csp_dict['RowGUID'] = csp_ids[dataset_num][specimenpart_num]['RowGUID']
		return


	def setProjects(self):
		self.projects = []
		p_count = 1
		for datadict in self.datadicts:
			if 'Projects' in datadict:
				for p_dict in datadict['Projects']:
					p_dict['dataset_num'] = datadict['dataset_num']
					p_dict['project_num'] = p_count
					p_count += 1 
					self.projects.append(dict(p_dict))
		return


	def setExternalDatasource(self):
		self.datasources = []
		for datadict in self.datadicts:
			if 'CollectionExternalDatasource' in datadict:
				ds_dict = datadict['CollectionExternalDatasource']
				ds_dict['dataset_num'] = datadict['dataset_num']
				self.datasources.append(dict(ds_dict))
		return

	'''
	def getInsertedCollectionSpecimenIDs(self):
		return self.inserted_specimen_ids

	def getInsertedAccessionNumbers(self):
		return self.inserted_accessionnumbers
	'''
