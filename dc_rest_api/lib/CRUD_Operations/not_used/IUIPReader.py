import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

class IUIPReader():
	def __init__(self, iu_dicts = [], csp_dicts = []):
		self.iu_dicts = iu_dicts
		self.csp_dicts = csp_dicts
		
		
		
		self.iuips_list = []
		self.iuips_dict = {}


	def readIUIPsFromDicts(self, dicts):
		for iuip in dicts:
			if ['CollectionSpecimenID'] in iuip and ['IdentificationUnitID'] in iuip and ['SpecimenPartID'] in iuip:
				key = '{0}_{1}_{2}'.format(iuip['CollectionSpecimenID'], iuip['IdentificationUnitID'], iuip['SpecimenPartID'])
				
				if key not in self.iuips_dict:
					self.iuips_dict[key] = {
						'CollectionSpecimenID': iuip['CollectionSpecimenID'],
						'IdentificationUnitID': iuip['IdentificationUnitID'],
						'SpecimenPartID': iuip['SpecimenPartID']
					}
		return
