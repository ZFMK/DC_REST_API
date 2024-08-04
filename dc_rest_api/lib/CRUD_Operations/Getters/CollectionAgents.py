
import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('elastic_indexer')
log_query = logging.getLogger('query')


class CollectionAgents():
	def __init__(self, datagetter):
		self.datagetter = datagetter
		
		self.cur = self.datagetter.cur
		self.con = self.datagetter.con


	def get_data_page(self):
		
		query = """
		SELECT DISTINCT
		[rownumber],
		idstemp.[idshash] AS [_id],
		ca.CollectorsName,
		ca.CollectorsAgentURI,
		ca.CollectorsSequence,
		ca.CollectorsNumber AS CollectorsSpecimenFieldNumber,
		ca.DataWithholdingReason AS CollectorsWithholdingReason,
		CASE 
			WHEN ca.[DataWithholdingReason] = '' THEN 'false'
			WHEN ca.[DataWithholdingReason] IS NULL THEN 'false'
			ELSE 'true'
		END AS CollectorsWithhold
		FROM [#temp_iu_part_ids] idstemp
		INNER JOIN IdentificationUnit iu 
		ON iu.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = idstemp.[IdentificationUnitID]
		INNER JOIN [CollectionAgent] ca
		ON ca.[CollectionSpecimenID] = iu.[CollectionSpecimenID]
		ORDER BY idstemp.[idshash], ca.[CollectorsSequence]
		;"""
		self.cur.execute(query)
		#self.columns = [column[0] for column in self.cur.description]
		
		self.rows = self.cur.fetchall()
		self.rows2dict()
		
		# select the ProjectIDs for each Collector of a Specimen
		query = """
		SELECT DISTINCT
		[rownumber],
		idstemp.[idshash] AS [_id],
		CONCAT(idstemp.[DatabaseID], '_', cp.[ProjectID]) AS [DB_ProjectID],
		cp.[ProjectID]
		FROM [#temp_iu_part_ids] idstemp
		INNER JOIN IdentificationUnit iu 
		ON iu.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = idstemp.[IdentificationUnitID]
		LEFT JOIN [CollectionProject] cp
		ON cp.CollectionSpecimenID = iu.CollectionSpecimenID
		ORDER BY idstemp.[idshash]
		;"""
		
		self.cur.execute(query)
		self.rows = self.cur.fetchall()
		
		self.addProjectIDs()
		
		return self.collectors_dict


	def addProjectIDs(self):
		for row in self.rows:
			if row[1] in self.collectors_dict:
				for collectordict in self.collectors_dict[row[1]]['CollectionAgents']:
					if 'ProjectID' not in collectordict:
						collectordict['ProjectID'] = []
						collectordict['DB_ProjectID'] = []
					collectordict['ProjectID'].append(row[3])
					collectordict['DB_ProjectID'].append(row[2])
		return


	def rows2dict(self):
		self.collectors_dict = {}
		
		collectors_order = 0
		for row in self.rows:
			if row[1] not in self.collectors_dict:
				self.collectors_dict[row[1]] = {}
				self.collectors_dict[row[1]]['CollectionAgents'] = []
				collectors_order = 0
			
			collector = {
				'CollectorsName': row[2],
				'CollectorsAgentURI': row[3],
				'CollectorsOrder': collectors_order,
				'CollectorsSpecimenFieldNumber': row[5],
				'CollectorsDataWithholding': row[6],
				'CollectorsWithhold': row[7]
			}
			
			collectors_order += 1
			
			self.collectors_dict[row[1]]['CollectionAgents'].append(collector)
			
		return















