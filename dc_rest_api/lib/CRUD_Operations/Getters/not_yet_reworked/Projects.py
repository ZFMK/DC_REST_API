
import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('elastic_indexer')
log_query = logging.getLogger('query')


class Projects():
	def __init__(self, datagetter):
		self.datagetter = datagetter
		
		self.cur = self.datagetter.cur
		self.con = self.datagetter.con


	def get_data_page(self):
		
		query = """
		SELECT 
		[rownumber],
		idstemp.[idshash] AS [_id],
		CONCAT(idstemp.[DatabaseID], '_', pp.[ProjectID]) AS [DB_ProjectID],
		pp.[ProjectID],
		pp.[Project],
		pp.[ProjectURI]
		FROM [#temp_iu_part_ids] idstemp
		INNER JOIN CollectionSpecimen cs 
		ON cs.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID]
		LEFT JOIN [CollectionProject] cp
		ON cp.CollectionSpecimenID = cs.CollectionSpecimenID
		LEFT JOIN [ProjectProxy] pp
		ON pp.ProjectID = cp.ProjectID
		ORDER BY [rownumber]
		;"""
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.rows = self.cur.fetchall()
		self.rows2dict()
		
		
		return self.projects_dict


	def rows2dict(self):
		self.projects_dict = {}
		
		for row in self.rows:
			if row[1] not in self.projects_dict:
				self.projects_dict[row[1]] = {}
				self.projects_dict[row[1]]['Projects'] = []
			
			project = {
				'DB_ProjectID': row[2],
				'ProjectID': row[3],
				'Project': row[4],
				'ProjectURI': row[5]
			}
			
			self.projects_dict[row[1]]['Projects'].append(project)
			
		return















