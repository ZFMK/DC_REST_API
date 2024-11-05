import pudb
import math

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class DataGetter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation


	def createGetTempTable(self):
		querylog.warning('#########################')
		querylog.warning(self.cur)
		
		
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.get_temptable)
		querylog.info(query)
		querylog.warning(self.cur)
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		CREATE TABLE [{0}] (
			[rowguid_to_get] uniqueidentifier,
			[DatabaseURN] NVARCHAR(500) COLLATE {1},
			INDEX [rowguid_to_get_idx] ([rowguid_to_get])
		) 
		;""".format(self.get_temptable, self.collation)
		querylog.info(query)
		querylog.warning(self.cur)
		self.cur.execute(query)
		self.con.commit()


	def fillGetTempTable(self):
		pagesize = 1000
		while len(self.row_guids) > 0:
			cached_guids = self.row_guids[:pagesize]
			del self.row_guids[:pagesize]
			placeholders = ['(?)' for _ in cached_guids]
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			VALUES {1}
			""".format(self.get_temptable, ', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, cached_guids)
			self.con.commit()
		
		return


	def setDatabaseURN(self):
		query = """
		UPDATE [{0}]
		SET [DatabaseURN] = ?
		;""".format(self.get_temptable)
		querylog.info(query)
		self.cur.execute(query, [self.dc_db.db_URN])
		self.con.commit()
		
		return


	def getProjectClause(self, clause_connector = 'AND'):
		projectclause = ""
		if len(self.users_project_ids) > 0:
			placeholders = ['?' for project_id in self.users_project_ids]
			placeholderstring = ', '.join(placeholders)
			projectclause = "{0} cp.ProjectID NOT IN ({1})".format(clause_connector, placeholderstring)
		
		return projectclause


	def getProjectJoinForWithhold(self, clause_connector = 'AND'):
		projectjoin = ""
		projectwhere = ""
		
		if len(self.users_project_ids) > 0:
			placeholders = ['?' for project_id in self.users_project_ids]
			placeholderstring = ', '.join(placeholders)
		
			projectjoin = """
			LEFT JOIN (
			SELECT cs.[CollectionSpecimenID], cs.[RowGUID], cp.ProjectID
					FROM [CollectionSpecimen] cs
					INNER JOIN [CollectionProject] cp
					ON cs.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
					AND 
					cp.ProjectID IN ({0})
			) AS users_cs
			ON users_cs.RowGUID = cs.RowGUID
			""".format(placeholderstring)
			
			projectwhere = "{0} users_cs.ProjectID IS NULL".format(clause_connector)
		
		return projectjoin, projectwhere
		

