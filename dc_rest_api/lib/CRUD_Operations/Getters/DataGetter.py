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
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.get_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		CREATE TABLE [{0}] (
			[row_num] INT IDENTITY,
			[rowguid_to_get] uniqueidentifier,
			PRIMARY KEY ([row_num]),
			INDEX [rowguid_to_get_idx] ([rowguid_to_get])
		) 
		;""".format(self.get_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()


	def fillGetTempTable(self):
		pagesize = 2000
		while len(self.row_guids) > 0:
			cached_guids = self.row_guids[:pagesize]
			del self.row_guids[:pagesize]
			placeholders = ['(?)' for _ in cached_guids]
			
			query = """
			INSERT INTO [{0}]
			VALUES {1}
			""".format(self.get_temptable, ', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, cached_guids)
			self.con.commit()
		
		return


	def getProjectClause(self, clause_connector = 'AND'):
		projectclause = ""
		if len(self.users_project_ids) > 0:
			placeholders = ['?' for project_id in self.users_project_ids]
			placeholderstring = ', '.join(placeholders)
			projectclause = "{0} cp.ProjectID NOT IN ({1})".format(clause_connector, placeholderstring)
		
		return projectclause



