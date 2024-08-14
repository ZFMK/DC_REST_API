import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class DCDeleter():
	def __init__(self, dc_db, users_project_ids = []):
		self.dc_db = dc_db
		self.users_project_ids = users_project_ids
		
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()


	def checkRowGUIDsUniqueness(self, tablename):
		query = """
		SELECT t.[RowGUID], COUNT(t.[RowGUID]) FROM [{0}] t
		INNER JOIN [{1}] rg_temp ON rg_temp.[rowguid_to_delete] = t.RowGUID
		GROUP BY t.[RowGUID]
		HAVING COUNT(t.[RowGUID]) > 1
		;""".format(tablename, self.delete_temptable)
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		if len(rows) > 0:
			raise ValueError ("Can not delete from table {0}. RowGUIDs are not unique")
		return


	def createDeleteTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.delete_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		CREATE TABLE [{0}] (
		[rowguid_to_delete] uniqueidentifier,
		INDEX [rowguid_to_delete_idx] ([rowguid_to_delete])
		) 
		;""".format(self.delete_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()


	def fillDeleteTempTable(self):
		pagesize = 2000
		while len(self.row_guids) > 0:
			cached_guids = self.row_guids[:pagesize]
			del self.row_guids[:pagesize]
			placeholders = ['(?)' for _ in cached_guids]
			
			query = """
			INSERT INTO [{0}]
			VALUES {1}
			""".format(self.delete_temptable, ', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, cached_guids)
			self.con.commit()
		
		return


	def deleteFromTable(self, tablename):
		query = """
		DELETE t FROM [{0}] t
		INNER JOIN [{1}] rg_temp
		ON rg_temp.[rowguid_to_delete] = t.[RowGUID]
		;""".format(tablename, self.delete_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def filterAllowedRowGUIDs(self, table, columns):
		# this methods checks if the Specimen or its child data are in one of the users projects
		# does only work with tables that have CollectionSpecimenID as part of their key
		prohibited = []
		
		if len(self.users_project_ids) < 1:
			raise ValueError('user has no Projects and can not delete any data set')
		
		placeholders = ['?' for project_id in self.users_project_ids]
		placeholderstring = ', '.join(placeholders)
		
		if not 'RowGUID' in columns:
			columns.append('RowGUID')
		columnstring = ', '.join(['t.' + column for column in columns])
		
		query = """
		SELECT {0}
		FROM [{1}] d_temp
		INNER JOIN [{2}] t 
		ON t.RowGUID = d_temp.[rowguid_to_delete]
		LEFT JOIN [CollectionProject] cp
		ON t.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE cp.ProjectID NOT IN ({3})
		;""".format(columnstring, self.delete_temptable, table, placeholderstring)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		rows = self.cur.fetchall()
		for row in rows:
			prohibited.append(dict(zip(columns, row)))
		
		query = """
		DELETE d_temp
		FROM [{0}] d_temp
		INNER JOIN [{1}] t
		ON t.RowGUID = d_temp.[rowguid_to_delete]
		LEFT JOIN [CollectionProject] cp
		ON t.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE cp.ProjectID NOT IN ({2})
		;""".format(self.delete_temptable, table, placeholderstring)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return prohibited
