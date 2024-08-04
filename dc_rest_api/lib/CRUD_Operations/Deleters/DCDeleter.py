import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class DCDeleter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()


	# not sure where to put this method, doubled in each child or once here but then with the need to provide the tablename as parameter
	def deleteFromTableByRowGUIDs(self, tablename, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness(tablename)
		self.deleteIdentifications(tablename)
		return


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



