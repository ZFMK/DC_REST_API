import pudb
import math

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class DataGetter():
	def __init__(self, ):
		
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		
		self.ids_temptable = '#getter_ids_temptable'
		
		self.pagesize = 1000
		self.max_page = 1



	def createIDsTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.ids_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
			[row_num] INT IDENTITY,
			[CollectionSpecimenID] INT UNIQUE,
			[RowGUID] VARCHAR(64) COLLATE {1} UNIQUE,
			[CollectionEventID] INT,
			[ExternalDatasourceID] INT,
			[AccessionNumber] NVARCHAR(50) COLLATE {1},
			PRIMARY KEY ([row_num])
			INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
			INDEX [CollectionEventID] ([CollectionEventID]),
			INDEX [ExternalDatasourceID] ([ExternalDatasourceID]),
			INDEX [AccessionNumber] ([AccessionNumber]),
			INDEX [RowGUID] ([RowGUID])
		)
		;""".format(self.ids_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return



	def fillIDsTempTableByPrimaryKeys(self, specimen_ids):
		self.createIDsTempTable()
		
		# insert of with placeholders is limited to 2100 values
		pagesize = 1000
		while len(specimen_ids) > 0:
			cached_ids = specimen_ids[:pagesize]
			del specimen_ids[:pagesize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			INSERT INTO [{0}] ([CollectionSpecimenID])
			VALUES {1}
			;""".format(self.ids_temptable)
			querylog.info(query)
			self.cur.execute(query, [values])
			self.con.commit()
			
		query = """
		UPDATE ids_temp
		SET ids_temp.[RowGUID] = cs.[RowGUID]
		FROM [{0}] ids_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.[CollectionSpecimenID] = ids_temp.[CollectionSpecimenID]
		;""".format(self.ids_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def fillIDsTempTableByRowGUIDs(self, rowguids):
		self.createIDsTempTable()
		
		# insert of with placeholders is limited to 2100 values
		pagesize = 1000
		while len(rowguids) > 0:
			cached_ids = rowguids[:pagesize]
			del rowguids[:pagesize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			INSERT INTO [{0}] ([RowGUID])
			VALUES {1}
			;""".format(self.ids_temptable)
			querylog.info(query)
			self.cur.execute(query, [values])
			self.con.commit()
			
		query = """
		UPDATE ids_temp
		SET ids_temp.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		FROM [{0}] ids_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.[RowGUID] = ids_temp.[RowGUID]
		;""".format(self.ids_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def set_max_page(self):
		query = """
		SELECT COUNT(CollectionSpecimenID) FROM [{0}]
		;""".format(self.ids_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		row = self.cur.fetchone()
		self.rownumber = row[0]
		
		self.max_page = math.ceil(self.rownumber / self.pagesize)
		if self.max_page < 1:
			self.max_page = 1
		
		return





