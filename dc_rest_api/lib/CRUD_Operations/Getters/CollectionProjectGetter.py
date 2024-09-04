import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter

class CollectionProjectGetter(DataGetter):
	def __init__(self, dc_db):
		DataGetter.__init__(self, dc_db)
		
		self.get_temptable = '#get_cp_temptable'



	def getByPrimaryKeys(self, cp_ids):
		self.createGetTempTable()
		
		batchsize = 1000
		while len(cp_ids) > 0:
			cached_ids = cp_ids[:batchsize]
			del cp_ids[:batchsize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			DROP TABLE IF EXISTS [#cp_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#cp_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[ProjectID] INT NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID])
				INDEX [ProjectID_idx] ([ProjectID])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#cp_pks_to_get_temptable] (
				[CollectionSpecimenID],
				[ProjectID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [CollectionProject] cp
			INNER JOIN [#cp_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = cp.[CollectionSpecimenID] AND pks.[ProjectID] = cp.[ProjectID]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		collectionprojects = self.getData()
		
		return collectionprojects


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		collectionprojects = self.getData()
		
		return collectionprojects



	def getData(self):
		
		query = """
		SELECT
		cp_temp.[row_num],
		cp_temp.[rowguid_to_get] AS [RowGUID],
		cp.[CollectionSpecimenID],
		pp.[ProjectID],
		pp.[Project],
		pp.[ProjectURI],
		pp.[StableIdentifierBase],
		pp.[StableIdentifierTypeID]
		FROM [{0}] cp_temp
		INNER JOIN [CollectionProject] cp
		ON cp.[RowGUID] = cp_temp.[rowguid_to_get]
		INNER JOIN [ProjectProxy] pp
		ON pp.[ProjectID] = cp.[ProjectID]
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.cp_rows = self.cur.fetchall()
		self.rows2list()
		
		return self.cp_list


	def rows2list(self):
		self.cp_list = []
		for row in self.cp_rows:
			self.cp_list.append(dict(zip(self.columns, row)))
		
		return


	def list2dict(self):
		self.cp_dict = {}
		for element in self.cp_list:
			if element['CollectionSpecimenID'] not in self.cp_dict:
				self.cp_dict[element['CollectionSpecimenID']] = {}
				
			self.cp_dict[element['CollectionSpecimenID']][element['ProjectID']] = element
		
		return











