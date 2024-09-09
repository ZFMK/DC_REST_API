import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter


class IdentificationGetter(DataGetter):
	def __init__(self, dc_db, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
		self.withholded = []
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_i_temptable'



	def getByPrimaryKeys(self, cs_i_ids):
		self.createGetTempTable()
		
		batchsize = 600
		while len(cs_i_ids) > 0:
			cached_ids = cs_i_ids[:batchsize]
			del cs_i_ids[:batchsize]
			placeholders = ['(?, ?, ?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			DROP TABLE IF EXISTS [#i_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#i_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[IdentificationUnitID] INT NOT NULL,
				[IdentificationSequence] SMALLINT NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
				INDEX [IdentificationSequence_idx] ([IdentificationSequence])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#i_pks_to_get_temptable] (
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[IdentificationSequence]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [Identification] i
			INNER JOIN [#i_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = i.[CollectionSpecimenID]
			AND pks.[IdentificationUnitID] = i.[IdentificationUnitID]
			AND pks.[IdentificationSequence] = i.[IdentificationSequence]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		identifications = self.getData()
		
		return identifications


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		identifications = self.getData()
		
		return identifications



	def getData(self):
		
		query = """
		SELECT
		g_temp.[row_num],
		g_temp.[rowguid_to_get] AS [RowGUID],
		i.[CollectionSpecimenID],
		i.[IdentificationUnitID],
		i.[IdentificationSequence],
		i.[RowGUID],
		i.[NameURI],
		i.[TaxonomicName],
		i.[VernacularTerm],
		i.[IdentificationDay],
		i.[IdentificationMonth],
		i.[IdentificationYear],
		i.[IdentificationDateSupplement],
		i.[ResponsibleName],
		i.[ResponsibleAgentURI],
		i.[IdentificationCategory],
		i.[IdentificationQualifier],
		i.[TypeStatus],
		i.[TypeNotes],
		i.[ReferenceTitle],
		i.[ReferenceURI],
		i.[ReferenceDetails],
		i.[Notes]
		FROM [{0}] g_temp
		INNER JOIN [Identification] i
		ON i.[RowGUID] = g_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.i_rows = self.cur.fetchall()
		self.rows2list()
		
		return self.i_list


	def rows2list(self):
		self.i_list = []
		for row in self.i_rows:
			self.i_list.append(dict(zip(self.columns, row)))
		return


	def list2dict(self):
		self.i_dict = {}
		for element in self.i_list:
			if element['CollectionSpecimenID'] not in self.i_dict:
				self.i_dict[element['CollectionSpecimenID']] = {}
			if element['IdentificationUnitID'] not in self.i_dict[element['CollectionSpecimenID']]:
				self.i_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']] = {}
			
			self.i_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['IdentificationSequence']] = element 
		
		return













