import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Deleters.DCDeleter import DCDeleter


class CollectionEventDeleter(DCDeleter):
	def __init__(self, dc_db, users_project_ids = []):
		DCDeleter.__init__(self, dc_db, users_project_ids)
		self.prohibited = []
		self.delete_temptable = '#event_to_delete'


	def deleteByPrimaryKeys(self, event_ids):
		self.createDeleteTempTable()
		
		query = """
		DROP TABLE IF EXISTS [#event_pks_to_delete_temptable]
		"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#event_pks_to_delete_temptable] (
			[CollectionEventID] INT NOT NULL,
			INDEX [CollectionEventID_idx] ([CollectionEventID])
		)
		;"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		pagesize = 2000
		while len(event_ids) > 0:
			cached_ids = event_ids[:pagesize]
			del event_ids[:pagesize]
			placeholders = ['(?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			INSERT INTO [#event_pks_to_delete_temptable] (
			[CollectionEventID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
		
		
		# must be out of the while loop that fills the #event_pks_to_delete_temptable,
		# otherwise RowGUIDs are inserted more than once
		query = """
		INSERT INTO [{0}] ([rowguid_to_delete])
		SELECT DISTINCT ce.[RowGUID]
		FROM [CollectionEvent] ce
		INNER JOIN [#event_pks_to_delete_temptable] pks
		ON pks.[CollectionEventID] = ce.[CollectionEventID]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		self.filterOtherwiseConnectedEvents()
		self.checkRowGUIDsUniqueness('CollectionEvent')
		self.deleteCollectionEventLocalisation()
		self.deleteFromTable('CollectionEvent')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.filterOtherwiseConnectedEvents()
		self.checkRowGUIDsUniqueness('CollectionEvent')
		self.deleteCollectionEventLocalisation()
		
		self.deleteFromTable('CollectionEvent')
		return


	def filterOtherwiseConnectedEvents(self):
		# CollectionSpecimens must be deleted before CollectionEvents, so
		# it can be checked, if the CollectionsEvents are connected to other CollectionSpecimens, too
		# if so, they should not and can not be deleted
		
		query = """
		DELETE rg_temp
		FROM [{0}] rg_temp
		INNER JOIN [CollectionEvent] ce
		ON ce.[RowGUID] = rg_temp.[rowguid_to_delete]
		LEFT JOIN CollectionSpecimen cs
		ON cs.CollectionEventID = ce.CollectionEventID
		WHERE cs.CollectionSpecimenID IS NOT NULL
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def deleteCollectionEventLocalisation(self):
		
		query = """
		DELETE cel
		FROM [CollectionEventLocalisation] cel
		INNER JOIN [CollectionEvent] ce
		ON ce.CollectionEventID = cel.CollectionEventID
		INNER JOIN [{0}] rg_temp
		ON ce.[RowGUID] = rg_temp.[rowguid_to_delete]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


