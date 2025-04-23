import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class CollectionExternalDatasourceMatcher():
	def __init__(self, dc_db, temptable):
		self.dc_db = dc_db
		self.temptable = temptable
		
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.prefiltered_temptable = '#prefiltered_externaldatasources'


	def matchExistingExternalDatasources(self):
		self.__createPrefilteredTempTable()
		self.__matchIntoPrefiltered()
		self.addExternalDatasourceSHA(self.prefiltered_temptable)
		
		self.__matchPrefilteredToTempTable()


	def __createPrefilteredTempTable(self):
		
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.prefiltered_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[ExternalDatasourceID] INT,
		[ExternalDatasourceName] VARCHAR(255) COLLATE {1} NOT NULL,
		[ExternalDatasourceVersion] VARCHAR(255) COLLATE {1},
		[Rights] NVARCHAR(500) COLLATE {1},
		[ExternalDatasourceAuthors] NVARCHAR(200) COLLATE {1},
		[ExternalDatasourceURI] VARCHAR(300) COLLATE {1},
		[ExternalDatasourceInstitution] VARCHAR(300) COLLATE {1},
		[ExternalAttribute_NameID] VARCHAR(255) COLLATE {1},
		 -- [InternalNotes] VARCHAR(1500) COLLATE {1},
		 -- [PreferredSequence] TINYINT,
		 -- [Disabled] BIT,
		[RowGUID] UNIQUEIDENTIFIER NOT NULL,
		 -- 
		[externaldatasource_sha] VARCHAR(64) COLLATE {1},
		INDEX [externaldatasource_sha_idx] ([externaldatasource_sha])
		)
		;""".format(self.prefiltered_temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __matchIntoPrefiltered(self):
		# first match all existing ExternalDatasources by ExternalDatasourceName and ExternalDatasourceURI
		
		query = """
		INSERT INTO [{0}] (
			[ExternalDatasourceID],
			[ExternalDatasourceName],
			[ExternalDatasourceVersion],
			[Rights],
			[ExternalDatasourceAuthors],
			[ExternalDatasourceURI],
			[ExternalDatasourceInstitution],
			[ExternalAttribute_NameID],
			 -- [InternalNotes],
			 -- [PreferredSequence],
			 -- [Disabled],
			[RowGUID]
		)
		SELECT 
			ed.[ExternalDatasourceID],
			ed.[ExternalDatasourceName],
			ed.[ExternalDatasourceVersion],
			ed.[Rights],
			ed.[ExternalDatasourceAuthors],
			ed.[ExternalDatasourceURI],
			ed.[ExternalDatasourceInstitution],
			ed.[ExternalAttribute_NameID],
			 -- ed.[InternalNotes],
			 -- ed.[PreferredSequence],
			 -- ed.[Disabled],
			ed.[RowGUID]
		FROM [CollectionExternalDatasource] ed
		INNER JOIN [{1}] ed_temp
		ON (ed_temp.[ExternalDatasourceName] = ed.[ExternalDatasourceName])
		AND ((ed_temp.[ExternalDatasourceURI] = ed.[ExternalDatasourceURI]) OR (ed_temp.[ExternalDatasourceURI] IS NULL AND ed.[ExternalDatasourceURI] IS NULL))
		 -- WHERE ed_temp.[ExternalDatasourceID] IS NULL
		;""".format(self.prefiltered_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def addExternalDatasourceSHA(self, tablename):
		query = """
		UPDATE t
		SET [externaldatasource_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[ExternalDatasourceName],
			[ExternalDatasourceVersion],
			[Rights],
			[ExternalDatasourceAuthors],
			[ExternalDatasourceURI],
			[ExternalDatasourceInstitution],
			[ExternalAttribute_NameID]
			 -- [InternalNotes],
			 -- [PreferredSequence],
			 -- [Disabled]
		)), 2)
		FROM [{0}] t
		;""".format(tablename)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __matchPrefilteredToTempTable(self):
		query = """
		UPDATE ed_temp
		SET ed_temp.[ExternalDatasourceID] = pf.[ExternalDatasourceID],
		ed_temp.[RowGUID] = pf.[RowGUID]
		FROM [{0}] ed_temp
		INNER JOIN [{1}] pf
		ON pf.[externaldatasource_sha] = ed_temp.[externaldatasource_sha]
		;""".format(self.temptable, self.prefiltered_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
