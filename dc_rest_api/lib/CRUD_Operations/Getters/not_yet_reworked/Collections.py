import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('elastic_indexer')
log_query = logging.getLogger('query')


class CollectionRelationsTempTable():
	def __init__(self, datagetter):
		self.datagetter = datagetter
		
		self.cur = self.datagetter.cur
		self.con = self.datagetter.con
		
		self.createCollectionRelationsTempTable()
		self.fillCollectionRelationsTempTable()


	def createCollectionRelationsTempTable(self):
		query = """
		DROP TABLE IF EXISTS [#temp_collection_relations]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#temp_collection_relations] (
		[RelationID] INT IDENTITY PRIMARY KEY,
		[AncestorID] INT NOT NULL,
		[DescendantID] INT NOT NULL,
		[PathLength] INT,
		[TreeLevel] INT,
		INDEX [idx_AncestorID] ([AncestorID]),
		INDEX [idx_DescendantID] ([DescendantID]),
		INDEX [idx_PathLength] ([PathLength]),
		INDEX [idx_TreeLevel] ([TreeLevel])
		)
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		return


	def fillCollectionRelationsTempTable(self):
		query = """
		INSERT INTO [#temp_collection_relations] ([AncestorID], [DescendantID], [PathLength])
		SELECT [CollectionID], [CollectionID], 0 FROM [Collection];
		"""
		
		log_query.debug(query)
		self.cur.execute(query)
		self.con.commit()
		
		pathlength = 0
		count = self.getCountByPathLength(pathlength)
		
		while count > 0:
			#pudb.set_trace()
			# set the parent relations
			logger.info("Fill collection relation_table pathlength {0}, with {1} possible childs".format(pathlength, count))
			query = """
			INSERT INTO [#temp_collection_relations] ([AncestorID], [DescendantID], [PathLength])
			SELECT c2.[CollectionID], tr.[DescendantID], tr.[PathLength] + 1
			FROM [#temp_collection_relations] tr
			INNER JOIN [Collection] c1 
				ON c1.[CollectionID] = tr.[AncestorID]
			INNER JOIN [Collection] c2
				ON c2.[CollectionID] = c1.[CollectionParentID]
			WHERE c1.[CollectionID] != c1.[CollectionParentID]
			AND tr.[PathLength] = ?
			;"""
			
			log_query.info(query)
			self.cur.execute(query, [pathlength])
			self.con.commit()
			
			pathlength += 1
			count = self.getCountByPathLength(pathlength)
		return


	def getCountByPathLength(self, pathlength):
		query = """
		SELECT COUNT(*)
		FROM [#temp_collection_relations] tr
		WHERE tr.pathLength = ?
		"""
		
		log_query.info(query)
		self.cur.execute(query, [pathlength])
		row = self.cur.fetchone()
		if row is not None:
			count = row[0]
		else:
			count = 0
		return count


	'''
	def setTreeLevels(self):
		
		query = """
		UPDATE `{0}_Taxa` t
		INNER JOIN (
			SELECT `DescendantTaxonID`, MAX(pathlength) AS `level`
			FROM `taxa_relations_temp`
			GROUP BY `DescendantTaxonID`
		) ml 
		ON (ml.`DescendantTaxonID` = t.id)
		SET t.`level` = ml.`level`
		""".format(self.db_suffix)
		
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
	'''


class Collections():
	def __init__(self, datagetter):
		self.datagetter = datagetter
		
		self.cur = self.datagetter.cur
		self.con = self.datagetter.con


	def get_data_page(self):
		
		query = """
		DROP TABLE IF EXISTS [#temp_collection]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#temp_collection] (
			[rownumber] INT,
			[_id] NVARCHAR(255) NOT NULL,
			[CollectionID] INT,
			[CollectionName] NVARCHAR(255),
			[CollectionAcronym] NVARCHAR(10),
			INDEX [idx_id] ([_id]),
			INDEX [idx_CollectionID] ([CollectionID])
		)
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		INSERT INTO [#temp_collection]
		([rownumber], [_id], [CollectionID], [CollectionName], [CollectionAcronym])
		SELECT 
		[rownumber],
		idstemp.[idshash] AS [_id],
		COALESCE(c_csp.[CollectionID], c_cs.[CollectionID]) AS [CollectionID],
		COALESCE(c_csp.[CollectionName], c_cs.[CollectionName]) AS [CollectionName],
		COALESCE(c_csp.[CollectionAcronym], c_cs.[CollectionAcronym]) AS [CollectionAcronym]
		FROM [#temp_iu_part_ids] idstemp
		INNER JOIN CollectionSpecimen cs 
			ON cs.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID]
		LEFT JOIN CollectionSpecimenPart csp 
			ON csp.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] AND csp.[SpecimenPartID] = idstemp.[SpecimenPartID]
		LEFT JOIN [Collection] c_csp
			ON c_csp.[CollectionID] = csp.[CollectionID]
		LEFT JOIN [Collection] c_cs
			ON c_cs.[CollectionID] = csp.[CollectionID]
		ORDER BY [rownumber]
		"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		SELECT 
		tc.[rownumber],
		tc.[_id], 
		tc.[CollectionID], tc.[CollectionName], tc.[CollectionAcronym],
		c.[CollectionID] AS ParentCollectionID, c.[CollectionName] AS ParentCollectionName, tl.[TreeLevel],
		c.[CollectionParentID]
		FROM [#temp_collection] tc
		INNER JOIN [#temp_collection_relations] tcr
			ON tc.[CollectionID] = tcr.[DescendantID]
		INNER JOIN [Collection] c
			ON c.[CollectionID] = tcr.[AncestorID]
		INNER JOIN (
			SELECT MAX(tcr.PathLength) AS TreeLevel, tcr.[DescendantID]
			FROM [#temp_collection_relations] tcr
			GROUP BY tcr.[DescendantID]
		) tl
			ON tl.[DescendantID] = c.[CollectionID]
		ORDER BY tc.[rownumber], tl.[TreeLevel]
		;"""
		
		self.cur.execute(query)
		
		# self.columns = [column[0] for column in self.cur.description]
		
		self.rows = self.cur.fetchall()
		self.rows2dict()
		
		return self.collections_dict


	def rows2dict(self):
		self.collections_dict = {}
		
		for row in self.rows:
			if row[1] not in self.collections_dict:
				self.collections_dict[row[1]] = {
					'CollectionID': row[2],
					'CollectionName': row[3],
					'CollectionAcronym': row[4]
				}
				self.collections_dict[row[1]]['CollectionsTree'] = []
			
			parentcollection = {
				'CollectionID': row[5],
				'CollectionName': row[6],
				'TreeLevel': row[7],
				'ParentCollectionID': row[8]
			}
			self.collections_dict[row[1]]['CollectionsTree'].append(parentcollection)
		
		return
