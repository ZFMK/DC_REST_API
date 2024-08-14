
import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('elastic_indexer')
log_query = logging.getLogger('query')


class IdentificationUnitParts():
	def __init__(self, datagetter):
		self.datagetter = datagetter
		
		self.cur = self.datagetter.cur
		self.con = self.datagetter.con


	def get_data_page(self):
		
		query = """
		SELECT 
		idstemp.[idshash] AS [_id], 
		idstemp.[DatabaseURI],
		idstemp.[DatabaseAccronym],
		idstemp.[DatabaseID],
		idstemp.[CollectionSpecimenID], 
		idstemp.[IdentificationUnitID], 
		idstemp.[SpecimenPartID], 
		idstemp.[SpecimenAccessionNumber], 
		idstemp.[PartAccessionNumber],
		CONCAT_WS('/', REPLACE(dbo.StableIdentifierBase(), 'http:', 'https:'), idstemp.[CollectionSpecimenID], idstemp.[IdentificationUnitID], idstemp.[SpecimenPartID]) AS [StableIdentifierURL],
		CONVERT (NVARCHAR, cs.[LogUpdatedWhen], 121) AS [LastUpdated],
		CONVERT(NVARCHAR, cs.[AccessionDate], 120) AS [AccessionDate], 
		cs.[DepositorsName], 
		cs.[DataWithholdingReason] AS SpecimenWithholdingReason, 
		CASE 
			WHEN cs.[DataWithholdingReason] = '' THEN 'false'
			WHEN cs.[DataWithholdingReason] IS NULL THEN 'false'
			ELSE 'true'
		END AS SpecimenWithhold,
		cs.Version AS SpecimenVersion,
		CONVERT(NVARCHAR, cs.LogCreatedWhen, 121) AS SpecimenCreatedWhen,
		cs.LogCreatedBy AS SpecimenCreatedBy,
		CONVERT(NVARCHAR, cs.LogUpdatedWhen, 121) AS SpecimenUpdatedWhen,
		cs.LogUpdatedBy AS SpecimenUpdatedBy,
		csp.PreparationMethod,
		CONVERT(NVARCHAR, csp.PreparationDate, 120) AS PreparationDate,
		csp.MaterialCategory,
		csp.StorageLocation,
		csp.StorageContainer,
		csp.Stock AS StockNumber,
		csp.StockUnit AS StockNumberUnit,
		csp.[DataWithholdingReason] AS PartWithholdingReason, 
		CASE 
			WHEN csp.[DataWithholdingReason] = '' THEN 'false'
			WHEN csp.[DataWithholdingReason] IS NULL THEN 'false'
			ELSE 'true'
		END AS PartWithhold,
		-- event data
		ce.CollectionEventID,
		ce.CollectorsEventNumber,
		ce.LocalityDescription,
		ce.LocalityVerbatim,
		ce.HabitatDescription,
		ce.CollectingMethod,
		ce.CountryCache,
		CONVERT(NVARCHAR, ce.CollectionDate, 120) AS CollectionDate,
		ce.[DataWithholdingReason] AS EventWithholdingReason,
		CASE 
			WHEN ce.[DataWithholdingReason] = '' THEN 'false'
			WHEN ce.[DataWithholdingReason] IS NULL THEN 'false'
			ELSE 'true'
		END AS EventWithhold,
		ce.[DataWithholdingReasonDate] AS EventWithholdingReasonDate, 
		CASE 
			WHEN ce.[DataWithholdingReasonDate] = '' THEN 'false'
			WHEN ce.[DataWithholdingReasonDate] IS NULL THEN 'false'
			ELSE 'true'
		END AS EventWithholdDate,
		CASE 
			WHEN celcoord.Location1 IS NOT NULL AND celcoord.Location2 IS NOT NULL
				THEN REPLACE(CONCAT('POINT(', celcoord.Location1, ' ', celcoord.Location2, ')'), ',', '.')
			ELSE NULL
		END AS WGS84_Coordinate,
		celcoord.LocationAccuracy AS [WGS84_Accuracy m],
		celaltitude.Location1 AS [Altitude mNN],
		celaltitude.LocationAccuracy AS [Altitude_Accuracy mNN],
		cel_named_area.Location1 AS [NamedArea],
		cel_named_area.Location2 AS [NamedAreaURL],
		cel_named_area.LocationNotes AS [NamedAreaNotes],
		TRIM(iu.[LastIdentificationCache]) AS [LastIdentificationCache],
		TRIM(iu.[FamilyCache]) AS [FamilyCache],
		TRIM(iu.[OrderCache]) AS [OrderCache],
		TRIM(iu.[TaxonomicGroup]) AS [TaxonomicGroup],
		iu.[HierarchyCache],
		i_last.NameURI AS [TaxonNameURI],
		CONVERT(VARCHAR(256), HASHBYTES('SHA2_256', i_last.NameURI), 2) AS [TaxonNameURI_sha],
		CASE 
			WHEN iu.[OnlyObserved] = 0 THEN 'false'
			ELSE 'true'
		END AS [OnlyObserved],
		iu.[LifeStage],
		iu.[Gender],
		iu.[NumberOfUnits],
		iu.[NumberOfUnitsModifier],
		iu.[UnitIdentifier],
		iu.[UnitDescription],
		iu.[Notes] AS [IdentificationUnitNotes],
		iu.[Circumstances] AS [IdentificationUnitCircumstances],
		iu.[DataWithholdingReason] AS [IUWithholdingReason], 
		CASE 
			WHEN iu.[DataWithholdingReason] = '' THEN 'false'
			WHEN iu.[DataWithholdingReason] IS NULL THEN 'false'
			ELSE 'true'
		END AS IUWithhold,
		iu.[DisplayOrder] AS [IUDisplayOrder]
		FROM [#temp_iu_part_ids] idstemp
		INNER JOIN IdentificationUnit iu 
		ON iu.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = idstemp.[IdentificationUnitID]
		LEFT JOIN (
			SELECT i.TaxonomicName, i.NameURI, i.IdentificationUnitID, i.CollectionSpecimenID
			FROM Identification i
			INNER JOIN (
				SELECT i.IdentificationUnitID, i.CollectionSpecimenID, MAX(i.IdentificationSequence) AS IdentificationSequence_Max
				FROM Identification i
				GROUP BY i.IdentificationUnitID, i.CollectionSpecimenID
			) i_max
			ON i.IdentificationUnitID = i_max.IdentificationUnitID 
			AND i.CollectionSpecimenID = i_max.CollectionSpecimenID
			AND i.IdentificationSequence = i_max.IdentificationSequence_Max
		) i_last
		ON i_last.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] AND i_last.IdentificationUnitID = idstemp.[IdentificationUnitID]
		LEFT JOIN CollectionSpecimenPart csp 
		ON csp.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] AND csp.[SpecimenPartID] = idstemp.[SpecimenPartID]
		INNER JOIN CollectionSpecimen cs 
		ON cs.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID]
		LEFT JOIN CollectionEvent ce
		ON cs.CollectionEventID = ce.CollectionEventID
		LEFT JOIN CollectionEventLocalisation celcoord
		ON (celcoord.CollectionEventID = ce.CollectionEventID and celcoord.LocalisationSystemID = 8)
		LEFT JOIN CollectionEventLocalisation celaltitude
		ON (celaltitude.CollectionEventID = ce.CollectionEventID and celaltitude.LocalisationSystemID = 4)
		LEFT JOIN CollectionEventLocalisation cel_named_area
		ON (cel_named_area.CollectionEventID = ce.CollectionEventID and cel_named_area.LocalisationSystemID = 7)
		;"""
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.iup_rows = self.cur.fetchall()
		self.rows2dict()
		
		
		return self.iu_parts_dict


	def rows2dict(self):
		iu_parts_list = []
		for row in self.iup_rows:
			iu_parts_list.append(dict(zip(self.columns, row)))
		
		self.iu_parts_dict = {}
		for element in iu_parts_list:
			element = self.set_initial_default_values(element)
			self.iu_parts_dict[element['_id']] = element
		
		return


	def set_initial_default_values(self, iu_part_dict):
		iu_part_dict['ImageAvailable'] = False
		return iu_part_dict













