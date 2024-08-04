import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('elastic_indexer')
log_query = logging.getLogger('query')

from DBConnectors.MSSQLConnector import MSSQLConnector


class CreateChangedSpecimensProcedure():
	def __init__(self, dc_params):
		self.dc_con = MSSQLConnector(dc_params['connectionstring'])
		self.server_url = dc_params['server_url']
		self.database_name = dc_params['database_name']
		self.accronym = dc_params['accronym']
		self.database_id = dc_params['database_id']
		
		self.cur = self.dc_con.getCursor()
		self.con = self.dc_con.getConnection()
		
		self.create_procedure_if_not_exists()


	def create_procedure_if_not_exists(self):
		query = """
		SELECT * FROM sys.objects WHERE type = 'P' AND OBJECT_ID = OBJECT_ID('dbo.procChangesFor_DC_API')
		;"""
		
		self.cur.execute(query)
		row = self.cur.fetchone()
		
		if row is not None:
			return
		
		# insert procedure as defined in procedure_query
		self.cur.execute(procedure_query)
		self.con.commit()
		return



procedure_query = """
-- --------------------------------------------------------------------------
-- Get Changes for DC API
-- Query all "Top Level" tables in DC to get the changes since last timestamp
-- Top level databases: Analysis, Annotation, CollectionEvent, Collection, 
--                      CollectionSpecimen, ExternalIdentifier, Method,
--                      Parameter, Processing, ProjectProxy, Regulation,
--                      Task, Transaction
-- Parameter: LastChangeDateStr [varchar] - date timestamp of date since when 
--                                          entries have been changed
-- A temporary table '#ChangedSpecimens' must exist from the caller side
-- Author: Peter Grobe
-- Date: 2024-04-05
-- --------------------------------------------------------------------------
CREATE Procedure [dbo].[procChangesFor_DC_API] 
	@LastChangeDateStr varchar(30)
AS  

/* -- Definition for temporary table '#ChangedSpecimens':
IF OBJECT_ID('tempdb..#ChangedSpecimens') IS NOT NULL DROP TABLE #ChangedSpecimens;
CREATE TABLE #ChangedSpecimens (
	  RowCounter int IDENTITY(1,1) NOT NULL
	, CollectionSpecimenID Int NOT NULL
	, CONSTRAINT PK_CollectionSpecimen_C PRIMARY KEY (CollectionSpecimenID)
);
*/

Declare @LastChangeDate Datetime;

-- Set @LastChangeDate=CONVERT(DATETIME, '2024-02-27 13:15:35.460', 121);
Set @LastChangeDate=CONVERT(DATETIME, @LastChangeDateStr, 121);

IF OBJECT_ID('tempdb..#ChangedSpecimensTemp') IS NOT NULL DROP TABLE #ChangedSpecimensTemp;
-- IF OBJECT_ID('tempdb..#ChangedSpecimens') IS NOT NULL DROP TABLE #ChangedSpecimens;
CREATE TABLE #ChangedSpecimensTemp (
  CollectionSpecimenID Int NULL,
  TableName Nvarchar(50) COLLATE Latin1_General_CI_AS NULL,
);

Insert Into #ChangedSpecimensTemp (CollectionSpecimenID, TableName)
	Select Distinct CollectionSpecimenID As CollectionSpecimenID
			, 'CollectionSpecimenID' As TableName
		From CollectionSpecimen_log Where LogState='D' And LogDate > @LastChangeDate
	UNION
	Select Distinct cs.CollectionSpecimenID, 'CollectionSpecimenID' 
		From CollectionSpecimen cs Where cs.LogUpdatedWhen > @LastChangeDate
	UNION
	-- For the following, only updates are considered. Iinserts and deletes are of no relevance, 
	-- as they are treated by the update trigger of the main table anyways.
	Select Distinct iua.CollectionSpecimenID, 'Analysis'
		From Analysis a
			Left Join IdentificationUnitAnalysis iua On iua.AnalysisID=a.AnalysisID
		Where a.LogUpdatedWhen > @LastChangeDate
	UNION
	Select Distinct cs.CollectionSpecimenID, 'Annotation'
		From Annotation an
			Left Join CollectionSpecimen cs On (cs.CollectionSpecimenID=an.ReferencedID)
			Where an.LogUpdatedWhen > @LastChangeDate And an.ReferencedTable='CollectionSpecimen'
	UNION
	Select Distinct iu.CollectionSpecimenID, 'Annotation'
		From Annotation an
			Left Join IdentificationUnit iu On (iu.IdentificationUnitID=an.ReferencedID)
			Where an.LogUpdatedWhen > @LastChangeDate And an.ReferencedTable='IdentificationUnit';

Insert Into #ChangedSpecimensTemp (CollectionSpecimenID, TableName)
	Select csp.CollectionSpecimenID, 'Collection'
		From Collection c
			Left Join CollectionSpecimenPart csp On csp.CollectionID=c.CollectionID
		Where c.LogUpdatedWhen > @LastChangeDate
	UNION
	Select Distinct csp.CollectionSpecimenID, 'ExternalIdentifier'
		From ExternalIdentifier ei
			Left Join CollectionSpecimenPart csp On (csp.SpecimenPartID=ei.ReferencedID)
			Where ei.LogUpdatedWhen > @LastChangeDate And ei.ReferencedTable='CollectionSpecimenPart'
	UNION
	Select Distinct cs.CollectionSpecimenID, 'ExternalIdentifier'
	From ExternalIdentifier ei
		Left Join CollectionSpecimen cs On (cs.CollectionSpecimenID=ei.ReferencedID)
		Where ei.LogUpdatedWhen > @LastChangeDate And ei.ReferencedTable='CollectionSpecimen'
	UNION
	Select Distinct iuam.CollectionSpecimenID, 'Method'
		From [Method] m
			inner Join IdentificationUnitAnalysisMethod iuam On iuam.MethodID=m.MethodID
		Where m.LogUpdatedWhen > @LastChangeDate;
	/*Select Distinct iuamp.CollectionSpecimenID, 'Parameter'  -- Skiped, cause too expensive
		From [Parameter] p
			inner Join IdentificationUnitAnalysisMethodParameter iuamp On iuamp.ParameterID=p.ParameterID
		Where p.LogUpdatedWhen > @LastChangeDate
	UNION*/

	Insert Into #ChangedSpecimensTemp (CollectionSpecimenID, TableName)
	Select Distinct csp.CollectionSpecimenID, 'Processing'
		From [Processing] p
			inner Join CollectionSpecimenProcessing csp ON csp.ProcessingID=p.ProcessingID
		Where p.LogUpdatedWhen > @LastChangeDate
	UNION
	Select Distinct cp.CollectionSpecimenID, 'ProjectProxy'
		From [ProjectProxy] p
			inner Join CollectionProject cp ON cp.ProjectID=p.ProjectID
		Where p.LastChanges > @LastChangeDate
	UNION
	Select Distinct cs.CollectionSpecimenID, 'CollectionEvent'
		From CollectionEvent ce
			Left Join CollectionSpecimen cs On cs.CollectionEventID=ce.CollectionEventID
		Where ce.LogUpdatedWhen > @LastChangeDate;
	
Insert Into #ChangedSpecimens (CollectionSpecimenID)
	Select
		CollectionSpecimenID
	From #ChangedSpecimensTemp
	Where CollectionSpecimenID is not Null
	Group By CollectionSpecimenID;
"""

