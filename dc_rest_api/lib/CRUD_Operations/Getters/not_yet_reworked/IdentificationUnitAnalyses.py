
import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('elastic_indexer')
log_query = logging.getLogger('query')


class IdentificationUnitAnalyses():
	def __init__(self, datagetter, amp_ids, fieldname):
		self.datagetter = datagetter
		
		self.amp_ids = amp_ids
		self.fieldname = fieldname
		
		self.cur = self.datagetter.cur
		self.con = self.datagetter.con


	def create_amp_filter_temptable(self):
		# create a table that holds the wanted combinations of:
		# AnalysisID
		#	MethodID
		#		ParameterID
		# it is needed because the MethodIDs and ParameterIDs are not guarantied to be unique
		# for different analyses and methods
		
		
		query = """
		DROP TABLE IF EXISTS [#temp_amp_filter]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#temp_amp_filter] (
			[AnalysisID] INT NOT NULL,
			[MethodID] INT,
			[ParameterID] INT,
			INDEX [idx_AnalysisID] ([AnalysisID]),
			INDEX [idx_MethodID] ([MethodID]),
			INDEX [idx_ParameterID] ([ParameterID])
		)
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		placeholders = ['(?, ?, ?)' for _ in self.amp_lists]
		values = []
		for amp_list in self.amp_lists:
			values.extend(amp_list)
		
		query = """
		INSERT INTO [#temp_amp_filter] (
			[AnalysisID],
			[MethodID],
			[ParameterID]
		)
		VALUES {0}
		;""".format(', '.join(placeholders))
		self.cur.execute(query, values)
		self.con.commit()
		
		return


	def create_analyses_temptable(self):
		query = """
		DROP TABLE IF EXISTS [#temp_analysis_ids]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#temp_analysis_ids] (
			[analysis_pk] INT IDENTITY PRIMARY KEY,
			[idshash] NVARCHAR(255) NOT NULL,
			[CollectionSpecimenID] INT NOT NULL,
			[IdentificationUnitID] INT NOT NULL,
			[AnalysisID] INT NOT NULL,
			[AnalysisNumber] NVARCHAR(50) NOT NULL,
			INDEX [idx_idshash] ([idshash]),
			INDEX [idx_CollectionSpecimenID] ([CollectionSpecimenID]),
			INDEX [idx_IdentificationUnitID] ([IdentificationUnitID]),
			INDEX [idx_AnalysisID] ([AnalysisID]),
			INDEX [idx_AnalysisNumber] ([AnalysisNumber])
		)
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [#temp_analysis_ids] (
			[idshash],
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber]
		)
		SELECT 
		DISTINCT
		idstemp.[idshash],
		idstemp.[CollectionSpecimenID],
		idstemp.[IdentificationUnitID],
		iua.[AnalysisID],
		iua.[AnalysisNumber]
		FROM [#temp_iu_part_ids] idstemp
		INNER JOIN [IdentificationUnitAnalysis] iua
			ON (idstemp.CollectionSpecimenID = iua.CollectionSpecimenID AND idstemp.IdentificationUnitID = iua.IdentificationUnitID)
		INNER JOIN [#temp_amp_filter] amp_filter
		ON amp_filter.AnalysisID = iua.AnalysisID
		ORDER BY idstemp.[idshash]
		;"""
		
		self.cur.execute(query)
		self.con.commit()


	def create_methods_temptable(self):
		query = """
		DROP TABLE IF EXISTS [#temp_method_ids]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#temp_method_ids] (
			[method_pk] INT IDENTITY PRIMARY KEY,
			[analysis_pk] INT NOT NULL,
			[idshash] NVARCHAR(255) NOT NULL,
			[CollectionSpecimenID] INT NOT NULL,
			[IdentificationUnitID] INT NOT NULL,
			[AnalysisID] INT NOT NULL,
			[AnalysisNumber] NVARCHAR(50) NOT NULL,
			[MethodID] INT NOT NULL,
			[MethodMarker] NVARCHAR(50) NOT NULL,
			INDEX [idx_analysis_pk] ([analysis_pk]),
			INDEX [idx_idshash] ([idshash]),
			INDEX [idx_CollectionSpecimenID] ([CollectionSpecimenID]),
			INDEX [idx_IdentificationUnitID] ([IdentificationUnitID]),
			INDEX [idx_AnalysisID] ([AnalysisID]),
			INDEX [idx_AnalysisNumber] ([AnalysisNumber]),
			INDEX [idx_MethodID] ([MethodID]),
			INDEX [idx_MethodMarker] ([MethodMarker])
		)
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [#temp_method_ids] (
			[analysis_pk],
			[idshash],
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber],
			[MethodID],
			[MethodMarker]
		)
		SELECT 
		DISTINCT
		a_temp.analysis_pk,
		a_temp.[idshash],
		a_temp.CollectionSpecimenID,
		a_temp.IdentificationUnitID,
		a_temp.AnalysisID,
		a_temp.AnalysisNumber,
		iuam.MethodID,
		iuam.MethodMarker
		FROM [#temp_analysis_ids] a_temp
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON (
			a_temp.CollectionSpecimenID = iuam.CollectionSpecimenID 
			AND a_temp.IdentificationUnitID = iuam.IdentificationUnitID
			AND a_temp.AnalysisID = iuam.AnalysisID
			AND a_temp.AnalysisNumber = iuam.AnalysisNumber COLLATE DATABASE_DEFAULT
		)
		INNER JOIN [#temp_amp_filter] amp_filter
		ON (
			amp_filter.AnalysisID = iuam.AnalysisID
			AND amp_filter.MethodID = iuam.MethodID
		)
		;"""
		
		self.cur.execute(query)
		self.con.commit()



	def set_analyses(self):
		query = """
		SELECT 
		DISTINCT
		a_temp.analysis_pk,
		a_temp.[idshash] AS [_id],
		iua.AnalysisNumber,
		iua.Notes AS AnalysisInstanceNotes,
		iua.ExternalAnalysisURI,
		iua.ResponsibleName,
		iua.[AnalysisDate],
		iua.AnalysisResult,
		iua.AnalysisID,
		a.DisplayText AS AnalysisDisplay,
		a.Description AS AnalysisDescription,
		a.MeasurementUnit,
		a.Notes AS AnalysisTypeNotes
		FROM [#temp_analysis_ids] a_temp
		INNER JOIN [IdentificationUnitAnalysis] iua
		ON (
			a_temp.CollectionSpecimenID = iua.CollectionSpecimenID 
			AND a_temp.IdentificationUnitID = iua.IdentificationUnitID
			AND a_temp.AnalysisID = iua.AnalysisID
			AND a_temp.AnalysisNumber = iua.AnalysisNumber COLLATE DATABASE_DEFAULT
		)
		INNER JOIN [Analysis] a
		ON iua.AnalysisID = a.AnalysisID
		ORDER BY analysis_pk, a_temp.[idshash], iua.AnalysisID, iua.AnalysisNumber
		;"""
		
		log_query.debug(query)
		
		self.cur.execute(query)
		columns = [column[0] for column in self.cur.description]
		
		rows = self.cur.fetchall()
		
		self.keys_dict = {}
		
		self.analyses_dict = {}
		
		analyses = []
		for row in rows:
			analyses.append(dict(zip(columns, row)))
		
		for analysis in analyses:
			analysis_pk = analysis['analysis_pk']
			idshash = analysis['_id']
			
			self.analyses_dict[analysis_pk] = {}
			
			for key in analysis:
				if key not in ['_id', 'analysis_pk']:
					self.analyses_dict[analysis_pk][key] = analysis[key]
			
			if idshash not in self.keys_dict:
				self.keys_dict[idshash] = {}
			self.keys_dict[idshash][analysis_pk] = {}
		
		return


	def set_methods(self):
		query = """
		SELECT 
		DISTINCT
		m_temp.method_pk,
		m_temp.analysis_pk,
		m_temp.[idshash] AS [_id],
		iuam.AnalysisID,
		iuam.AnalysisNumber,
		iuam.MethodID,
		iuam.MethodMarker,
		m.DisplayText AS MethodDisplay,
		m.Description AS MethodDescription,
		m.Notes AS MethodTypeNotes
		FROM [#temp_method_ids] m_temp
		INNER JOIN [#temp_analysis_ids] a_temp
		ON a_temp.analysis_pk = m_temp.analysis_pk
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON (
			a_temp.CollectionSpecimenID = iuam.CollectionSpecimenID 
			AND m_temp.IdentificationUnitID = iuam.IdentificationUnitID
			AND m_temp.AnalysisID = iuam.AnalysisID
			AND m_temp.AnalysisNumber = iuam.AnalysisNumber COLLATE DATABASE_DEFAULT
			AND m_temp.MethodID = iuam.MethodID
			AND m_temp.MethodMarker = iuam.MethodMarker COLLATE DATABASE_DEFAULT
		)
		INNER JOIN [MethodForAnalysis] mfa
		ON (
			iuam.MethodID = mfa.MethodID
			AND iuam.AnalysisID = mfa.AnalysisID
		)
		INNER JOIN [Method] m
		ON mfa.MethodID = m.MethodID
		ORDER BY m_temp.[idshash], iuam.AnalysisID, iuam.AnalysisNumber, iuam.MethodID, iuam.MethodMarker
		;"""
		
		log_query.info(query)
		
		self.cur.execute(query)
		columns = [column[0] for column in self.cur.description]
		
		rows = self.cur.fetchall()
		
		self.methods_dict = {}
		
		methods = []
		for row in rows:
			methods.append(dict(zip(columns, row)))
		
		for method in methods:
			idshash = method['_id']
			analysis_pk = method['analysis_pk']
			method_pk = method['method_pk']
			
			self.methods_dict[method_pk] = {}
			
			self.methods_dict[method_pk]['MethodDisplay'] = method['MethodDisplay']
			self.methods_dict[method_pk]['MethodDescription'] = method['MethodDescription']
			
			'''
			for key in method:
				if key not in ('_id', 'method_pk', 'analysis_pk', 'AnalysisID', 'AnalysisNumber'):
					self.methods_dict[method_pk][key] = method[key]
			'''
			
			self.keys_dict[idshash][analysis_pk][method_pk] = {}
			
		return


	def set_parameters(self):
		query = """
		SELECT 
		DISTINCT
		ROW_NUMBER() OVER(ORDER BY a_temp.[idshash], a_temp.[analysis_pk],m_temp.[method_pk], iuamp.ParameterID) AS parameter_pk,
		m_temp.[method_pk],
		a_temp.[analysis_pk],
		a_temp.[idshash] AS [_id],
		 -- iuamp.ParameterID,
		iuamp.[Value] AS ParameterValue,
		p.DisplayText AS ParameterDisplay -- ,
		 -- p.Description AS ParameterDescription,
		 -- p.Notes AS ParameterNotes
		FROM [#temp_method_ids] m_temp
		INNER JOIN [#temp_analysis_ids] a_temp
		ON m_temp.analysis_pk = a_temp.analysis_pk
		INNER JOIN [IdentificationUnitAnalysisMethodParameter] iuamp
		ON (
			a_temp.CollectionSpecimenID = iuamp.CollectionSpecimenID 
			AND a_temp.IdentificationUnitID = iuamp.IdentificationUnitID
			AND a_temp.AnalysisID = iuamp.AnalysisID
			AND a_temp.AnalysisNumber = iuamp.AnalysisNumber COLLATE DATABASE_DEFAULT
			AND m_temp.MethodID = iuamp.MethodID
			AND m_temp.MethodMarker = iuamp.MethodMarker COLLATE DATABASE_DEFAULT
		)
		INNER JOIN [#temp_amp_filter] amp_filter
		ON (
			amp_filter.AnalysisID = iuamp.AnalysisID
			AND amp_filter.MethodID = iuamp.MethodID
			AND amp_filter.ParameterID = iuamp.ParameterID
		)
		INNER JOIN [Parameter] p
		ON (
			p.MethodID = iuamp.MethodID
			AND p.ParameterID = iuamp.ParameterID
		)
		;"""
		
		log_query.info(query)
		
		self.cur.execute(query)
		columns = [column[0] for column in self.cur.description]
		
		rows = self.cur.fetchall()
		
		self.parameters_dict = {}
		
		parameters = []
		for row in rows:
			parameters.append(dict(zip(columns, row)))
		
		for parameter in parameters:
			idshash = parameter['_id']
			parameter_pk = parameter['parameter_pk']
			method_pk = parameter['method_pk']
			analysis_pk = parameter['analysis_pk']
			
			self.parameters_dict[parameter_pk] = []
			
			# use only the name and the value of each parameter
			# it is easier to put them into self.iuanalyses_dict later when they are provided as list
			self.parameters_dict[parameter_pk] = [parameter['ParameterDisplay'], parameter['ParameterValue']]
			
			'''
			for key in parameter:
				if key not in ('_id', 'method_pk', 'analysis_pk', 'parameter_pk', 'AnalysisID', 'AnalysisNumber', 'MethodID', 'MethodMarker'):
					self.parameters_dict[parameter_pk][key] = parameter[key]
			'''
			
			self.keys_dict[idshash][analysis_pk][method_pk][parameter_pk] = {}
			
		return


	def set_iuanalyses_dict(self):
		
		self.iuanalyses_dict = {}
		
		for idshash in self.keys_dict:
			if idshash not in self.iuanalyses_dict:
				self.iuanalyses_dict[idshash] = {}
				self.iuanalyses_dict[idshash][self.fieldname] = []
			
			for analysis_pk in self.keys_dict[idshash]:
				analysis = self.analyses_dict[analysis_pk]
				analysis['Methods'] = []
				
				for method_pk in self.keys_dict[idshash][analysis_pk]:
					method = self.methods_dict[method_pk]
					
					# about 40 parameters per method in barcoding
					# put parameters directly into method dict, by using the ParameterDisplay as key in method dict
					# method['Parameters'] = {}
					
					for parameter_pk in self.keys_dict[idshash][analysis_pk][method_pk]:
						parameter = self.parameters_dict[parameter_pk]
						
						method[parameter[0]] = parameter[1]
					
					analysis['Methods'].append(method)
				
				self.iuanalyses_dict[idshash][self.fieldname].append(analysis)
		
		return


	def set_amp_filter_lists(self):
		self.amp_lists = []
		
		for analysis_id in self.amp_ids:
			if len(self.amp_ids[analysis_id]) <= 0:
				self.amp_lists.append((analysis_id, None, None))
			else:
				for method_id in self.amp_ids[analysis_id]:
					if len(self.amp_ids[analysis_id][method_id]) <= 0:
						self.amp_lists.append((analysis_id, method_id, None))
					else:
						for parameter_id in self.amp_ids[analysis_id][method_id]:
							self.amp_lists.append((analysis_id, method_id, parameter_id))
		return



	def get_data_page(self):
		
		# about 40 parameters per method in barcoding is too much, so it must be filtered by amp_ids
		self.set_amp_filter_lists()
		self.create_amp_filter_temptable()
		
		self.create_analyses_temptable()
		self.create_methods_temptable()
		
		self.set_analyses()
		self.set_methods()
		
		self.set_parameters()
		
		self.set_iuanalyses_dict()
		
		return self.iuanalyses_dict









