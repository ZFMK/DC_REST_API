import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')




class AnalysisMethodParameterFilter():
	"""
	IDs in Analysis, Methods, and Parameters that should be used in getData() method 
	Create Temptables with the according IDs
	"""
	def __init__(self, dc_db, fieldname):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.fieldname = fieldname
		
		self.amp_filter_ids = {}
		self.amp_lists = []
		
		if fieldname == 'Barcodes':
			self.setBarcodeAMPFilterIDS()
		elif fieldname == 'FOGS':
			self.setFOGSAMPFilterIDS()
		elif fieldname == 'MAM_Measurements':
			self.setMamAMPFilterIDS()
		
		self.amp_filter_temptable = '#temp_amp_filter'
		
		self.set_amp_filter_lists()
		self.create_amp_filter_temptable()


	def setBarcodeAMPFilterIDS(self):
		self.amp_filter_ids = {
		'161': {
				'12': ['62', '86', '87'],
				'16': ['73', '63', '64', '65', '66', '71', '72', '74', '75', '84', '91', '92']
			}
		}


	def setFOGSAMPFilterIDS(self):
		self.amp_filter_ids = {
		'327': {
				'23': ['140', '141', '150', '164', '165', '166', '167'],
				'25': ['144', '145', '146', '148']
			}
		}


	def setMamAMPFilterIDS(self):
		self.amp_filter_ids = {
			'210': {},
			'220': {},
			'230': {},
			'240': {},
			'250': {},
			'260': {},
			'270': {},
			'280': {},
			'290': {},
			'293': {},
			'294': {},
			'295': {},
			'296': {},
			'299': {},
			'301': {},
			'302': {},
			'303': {}
		}


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
		CREATE TABLE [{0}] (
			[AnalysisID] INT NOT NULL,
			[MethodID] INT,
			[ParameterID] INT,
			INDEX [idx_AnalysisID] ([AnalysisID]),
			INDEX [idx_MethodID] ([MethodID]),
			INDEX [idx_ParameterID] ([ParameterID])
		)
		;""".format(self.amp_filter_temptable)
		self.cur.execute(query)
		self.con.commit()
		
		placeholders = ['(?, ?, ?)' for _ in self.amp_lists]
		values = []
		for amp_list in self.amp_lists:
			values.extend(amp_list)
		
		query = """
		INSERT INTO [{0}] (
			[AnalysisID],
			[MethodID],
			[ParameterID]
		)
		VALUES {1}
		;""".format(self.amp_filter_temptable, ', '.join(placeholders))
		self.cur.execute(query, values)
		self.con.commit()
		
		return
