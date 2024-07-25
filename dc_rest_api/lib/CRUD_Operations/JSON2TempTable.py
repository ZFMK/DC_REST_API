import pudb

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.ReferencedJSON import ReferencedJSON


class JSON2TempTable():

	def __init__(self, dc_db, schema):
		
		self.collation = dc_db.collation
		
		self.con = dc_db.getConnection()
		self.cur = dc_db.getCursor()
		
		self.schema = schema


	def set_datadicts(self, json_dicts = []):
		'''
		the set_datadicts method is used by the child objects to compare the data with the data schemes given in each child object
		the data schemes should ensure, that default values are added for missing entries (isn't that defined in database?)
		and that only available columns are set in the data dicts
		'''
		
		self.datadicts = []
		
		for json_dict in json_dicts:
			#try:
			values_not_none = 0
			for entry in self.schema:
				if 'None allowed' in entry and entry['None allowed'] is False:
					if entry['colname'] not in json_dict or json_dict[entry['colname']] is None or json_dict[entry['colname']] == "":
						if 'default' in entry:
							json_dict[entry['colname']] = entry['default']
						else:
							raise ValueError('Can not insert data, field {0} is empty'.format(entry['colname']))
						if 'Minimal string length' in entry:
							if len(json_dict[entry['colname']]) < entry['Minimal string length']:
								raise ValueError('Can not insert data, value in field {0} must have a length of at least {1} letters'.format(entry['colname'], entry['Minimal string length']))
				elif entry['colname'] not in json_dict:
					if 'default' in entry:
						json_dict[entry['colname']] = entry['default']
					else:
						json_dict[entry['colname']] = None
				else:
					# just let the json_dict entry as it is
					pass
				
				if entry['colname'] != 'dataset_num' and json_dict[entry['colname']] is not None:
					values_not_none += 1
			
			# check that at least one value in json_dict entries is not None
			if values_not_none < 1:
				raise ValueError('Can not insert data, all fields are empty')
			
			self.datadicts.append(json_dict)
			#except:
			#	pass
		
		return


	def fill_temptable(self, temptable):
		valuelists = []
		placeholderstrings = []
		colnames = [entry['colname'] for entry in self.schema]
		
		for datadict in self.datadicts:
			valuelist = []
			placeholders = []
			for entry in self.schema:
				# convert all values not None to str because multirow insert fails when one datatype other than string causes the conversion to the data type of higher precedence:
				# https://learn.microsoft.com/en-us/sql/t-sql/queries/table-value-constructor-transact-sql?view=sql-server-ver15#data-types
				if 'compute sha of' in entry:
					if datadict[entry['compute sha of']] is not None:
						valuelist.append(str(datadict[entry['compute sha of']]))
					else:
						valuelist.append(datadict[entry['compute sha of']])
					placeholders.append("CONVERT(VARCHAR(64), HASHBYTES('sha2_256', ?), 2)")
				else:
					if datadict[entry['colname']] is not None:
						valuelist.append(str(datadict[entry['colname']]))
					else:
						valuelist.append(datadict[entry['colname']])
					placeholders.append('?')
			placeholderstrings.append('(' + ', '.join(placeholders) + ')')
			valuelists.append(valuelist)
		
		pagesize = 1
		if len(valuelists) > 0:
			pagesize = int(2000 / len(valuelists[0]))
			
			while len(valuelists) > 0:
				values = []
				
				cachedvaluelists = valuelists[:pagesize]
				del valuelists[:pagesize]
				
				cachedplaceholderstrings = placeholderstrings[:pagesize]
				del placeholderstrings[:pagesize]
				
				for valuelist in cachedvaluelists:
					values.extend(valuelist)
					
				placeholderstring = ', '.join(cachedplaceholderstrings)
				
				query = """
				INSERT INTO {0} (
					{1}
				)
				VALUES {2}
				;""".format(temptable, ',\n'.join(colnames), placeholderstring)
				
				querylog.info(query)
				querylog.info(values)
				
				self.cur.execute(query, values)
				self.con.commit()
		
		return





