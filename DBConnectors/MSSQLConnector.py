import pyodbc
import warnings
import re

import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
log_error = logging.getLogger('error')
log_query = logging.getLogger('query')


class MSSQLConnector():
	def __init__(self, connectionstring = None, config = None, autocommit=False):
		if config is not None:
			server = config.get('server', None)
			dsn = config.get('dsn', None)
			database = config.get('database', None)
			port = config.get('port', None)
			driver = config.get('driver', '/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so')
			username = config.get('username', None)
			password = config.get('password', None)
			trust_certificate = config.get('trust_certificate', None)
			trusted_connection = config.get('trusted_connection', None)
			encrypt = config.get('encrypt', None)
			
			mssqlparams = MSSQLConnectionParams(
				dsn = dsn, 
				server = server, 
				port = port, 
				driver = driver, 
				database = database, 
				uid = username, 
				pwd = password,
				trust_certificate = trust_certificate,
				trusted_connection = trusted_connection,
				encrypt = encrypt
				)
			self.connectionstring = mssqlparams.getConnectionString()
			
			self.databasename = database
			self.collation = config.get('collation', 'Latin1_General_CI_AS')
			
		elif connectionstring is not None:
			self.connectionstring = connectionstring
			# read the database name from connectionstring and assign it to attribute self.databasename
			matchobj = re.search('Database\=([^;]+)', connectionstring, re.I)
			if matchobj is not None:
				self.databasename = matchobj.groups()[0]
				self.collation = config.get('collation', 'Latin1_General_CI_AS')
			else:
				raise Exception ('No database name given')
			
		else:
			raise Exception ('No database connection parameters given')
		
		self.con = None
		self.cur = None
		self.open_connection(autocommit)


	def open_connection(self, autocommit=False):
		self.con = self.__mssql_connect()
		self.cur = self.con.cursor()
		if autocommit:
			self.con.autocommit = True


	def __mssql_connect(self):
		try:
			con = pyodbc.connect(self.connectionstring)
		except pyodbc.Error as e:
			log_error.warn("Error {0}: {1}".format(*e.args))
			#print("Error {0}: {1}".format(*e.args))
			raise
		return con

	def closeConnection(self):
		self.con.close()

	'''
	expose the connection and cursor
	'''
	
	def getCursor(self):
		return self.cur
	
	def getConnection(self):
		return self.con


class MSSQLConnectionParams():
	def __init__(self, 
					dsn = None,
					server = None,
					port = None,
					driver = '/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so',
					database = None,
					uid = None,
					pwd = None, 
					trust_certificate = None,
					trusted_connection = None,
					encrypt = None):
		self.paramsdict = {}
		
		self.setDSN(dsn)
		self.setServer(server)
		self.setPort(port)
		self.setDatabase(database)
		self.setUID(uid)
		self.setPWD(pwd)
		self.setDriver(driver)
		self.setTrustCertificate(trust_certificate)
		self.setTrustedConnection(trusted_connection)
		self.setEncrypt(encrypt)
		
	def setDSN(self, dsn = None):
		self.paramsdict['DSN'] = dsn
		if dsn is not None:
			self.paramsdict['server'] = None
	
	def setServer(self, server = None):
		self.paramsdict['Server'] = server
		if server is not None:
			self.paramsdict['dsn'] = None
	
	def setDriver(self, driver):
		self.paramsdict['Driver'] = driver
	
	def setPort(self, port = None):
		self.paramsdict['Port'] = port
	
	def setDatabase(self, database = None):
		self.paramsdict['Database'] = database
	
	def setUID(self, uid = None):
		self.paramsdict['UID'] = uid
	
	def setPWD(self, pwd = None):
		self.paramsdict['PWD'] = pwd
	
	def setTrustCertificate(self, trust_certificate):
		self.paramsdict['TrustServerCertificate'] = trust_certificate
		
	def setTrustedConnection(self, trusted_connection):
		self.paramsdict['Trusted_Connection'] = trusted_connection
	
	def setEncrypt(self, encrypt):
		self.paramsdict['Encrypt'] = encrypt
	
	
	def getDSN(self):
		return self.paramsdict['DSN']
	
	def getServer(self):
		return self.paramsdict['Server']
	
	def getDriver(self):
		return self.paramsdict['Driver']
	
	def getPort(self):
		return self.paramsdict['Port']
	
	def getDatabase(self):
		return self.paramsdict['Database']
	
	def getUID(self):
		return self.paramsdict['UID']
	
	def getTrustCertificate(self):
		return self.paramsdict['TrustServerCertificate']
		
	def getTrustedConnection(self):
		return self.paramsdict['Trusted_Connection']
	
	def getEncrypt(self):
		return self.paramsdict['Encrypt']
	
	'''
	def getPWD(self):
		return self.paramsdict['PWD']
	'''
	
	def getConnectionString(self):
		connectionstring = ''
		paramslist = []
		for key in self.paramsdict:
			if self.paramsdict[key] is not None:
				paramslist.append("{0}={1}".format(key, self.paramsdict[key]))
		connectionstring = ';'.join(paramslist)
		return connectionstring
	






