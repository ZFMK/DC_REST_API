import pymysql
import warnings
import re

import pudb
import logging, logging.config
logging.config.fileConfig('logging.conf')
log_error = logging.getLogger('error')
log_query = logging.getLogger('sql_queries')


class MySQLConnector():
	def __init__(self, config = None, host = None, user = None, passwd = None, db = None, charset = None, port = None):
		'''
		Class to read and represent the database structure
		'''
		if config is None:
			self.config = {
			'host': 'localhost',
			'passwd': '',
			'charset': 'utf8mb4',
			'port': '3306',
			'user': None,
			'db': None
			}
		else: 
			self.config = config
		
		# check if there is something in the other parameters
		if host is not None:
			self.config['host'] = host
		if user is not None:
			self.config['user'] = user
		if passwd is not None:
			self.config['passwd'] = passwd
		if db is not None:
			self.config['db'] = db
		if charset is not None:
			self.config['charset'] = charset
		if port is not None:
			self.config['port'] = port
		
		
		if (self.config['db'] is None) or (self.config['user'] is None):
			raise Exception ('Not enough data base connection parameters given')
		self.con = None
		self.cur = None
		self.open_connection()
		self.MysqlError = pymysql.err

		self.databasename = self.config['db']

		

	def open_connection(self):
		self.con = self.__mysql_connect()
		self.cur = self.con.cursor()
		self.con.autocommit(True)


	def __mysql_connect(self):
		try:
			con = pymysql.connect(host=self.config['host'], user=self.config['user'], passwd=self.config['passwd'], db=self.config['db'], charset=self.config['charset'])
		except pymysql.Error as e:
			log_error.info("Error {0}: {1}".format(*e.args))
			print("Error {0}: {1}".format(*e.args))
			raise
		return con


	def closeConnection(self):
		if self.con:
			self.con.close()



	'''
	expose the connection and cursor
	'''
	
	def getCursor(self):
		return self.cur
	
	def getConnection(self):
		return self.con
	






