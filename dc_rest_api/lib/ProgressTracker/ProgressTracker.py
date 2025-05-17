import pudb

import logging
import logging.config

from DBConnectors.MySQLConnector import MySQLConnector

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('dc_api')
logger_query = logging.getLogger('query')

from configparser import ConfigParser
config = ConfigParser(allow_no_value=True)
config.read('./config.ini')


class ProgressTracker:
	def __init__(self):
		
		host = config.get('session_db', 'host')
		port = int(config.get('session_db', 'port'))
		user = config.get('session_db', 'username')
		passwd = config.get('session_db', 'password')
		db = config.get('session_db', 'database')
		charset = config.get('session_db', 'charset')
		
		mysql_db = MySQLConnector(host = host, user = user, passwd = passwd, db = db, port = port, charset = charset)
		self.cur = mysql_db.getCursor()
		self.con = mysql_db.getConnection()
		
		self.create_tables()
		self.delete_old_tasks()


	def create_tables(self):
		
		query = """
		CREATE TABLE IF NOT EXISTS task_progress (
			task_id VARCHAR(36) NOT NULL,
			task_name VARCHAR(50),
			progress_in_percent FLOAT,
			`status` VARCHAR(10),
			`date_submitted` DATETIME NOT NULL,
			`date_completed` DATETIME DEFAULT NULL,
			message VARCHAR(255),
			task_result JSON,
			PRIMARY KEY (task_id),
			KEY (task_name),
			KEY (`status`)
		)
		;"""
		self.cur.execute(query)
		self.con.commit()
		return


	def delete_old_tasks(self):
		# delete all tasks that are older than one month
		
		query = """
		DELETE FROM task_progress
		WHERE date_submitted < DATE_ADD(NOW(), INTERVAL -1 MONTH) 
		;"""
		self.cur.execute(query)
		self.con.commit()
		return


	def get_progress(self, task_id):
		query = """
		SELECT progress_in_percent,
		`status`
		FROM Task_progress 
		WHERE task_id = %s
		;"""
		self.cur.execute(query, [task_id])
		row = self.cur.fetchone()
		if row is not None:
			return row[0], row[1]
		return


	def update_progress(self, task_id, progress_in_percent, status, task_result = None, message = None):
		if task_result is not None:
			task_result_string = self.convert_to_json_string(task_result)
		
		query = """
		UPDATE task_progress tp
		SET tp.progress_in_percent = %s,
		tp.`status` = %s,
		tp.task_result = %s,
		tp.message = %s
		WHERE task_id = %s
		;"""
		self.cur.execute(query, [progress_in_percent, status, task_id, task_result_string, message])
		self.con.commit()
		return


	def insert_new_task(self, task_id, task_name, progress_in_percent, status):
		query = """
		INSERT INTO task_progress (
			task_id, task_name, progress_in_percent, `status`, date_submitted, date_completed, message, task_result
		)
		VALUES (
			%s, %s, %s, %s, NOW(), NULL, NULL, NULL 
		)
		;"""
		self.cur.execute(query, [task_id, task_name, progress_in_percent, status])
		self.con.commit()
		return


	def set_task_result(self, task_result):
		if task_result is not None:
			task_result_string = self.convert_to_json_string(task_result)
		
		query = """
		UPDATE task_progress
		SET 
			task_result = %s,,
			date_completed = NOW(),
			progress_in_percent = 100,
			status = 'complete'
		WHERE Task_id = %s
		;"""
		self.cur.execute(query, [task_result_string, task_id])
		row = self.cur.fetchone()
		
		return


	def get_task_result(self):
		query = """
		SELECT task_result,
		`status`,
		date_submitted,
		date_completed,
		DATE_ADD(date_submitted, INTERVAL +1 MONTH) AS available_until,
		message,
		FROM task_progress
		WHERE Task_id = %s
		;"""
		self.cur.execute(query, [task_id])
		row = self.cur.fetchone()
		if row is not None:
			
			json_result = json.loads(row[0])
			json_result.update({
				"status": row[1],
				"date_submitted": row[2],
				"date_completed": row[3],
				"available_until": row[4],
				"message": row[5]
			})
		else:
			json_result = {}
		return json_result


	def convert_to_json_string(self, task_result):
		try:
			task_result_string = json.dumps(task_result)
		except Exception as e:
			self.messages.append(e)
			task_result_string = '{}'
		return task_result_string
