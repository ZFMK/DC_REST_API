import pudb
import json

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
		
		#self.create_tables()
		self.delete_old_tasks()


	def create_tables(self):
		# this method is only for the first time the ProgressTracker is called
		# should be put into an extra file?!
		
		# Delete was only used to change the table during development
		
		query = """
		DROP TABLE IF EXISTS task_progress
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE IF NOT EXISTS task_progress (
			task_id VARCHAR(36) NOT NULL,
			task_name VARCHAR(50),
			progress_in_percent FLOAT,
			`status` VARCHAR(50),
			`notification_url` VARCHAR(255) DEFAULT NULL,
			`date_submitted` DATETIME NOT NULL,
			`date_completed` DATETIME DEFAULT NULL,
			message VARCHAR(255),
			task_result JSON,
			step_result JSON,
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
		`status`,
		`message`,
		`notification_url`
		FROM task_progress 
		WHERE task_id = %s
		;"""
		self.cur.execute(query, [task_id])
		row = self.cur.fetchone()
		if row is not None:
			return {
				'progress': row[0],
				'status': row[1],
				'message': row[2],
				'notification_url': row[3]
			}
		return {}


	def get_progress_with_results(self, task_id):
		query = """
		SELECT progress_in_percent,
		`status`,
		`message`,
		task_result JSON,
		step_result JSON,
		`notification_url`
		FROM task_progress 
		WHERE task_id = %s
		;"""
		self.cur.execute(query, [task_id])
		row = self.cur.fetchone()
		if row is not None:
			return {
				'progress': row[0],
				'status': row[1],
				'message': row[2],
				'task_result': row[3],
				'step_result': row[4],
				'notification_url': row[5]
			}
		return {}


	def update_progress(self, task_id, progress_in_percent, status, step_result = None, task_result = None, message = None):
		task_result_string = None
		if task_result is not None:
			task_result_string = self.convert_to_json_string(task_result)
		
		step_result_string = None
		if step_result is not None:
			step_result_string = self.convert_to_json_string(step_result)
		
		query = """
		UPDATE task_progress tp
		SET tp.progress_in_percent = %s,
		tp.`status` = %s,
		tp.step_result = %s,
		tp.task_result = %s,
		tp.message = %s
		WHERE task_id = %s
		;"""
		self.cur.execute(query, [progress_in_percent, status, step_result_string, task_result_string, message, task_id])
		self.con.commit()
		return


	def insert_new_task(self, task_id, task_name, progress_in_percent, status, notification_url = None):
		query = """
		INSERT INTO task_progress (
			task_id, task_name, progress_in_percent, `status`, `notification_url`, date_submitted, date_completed, message, step_result, task_result
		)
		VALUES (
			%s, %s, %s, %s, %s, NOW(), NULL, NULL, NULL, NULL
		)
		;"""
		self.cur.execute(query, [task_id, task_name, progress_in_percent, status, notification_url])
		self.con.commit()
		return


	def set_task_result(self, task_id, task_result):
		task_result_string = None
		if task_result is not None:
			task_result_string = self.convert_to_json_string(task_result)
		
		query = """
		UPDATE task_progress
		SET 
			task_result = %s,
			date_completed = NOW(),
			progress_in_percent = 100,
			status = 'complete'
		WHERE Task_id = %s
		;"""
		self.cur.execute(query, [task_result_string, task_id])
		row = self.cur.fetchone()
		
		return


	def get_task_result(self, task_id):
		query = """
		SELECT task_result,
		`status`,
		`progress_in_percent`,
		`notification_url`,
		date_submitted,
		date_completed,
		DATE_ADD(date_submitted, INTERVAL +1 MONTH) AS available_until,
		message
		FROM task_progress
		WHERE Task_id = %s
		;"""
		self.cur.execute(query, [task_id])
		row = self.cur.fetchone()
		if row is not None:
			
			json_result = json.loads(row[0])
			json_result.update({
				"status": row[1],
				"progress": row[2],
				"notification_url": row[3],
				"date_submitted": row[4],
				"date_completed": row[5],
				"available_until": row[6],
				"message": row[7]
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
