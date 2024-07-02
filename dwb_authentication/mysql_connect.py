import pudb
import pymysql

from configparser import ConfigParser
config = ConfigParser(allow_no_value=True)
config.read('./config.ini')


def mysql_connect():
	host = config.get('session_db', 'host')
	port = int(config.get('session_db', 'port'))
	user = config.get('session_db', 'username')
	passwd = config.get('session_db', 'password')
	db = config.get('session_db', 'database')
	
	conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
	
	# irgendwo in der dbsession scheint ein con.commit() zu fehlen, ich finde es nicht
	conn.autocommit(True)
	cur = conn.cursor()
	return (conn, cur)
