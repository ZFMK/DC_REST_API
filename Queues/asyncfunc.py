"""
http://blog.miguelgrinberg.com/post/the-flask-mega-tutorial-part-xi-email-support
"""
from threading import Thread

def asyncfunc(f):
	def wrapper(*args, **kwargs):
		thr = Thread(target=f, args=args, kwargs=kwargs)
		# important to have it as a daemon here, so the thread will be killed when the parent thread is killed by ctrl c
		# or when the tests.py ends
		thr.daemon = True
		thr.start()
	return wrapper
