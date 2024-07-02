import pudb

from dwb_authentication.security import SecurityPolicy


# TODO: this should be named ValidateLoginData and do not double the functions done by SecurityPolicy and DBSession
# To solve: access to db connection parameters

class UserLogin():
	def __init__(self, request):
		self.request = request
		
		self.uid = None
		self.roles = []
		self.users_projects = []
		self.users_project_ids = []
		
		self.token = None
		
		self.missing_params = []
		self.messages = []



	def handle_credentials(self, credentials):
		# check if there are any authentication data given in request
		# and if so: authenticate the user
		
		if 'logout' in credentials and credentials['logout'] == 'logout':
			self.log_out_user()
		
		if 'username' in credentials and 'password' in credentials:
			self.token = self.authenticate_user(credentials['username'], credentials['password'])
		
		elif 'token' in credentials and credentials['token']:
			self.token = credentials['token']
			self.authenticate_by_token()
		
		return


	def check_loginparams(self, username, password):
		self.missing_params = []
		
		loginparams = {
			'username': username,
			'password': password
		}
		
		for key in loginparams:
			if loginparams[key] is None or loginparams[key] == '':
				self.missing_params.append(key)
				self.messages.append('Login is not possible, please enter {0}'.format(key))
		
		if len(self.missing_params) > 0:
			loginparams = {}
		
		else:
			# check password against haveibeenpwned to check if it is a 'well known' password
			secure_password = self.password_not_pawned(password)
			if secure_password is False:
				loginparams = {}
				
		return loginparams


	'''
	check against https://haveibeenpwned.com disallow pawned passwords
	code from:
	https://pwcheck.gwdg.de/api.html
	with usage of haveibeenpwned.com API
	https://haveibeenpwned.com/API/v2#SearchingPwnedPasswordsByRange
	'''
	def password_not_pawned(self, password):
		
		import hashlib, urllib.request
		
		pwhash = hashlib.sha1(password.encode('utf8')).hexdigest().upper()
		prefix = pwhash[:5]
		suffix = pwhash[5:]
		
		unknown_password = False
		
		url = "https://api.pwnedpasswords.com/range/" + prefix
		with urllib.request.urlopen(url) as response:
			unknown_password = True
			for line in response.read().splitlines():
				line = line.decode('ASCII')
				if line.startswith(suffix):
					self.messages.append('Your password is insecure. Please change the password immediately')
					return False
		
		return unknown_password


	def authenticate_user(self, username, password):
		
		loginparams = self.check_loginparams(username, password)
		if len(loginparams) < 2:
			return None
		
		security = SecurityPolicy()
		self.token = security.validate_credentials(username = loginparams['username'], password = loginparams['password'])
		
		if self.token is not None:
			self.authenticate_by_token()
		
		else:
			self.messages.append('Login failed, please check your credentials')
		
		#headers = remember(self.request, login)
		return self.token


	def authenticate_by_token(self):
		self.request.session['token'] = self.token
		
		self.uid = self.request.authenticated_userid
		self.roles = self.request.identity['dwb_roles']
		self.users_projects = self.request.identity['projects']
		self.users_project_ids = [project[0] for project in self.users_projects]
		
		if self.uid is None:
			self.messages.append('Access token is not valid, please log in again')
			self.token = None
		
		return self.token


	def log_out_user(self):
		security = SecurityPolicy()
		security.de_authenticate(self.request)
		self.request.session['token'] = None
		
		self.uid = self.request.authenticated_userid
		self.roles = self.request.identity['dwb_roles']
		self.users_projects = self.request.identity['projects']
		self.users_project_ids = [project[0] for project in self.users_projects]
		
		return


	def get_messages(self):
		return self.messages


	def get_identity(self):
		return self.token, self.uid, self.roles, self.users_projects, self.users_project_ids
