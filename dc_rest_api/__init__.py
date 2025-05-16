from pyramid.config import Configurator
from pyramid.renderers import JSONP

from pyramid.authentication import SessionAuthenticationPolicy
from pyramid_beaker import session_factory_from_settings

from dwb_authentication.security import SecurityPolicy

# recreate the session db each time the server is started
# code might be kept in memory, how can i prevent that?
from dwb_authentication.setup_session_db.create_database import SessionDBSetup


def main(global_config, **settings):
	session_db = SessionDBSetup()
	del session_db
	
	config = Configurator(settings=settings)
	
	config.include('pyramid_beaker')
	session_factory = session_factory_from_settings(settings)
	config.set_security_policy(SecurityPolicy())
	
	config.include('pyramid_chameleon')
	config.add_renderer('jsonp', JSONP(param_name='callback'))
	
	config.add_route('login', '/login')
	config.add_route('logout', '/logout')
	
	#config.add_route('specimen', '/specimen/{id}')
	config.add_route('specimens', '/specimens')
	
	config.add_route('projects', '/projects')
	config.add_route('task_progress', '/task_progress/{task_id}')
	config.add_route('task_result', '/task_result/{task_id}')
	
	config.add_route('help', '/help')
	
	config.add_static_view(name='static', path='dc_rest_api:static')
	
	config.scan('dc_rest_api.views')
	return config.make_wsgi_app()
