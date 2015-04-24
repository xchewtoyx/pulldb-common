#pylint: disable=missing-docstring
import logging
import os

from google.appengine.api import oauth
from google.appengine.api import users

# pylint: disable=F0401,E1002,E1101,C0103,W0201
import jinja2
import webapp2
from webapp2 import Route # pylint: disable=W0611

from pulldb.varz import VarzContext

class BaseHandler(webapp2.RequestHandler):
    def __init__(self, *args, **kwargs):
        super(BaseHandler, self).__init__(*args, **kwargs)
        self.templates = jinja2.Environment(
            loader=jinja2.FileSystemLoader(
                os.path.join(os.path.curdir, 'template')),
            extensions=['jinja2.ext.autoescape'])

    def get_user_info(self):
        user = users.get_current_user()
        if user:
            user_info = {
                'user_info_url': users.create_logout_url(self.request.path_url),
                'user_info_text': 'Logout',
                'user_info_name': user.nickname(),
                'user_is_admin': users.is_current_user_admin(),
            }
        else:
            user_info = {
                'user_info_url': users.create_login_url(self.request.uri),
                'user_info_text': 'Login',
                'user_info_name': None,
                'user_is_admin': False,
            }
        return user_info

    def base_template_values(self):
        template_values = {
            'url_path': self.request.path,
        }
        template_values.update(self.get_user_info())
        return template_values

    @VarzContext('handler')
    def dispatch(self):
        # pylint: disable=protected-access
        self.varz.handler_type = 'base'
        super(BaseHandler, self).dispatch()


class OauthHandler(BaseHandler):
    @VarzContext('handler')
    def dispatch(self):
        self.varz.handler_type = 'oauth'
        self.scope = 'https://www.googleapis.com/auth/userinfo.email'
        try:
            user = oauth.get_current_user(self.scope)
        except oauth.OAuthRequestError as error:
            logging.warn('Unable to determine user for request')
            logging.debug(error)
            self.abort(401)
        self.user = user
        logging.info('Request authorized by %r', user)
        super(OauthHandler, self).dispatch()


class TaskHandler(BaseHandler):
    @VarzContext('handler')
    def dispatch(self):
        self.varz.handler_type = 'task'
        self.scope = 'https://www.googleapis.com/auth/userinfo.email'
        try:
            user = oauth.get_current_user(self.scope)
        except oauth.OAuthRequestError as error:
            logging.warn('Unable to determine user for request')
            logging.debug(error)
            user = users.get_current_user()
            if not user and 'X-Appengine-Cron' in self.request.headers:
                user = users.User('russell+cron@heilling.net')
        if not user:
            self.abort(401)
        self.user = user
        logging.info('Request authorized by %r', user)
        super(TaskHandler, self).dispatch()


def create_app(handlers, debug=True, *args, **kwargs):
    return webapp2.WSGIApplication(handlers, debug=debug, *args, **kwargs)
