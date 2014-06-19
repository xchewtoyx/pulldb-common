# Copyright 2013 Russell Heilling
import logging

from google.appengine.api import users
from google.appengine.ext import ndb

class User(ndb.Model):
    '''User object in datastore.

    Holds the email and ID of the users that have a pull-list.
    '''
    userid = ndb.StringProperty()
    image = ndb.StringProperty()
    nickname = ndb.StringProperty()
    oauth_token = ndb.StringProperty()
    trusted = ndb.BooleanProperty()

def user_key(app_user=None, create=True):
    if not app_user:
        app_user = users.get_current_user()
    logging.debug("Looking up user key for: %r", app_user)
    key = None
    user = User.query(User.userid == app_user.user_id()).get()
    if user:
        key = user.key
    elif create:
        logging.info('Adding user to datastore: %s', app_user.nickname())
        user = User(userid=app_user.user_id(),
                    nickname=app_user.nickname())
        user.put()
        key = user.key
    return user.key
