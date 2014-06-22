# Copyright 2013 Russell Heilling

from google.appengine.ext import ndb

from pulldb.models import users

class Stream(ndb.Model):
    '''Issue stream object in datastore.

    Holds issue stream data.  Parent key should be a subscription.
    '''
    name = ndb.StringProperty()
    issues = ndb.KeyProperty(kind='Issue', repeated=True)
    length = ndb.IntegerProperty()
    publishers = ndb.KeyProperty(kind='Publisher', repeated=True)
    user = ndb.KeyProperty(kind='User')
    volumes = ndb.KeyProperty(kind='Volume', repeated=True)

def stream_key(stream_data, user_key=None, create=False, batch=False):
    if not user_key:
        user_key = users.user_key()
    if isinstance(stream_data, dict):
        stream_id = stream_data['name']
    if isinstance(stream_data, basestring):
        stream_id = stream_data
    stream_key = ndb.Key(Stream, stream_id, parent=user_key)

    if create:
        stream = stream_key.get()
        if not stream:
            stream = Stream(key=stream_key)
            stream.populate(**stream_data)
        if batch:
            return stream
        stream.put()

    return stream_key
