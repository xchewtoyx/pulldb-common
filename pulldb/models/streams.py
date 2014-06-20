# Copyright 2013 Russell Heilling

from google.appengine.ext import ndb

class Stream(ndb.Model):
    '''Issue stream object in datastore.

    Holds issue stream data.  Parent key should be a subscription.
    '''
    publishers = ndb.KeyProperty(kind='Publisher', repeated=True)
    volumes = ndb.KeyProperty(kind='Volume', repeated=True)
    length = ndb.IntegerProperty()
