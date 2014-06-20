# Copyright 2013 Russell Heilling

from google.appengine.ext import ndb

class Pull(ndb.Model):
    '''Pulled Issue object in datastore.

    Holds pulled issue data.  Parent key should be a subscription.
    '''
    issue = ndb.KeyProperty(kind='Issue')
    pulled = ndb.BooleanProperty(default=False)
    read = ndb.BooleanProperty(default=False)
    pubdate = ndb.DateTimeProperty(default=datetime.min)
