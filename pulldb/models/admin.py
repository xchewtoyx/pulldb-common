# Copyright 2013 Russell Heilling
from google.appengine.ext import ndb

class Setting(ndb.Model):
  '''Setting object in datastore.

  Holds settings data.
  '''
  name = ndb.StringProperty()
  value = ndb.StringProperty()
