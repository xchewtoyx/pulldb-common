# Copyright 2013 Russell Heilling

from google.appengine.ext import ndb

from pulldb.models import comicvine
from pulldb.models.properties import ImageProperty

class Publisher(ndb.Model):
  '''Publisher object in datastore.

  Holds publisher data.
  '''
  identifier = ndb.IntegerProperty()
  name = ndb.StringProperty()
  image = ImageProperty()

def publisher_key(comicvine_publisher, create=True):
    if not comicvine_publisher:
        return
    key = ndb.Key(Publisher, str(comicvine_publisher['id']))
    publisher = key.get()
    if not publisher and create:
        if 'image' not in comicvine_publisher:
            cv = comicvine.Comicvine()
            comicvine_publisher = cv.fetch_publisher(
                comicvine_publisher['id'],
                field_list='id,name,image',
            )
        publisher = Publisher(
            key=key,
            identifier=comicvine_publisher['id'],
            name=comicvine_publisher['name']
        )
        if comicvine_publisher.get('image'):
            publisher.image=comicvine_publisher['image'].get('tiny_url')
        publisher.put()

    return key
