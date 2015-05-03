# Copyright 2013 Russell Heilling
import logging

from google.appengine.ext import ndb

from pulldb.models import comicvine
from pulldb.models.properties import ImageProperty

class NoSuchPublisher(Exception):
    pass

class Publisher(ndb.Model):
    '''Publisher object in datastore.

    Holds publisher data.
    '''
    identifier = ndb.IntegerProperty()
    name = ndb.StringProperty()
    image = ImageProperty()
    json = ndb.JsonProperty(indexed=False)

    @classmethod
    def projection(cls):
        return [ 'identifier', 'name', 'image' ]

def publisher_key(publisher_data, create=True):
    if not publisher_data:
        message = 'Cannot lookup publisher for: %r' % publisher_data
        logging.warn(message)
        raise NoSuchPublisher(message)

    if isinstance(publisher_data, basestring):
        publisher_id = publisher_data
    if isinstance(publisher_data, int):
        publisher_id = publisher_data
    if isinstance(publisher_data, dict):
        publisher_id = publisher_data['id']

    key = ndb.Key(Publisher, str(publisher_id))

    if isinstance(publisher_data, dict):
        publisher = key.get()
        if not publisher and create:
            if 'image' not in publisher_data:
                cv = comicvine.Comicvine()
                publisher_data = cv.fetch_publisher(
                    publisher_id,
                    field_list='id,name,image',
                )
            publisher = Publisher(
                key=key,
                identifier=publisher_data['id'],
                name=publisher_data['name'],
                json=publisher_data,
            )
            if publisher_data.get('image'):
                publisher.image=publisher_data['image'].get('tiny_url')
            publisher.put()

    return key
