# Copyright 2013 Russell Heilling
from datetime import datetime
import logging

from google.appengine.api import search
from google.appengine.ext import ndb

from dateutil.parser import parse as parse_date

# pylint: disable=F0401
from pulldb.models import publishers
from pulldb.models import comicvine
from pulldb.models.properties import ImageProperty

# pylint: disable=W0232,C0103,E1101,R0201,R0903,R0902

class Volume(ndb.Model):
    '''Volume object in datastore.

    Holds volume data.
    '''
    identifier = ndb.IntegerProperty()
    image = ImageProperty()
    issue_count = ndb.IntegerProperty()
    last_updated = ndb.DateTimeProperty(default=datetime.min)
    name = ndb.StringProperty()
    publisher = ndb.KeyProperty(kind=publishers.Publisher)
    site_detail_url = ndb.StringProperty()
    start_year = ndb.IntegerProperty()
    shard = ndb.IntegerProperty(default=-1)
    json = ndb.JsonProperty(indexed=False)
    changed = ndb.DateTimeProperty(auto_now=True)
    indexed = ndb.BooleanProperty(default=False)

def volume_updated(volume, comicvine_volume):
    updated = False
    volume_dict = volume.json or {}

    cv_update = comicvine_volume.get('date_last_updated')
    if cv_update:
        last_update = parse_date(cv_update)
    else:
        last_update = datetime.now()

    if last_update > volume.last_updated:
        updated = True

    if set(comicvine_volume.keys()) - set(volume_dict.keys()):
        # keys differ between stored and fetched
        updated = True

    return updated, last_update

def volume_key(comicvine_volume, create=True, reindex=False, batch=False):
    if not comicvine_volume:
        return
    changed = False
    key = ndb.Key(Volume, str(comicvine_volume['id']))
    volume = key.get()
    if 'publisher' not in comicvine_volume:
        if volume:
            return key
        else:
            cv = comicvine.load()
            comicvine_volume = cv.fetch_volume(comicvine_volume['id'])
    if create and not volume:
        logging.info('Creating volume: %r', comicvine_volume)
        publisher = comicvine_volume['publisher']['id']
        publisher_key = publishers.publisher_key(comicvine_volume['publisher'])
        volume = Volume(
            key=key,
            identifier=comicvine_volume['id'],
            publisher=publisher_key,
            last_updated=datetime.min,
        )
    updated, last_updated = volume_updated(volume, comicvine_volume)
    if updated:
        logging.info('Volume has changes: %r', comicvine_volume)
        # Volume is new or has been info has been updated since last put
        volume.json=comicvine_volume
        volume.name=comicvine_volume.get('name')
        volume.issue_count=comicvine_volume.get('count_of_issues')
        volume.site_detail_url=comicvine_volume.get('site_detail_url')
        volume.start_year=int(comicvine_volume.get('start_year'))
        if comicvine_volume.get('image'):
            volume.image = comicvine_volume['image'].get('small_url')
        volume.last_updated = last_updated
        volume.indexed=False
        changed = True

    if changed:
        logging.info('Saving volume updates: %r', comicvine_volume)
        if batch:
            volume.put_async()
        else:
            volume.put()

    return key

def index_volume(key, volume, batch=False):
    document_fields = [
        search.TextField(name='name', value=volume.name),
        search.NumberField(name='volume_id', value=volume.identifier),
    ]
    if volume.start_year:
        document_fields.append(
            search.NumberField(name='start_year', value=volume.start_year))
    if volume.json:
        contributors = volume.json.get('people')
        if contributors:
            for person in contributors:
                document_fields.append(
                    search.TextField(
                        name='person', value=person['name']
                    )
                )
        description = volume.json.get('description')
        if description:
            document_fields.append(
                search.HtmlField(name='description', value=description)
            )

    volume_doc = search.Document(
        doc_id = key.urlsafe(),
        fields = document_fields)
    if batch:
        return volume_doc
    try:
        index = search.Index(name="volumes")
        index.put(volume_doc)
    except search.Error as error:
        logging.exception('Put failed: %r', error)

@ndb.tasklet
def volume_context(volume):
    publisher = yield volume.publisher.get_async()
    raise ndb.Return({
        'volume': volume,
        'publisher': publisher,
    })

@ndb.tasklet
def refresh_volume_shard(shard, shard_count, subscription, cv):
    volume = yield subscription.volume.get_async()
    if volume.identifier % shard_count == shard:
        raise ndb.Return(volume.identifier)
