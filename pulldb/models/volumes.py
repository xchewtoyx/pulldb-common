# Copyright 2013 Russell Heilling
from datetime import datetime
import logging

from google.appengine.api import search
from google.appengine.ext import ndb

from dateutil.parser import parse as parse_date

# pylint: disable=F0401
from pulldb.models import base
from pulldb.models import comicvine
from pulldb.models import publishers
from pulldb.models.properties import ImageProperty

# pylint: disable=W0232,C0103,E1101,R0201,R0903,R0902

class Volume(ndb.Model):
    '''Volume object in datastore.

    Holds volume data.
    '''
    # Attributes from comicvine data
    identifier = ndb.IntegerProperty()
    image = ImageProperty()
    issue_count = ndb.IntegerProperty()
    last_updated = ndb.DateTimeProperty(default=datetime.min)
    name = ndb.StringProperty()
    publisher = ndb.KeyProperty(kind=publishers.Publisher)
    site_detail_url = ndb.StringProperty()
    start_year = ndb.IntegerProperty()
    # Attributes containting local data
    changed = ndb.DateTimeProperty(auto_now=True)
    indexed = ndb.BooleanProperty(default=False)
    json = ndb.JsonProperty(indexed=False)
    shard = ndb.IntegerProperty(default=-1)

    def apply_changes(self, data):
        self.json=data
        self.name=data.get('name', self.name)
        self.issue_count=data.get('count_of_issues', self.issue_count)
        self.site_detail_url=data.get('site_detail_url', self.site_detail_url)
        self.start_year=int(data.get('start_year'), self.start_year)
        if data.get('image'):
            self.image = data['image'].get('small_url')
        last_updated = data.get('date_last_updated')
        if last_updated:
            last_updated = parse_date(new_data_date)
        else:
            last_updated = datetime.now()
        self.last_updated = last_updated
        self.indexed = False

    def has_updates(self, new_data):
        volume_data = volume.json or {}
        updates = False

        new_data_date = new_data_date.get('date_last_updated')
        if new_data_date:
            last_update = parse_date(new_data_date)
        else:
            last_update = datetime.now()

        if last_update > self.last_updated:
            updated = True

        if set(new_data.keys()) - set(volume_data.keys()):
            # keys differ between stored and fetched
            updated = True

        return updated, last_update

    def index_document(self, batch=False):
        document_fields = [
            search.TextField(name='name', value=self.name),
            search.NumberField(name='volume_id', value=self.identifier),
        ]
        if self.start_year:
            document_fields.append(
                search.NumberField(name='start_year', value=self.start_year))
        if self.json:
            contributors = self.json.get('people')
            if contributors:
                for person in contributors:
                    document_fields.append(
                        search.TextField(
                            name='person', value=person['name']
                        )
                    )
            description = self.json.get('description')
            if description:
                document_fields.append(
                    search.HtmlField(name='description', value=description)
                )

        volume_doc = search.Document(
            doc_id=self.identifier,
            fields=document_fields
        )
        if batch:
            return volume_doc
        try:
            index = search.Index(name="volumes")
            index.put(volume_doc)
        except search.Error as error:
            logging.exception('Put failed: %r', error)

def volume_key(volume_data, create=True, reindex=False, batch=False):
    if not volume_data:
        message = 'Unable to identify volume for: %r' % volume_data
        logging.warn(message)
        raise NoSuchVolume(message)

    if isinstance(volume_data, basestring):
        volume_id = volume_data
    if isinstance(volume_data, ndb.Key):
        volume_id = volume_data.id()
    if isinstance(volume_data, Volume):
        volume_id = volume_data.key.id()
    if isinstance(volume_data, dict):
        volume_id = volume['id']

    key = ndb.Key(Volume, str(volume_id))
    volume = key.get()
    if create and not volume:
        if 'publisher' not in comicvine_volume:
            if volume:
                return key
            else:
                cv = comicvine.load()
                comicvine_volume = cv.fetch_volume(comicvine_volume['id'])
        logging.info('Creating volume: %r', comicvine_volume)
        publisher = comicvine_volume['publisher']['id']
        publisher_key = publishers.publisher_key(comicvine_volume['publisher'])
        volume = Volume(
            key=key,
            identifier=comicvine_volume['id'],
            publisher=publisher_key,
            last_updated=datetime.min,
        )

    if volume.has_updates(comicvine_volume):
        logging.info('Volume has changes: %r', comicvine_volume)
        # Volume is new or has been info has been updated since last put
        volume.apply_changes(comicvine_volume)
        changed = True

    if changed:
        logging.info('Saving volume updates: %r', comicvine_volume)
        if batch:
            volume.put_async()
        else:
            volume.put()

    return key

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
