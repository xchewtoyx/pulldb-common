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

class NoSuchVolume(base.PullDBModelException):
    pass

class Volume(ndb.Model):
    '''Volume object in datastore.

    Holds volume data.
    '''
    # Attributes from comicvine data
    first_issue = ndb.KeyProperty(kind='Issue')
    first_issue_date = ndb.DateProperty()
    identifier = ndb.IntegerProperty()
    image = ImageProperty()
    issue_count = ndb.IntegerProperty()
    last_issue = ndb.KeyProperty(kind='Issue')
    last_issue_date = ndb.DateProperty()
    last_updated = ndb.DateTimeProperty(default=datetime.min)
    name = ndb.StringProperty()
    publisher = ndb.KeyProperty(kind=publishers.Publisher)
    site_detail_url = ndb.StringProperty()
    start_year = ndb.IntegerProperty()
    # Attributes containting local data
    changed = ndb.DateTimeProperty(auto_now=True)
    complete = ndb.BooleanProperty(default=False)
    indexed = ndb.BooleanProperty(default=False)
    json = ndb.JsonProperty(indexed=False)
    fast_shard = ndb.IntegerProperty(default=-1)
    shard = ndb.IntegerProperty(default=-1)

    @classmethod
    def projection(cls):
        return [
            'first_issue_date', 'identifier', 'image', 'issue_count',
            'last_issue_date', 'last_updated', 'name', 'publisher',
            'site_detail_url', 'start_year', 'indexed', 'shard',
        ]

    def apply_changes(self, data):
        # avoid overwriting data with a less complete version by merging
        # the new data over the existing data
        merged_data = self.json or {}
        merged_data.update(data)
        data = merged_data

        self.json=data
        self.name=data.get('name', self.name)
        self.issue_count=data.get('count_of_issues', self.issue_count)
        self.site_detail_url=data.get('site_detail_url', self.site_detail_url)
        if data.get('start_year'):
            try:
                self.start_year=int(data['start_year'])
            except ValueError as err:
                logging.error('error converting start_year: %r', err)
        if data.get('image'):
            self.image = data['image'].get('small_url')
        if data.get('first_issue'):
            first_issue_key = ndb.Key('Issue', str(data['first_issue']['id']))
            if self.first_issue != first_issue_key:
                self.first_issue = first_issue_key
                # Don't fetch the issue data here, queue for batch refresh
                self.complete = False
        if data.get('last_issue'):
            last_issue_key = ndb.Key('Issue', str(data['last_issue']['id']))
            if self.last_issue != last_issue_key:
                self.last_issue = last_issue_key
                # Don't fetch the issue data here, queue for batch refresh
                self.complete = False

        last_updated = data.get('date_last_updated')
        if last_updated:
            last_updated = parse_date(last_updated)
        else:
            last_updated = datetime.min
        if last_updated > self.last_updated:
            self.last_updated = last_updated
        self.indexed = False

    def has_updates(self, new_data):
        volume_data = self.json or {}
        updates = False

        new_data_date = new_data.get('date_last_updated')
        if new_data_date:
            last_update = parse_date(new_data_date)
        else:
            last_update = datetime.min

        if last_update > self.last_updated:
            updates = True

        return updates, last_update

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
            doc_id=str(self.identifier),
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
        volume_id = volume_data['id']

    key = ndb.Key(Volume, str(volume_id))
    volume = key.get()
    changed = False
    if create and not volume:
        if 'publisher' not in volume_data:
            if volume:
                return key
            else:
                cv = comicvine.load()
                volume_data = cv.fetch_volume(volume_data['id'])
        logging.info('Creating volume: %r', volume_data)
        if volume_data.get('publisher'):
            publisher_key = publishers.publisher_key(volume_data['publisher'])
        else:
            logging.warn('volume %d has no publisher', volume_data['id'])
            publisher_key = None
        volume = Volume(
            key=key,
            identifier=volume_data['id'],
            publisher=publisher_key,
            last_updated=datetime.min,
        )

    if volume and isinstance(volume_data, dict):
        volume_updated, last_update = volume.has_updates(volume_data)
        if volume_updated:
            # Volume is new or has been info has been updated since last put
            volume.apply_changes(volume_data)
            changed = True

    if changed:
        logging.info('Saving volume updates: %r[%r]',
                     volume.identifier, volume.last_updated)
        if batch:
            return volume.put_async()
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
