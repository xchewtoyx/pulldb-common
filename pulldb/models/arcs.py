# Copyright 2013 Russell Heilling
# pylint: disable=missing-docstring
from datetime import datetime
import logging

from google.appengine.api import search #pylint: disable=import-error
from google.appengine.ext import ndb #pylint: disable=import-error

from dateutil.parser import parse as parse_date

from pulldb.models import base
from pulldb.models import comicvine
from pulldb.models import publishers
from pulldb.models.properties import ImageProperty


class NoSuchArc(base.PullDBModelException):
    pass


class StoryArc(ndb.Model):
    '''StoryArc object in datastore.

    Holds arc data.
    '''
    # pylint: disable=no-init,too-many-instance-attributes
    # Attributes from comicvine data
    first_issue = ndb.KeyProperty(kind='Issue')
    first_issue_date = ndb.DateProperty()
    identifier = ndb.IntegerProperty()
    image = ImageProperty()
    issue_count = ndb.IntegerProperty()
    last_updated = ndb.DateTimeProperty(default=datetime.min)
    name = ndb.StringProperty()
    publisher = ndb.KeyProperty(kind=publishers.Publisher)
    site_detail_url = ndb.StringProperty()
    # Attributes containting local data
    changed = ndb.DateTimeProperty(auto_now=True)
    complete = ndb.BooleanProperty(default=False)
    indexed = ndb.BooleanProperty(default=False)
    json = ndb.JsonProperty(indexed=False)
    shard = ndb.IntegerProperty(default=-1)

    @classmethod
    def projection(cls):
        return [
            'first_issue_date', 'identifier', 'image', 'issue_count',
            'last_updated', 'name', 'publisher',
            'site_detail_url', 'indexed', 'shard',
        ]

    def apply_changes(self, data):
        self.json = data
        self.name = data.get('name', self.name)
        self.site_detail_url = data.get('site_detail_url', self.site_detail_url)
        if data.get('image'):
            self.image = data['image'].get('small_url')
        if data.get('first_appeared_in_issue'):
            first_issue_key = ndb.Key(
                'Issue', str(data['first_appeared_in_issue']['id']))
            if self.first_issue != first_issue_key:
                self.first_issue = first_issue_key
                # Don't fetch the issue data here, queue for batch refresh
                self.complete = False

        last_updated = data.get('date_last_updated')
        if last_updated:
            last_updated = parse_date(last_updated)
        else:
            last_updated = datetime.min
        self.last_updated = last_updated
        self.indexed = False

    def has_updates(self, new_data):
        arc_data = self.json or {}
        updates = False

        new_data_date = new_data.get('date_last_updated')
        if new_data_date:
            last_update = parse_date(new_data_date)
        else:
            last_update = datetime.min

        if last_update > self.last_updated:
            updates = True

        if set(new_data.keys()) - set(arc_data.keys()):
            # keys differ between stored and fetched
            updates = True

        return updates, last_update

    def index_document(self, batch=False):
        document_fields = [
            search.TextField(name='name', value=self.name),
            search.NumberField(name='volume_id', value=self.identifier),
        ]
        if self.json:
            aliases = self.json.get('aliases')
            if aliases:
                for alias in aliases:
                    document_fields.append(
                        search.TextField(
                            name='alias', value=alias))
            deck = self.json.get('deck')
            if deck:
                document_fields.append(
                    search.TextField(name='deck', value=deck))
            description = self.json.get('description')
            if description:
                document_fields.append(
                    search.HtmlField(name='description', value=description))

        arc_doc = search.Document(
            doc_id=str(self.identifier),
            fields=document_fields
        )
        if batch:
            return arc_doc
        try:
            index = search.Index(name="arcs")
            index.put(arc_doc)
        except search.Error as error:
            logging.exception('Put failed: %r', error)


def identify_arc(arc_data):
    arc_id = None
    if isinstance(arc_data, int):
        arc_id = str(arc_data)
    if isinstance(arc_data, basestring):
        arc_id = arc_data
    if isinstance(arc_data, ndb.Key):
        arc_id = arc_data.id()
    if isinstance(arc_data, StoryArc):
        arc_id = arc_data.key.id()
    if isinstance(arc_data, dict):
        arc_id = arc_data['id']
    return arc_id

def arc_key(arc_data, create=True, reindex=False, batch=False):
    arc_id = identify_arc(arc_data)
    if not arc_id:
        message = 'Unable to identify arc for: %r' % arc_data
        logging.warn(message)
        raise NoSuchArc(message)

    key = ndb.Key(StoryArc, str(arc_id))
    arc = key.get()
    changed = False
    if create and not arc:
        if 'publisher' not in arc_data:
            api = comicvine.load()
            arc_data = api.fetch_story_arc(arc_data['id'])
        logging.info('Creating arc: %r', arc_data)
        publisher_key = publishers.publisher_key(arc_data['publisher'])
        arc = StoryArc(
            key=key,
            identifier=arc_data['id'],
            publisher=publisher_key,
            last_updated=datetime.min,
        )

    if arc and isinstance(arc_data, dict):
        if arc.has_updates(arc_data):
            # Arc is new or has been info has been updated since last put
            arc.apply_changes(arc_data)
            changed = True
        if reindex and arc.indexed:
            arc.indexed = False
            changed = True

    if changed:
        # Pylint doesn't know about model methods
        # pylint: disable=no-member
        logging.info('Saving arc updates: %r[%r]',
                     arc.identifier, arc.last_updated)
        if batch:
            return arc.put_async()
        arc.put()

    return key

@ndb.tasklet
def arc_context(arc):
    publisher = yield arc.publisher.get_async()
    raise ndb.Return({
        'arc': arc,
        'publisher': publisher,
    })
