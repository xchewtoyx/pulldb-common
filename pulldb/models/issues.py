# Copyright 2013 Russell Heilling
# pylint: disable=missing-docstring
from datetime import datetime, date
from dateutil.parser import parse as parse_date
import logging
import re

from google.appengine.api import search
from google.appengine.ext import ndb
from google.appengine.ext.ndb.model import BlobProperty
from google.appengine.ext.ndb.model import BooleanProperty
from google.appengine.ext.ndb.model import DateProperty
from google.appengine.ext.ndb.model import DateTimeProperty
from google.appengine.ext.ndb.model import IntegerProperty
from google.appengine.ext.ndb.model import JsonProperty
from google.appengine.ext.ndb.model import KeyProperty
from google.appengine.ext.ndb.model import StringProperty

from pulldb.models import base
from pulldb.models import arcs
from pulldb.models import volumes
from pulldb.models.properties import ImageProperty

class NoSuchIssue(base.PullDBModelException):
    pass


class Issue(ndb.Model):
    '''Issue object in datastore.

    Holds issue data.  Parent key should be a volume.
    '''
    # pylint: disable=too-many-instance-attributes
    # These are properties of the comicvine issue
    identifier = IntegerProperty()
    cover = BlobProperty()
    image = ImageProperty()
    issue_number = StringProperty()
    last_updated = DateTimeProperty(default=datetime.min)
    pubdate = DateProperty()
    title = StringProperty()
    site_detail_url = StringProperty()
    volume = KeyProperty(kind='Volume')
    # collection holds all volumes and arcs for the issue
    collection = KeyProperty(repeated=True)
    # These are local properties
    file_path = StringProperty()
    shard = IntegerProperty(default=-1)
    json = JsonProperty(indexed=False)
    name = StringProperty()
    changed = DateTimeProperty(auto_now=True)
    complete = BooleanProperty(default=False)
    indexed = BooleanProperty(default=False)

    @classmethod
    def projection(cls):
        return [
            'identifier', 'image', 'issue_number', 'last_updated',
            'pubdate', 'site_detail_url', 'title', 'volume',
            'indexed', 'name', 'shard',
        ]

    def apply_changes(self, issue_data):
        self.json = issue_data
        self.name = '%s %s' % (
            issue_data['volume']['name'],
            issue_data['issue_number'],
        )
        self.title = issue_data.get('name')
        self.issue_number = issue_data.get('issue_number', '')
        self.site_detail_url = issue_data.get('site_detail_url')
        volume_key = volumes.volume_key(issue_data['volume'], create=False)
        if volume_key not in self.collection:
            self.collection.append(volume_key)
        story_arcs = issue_data.get('story_arc_credits', [])
        for arc in story_arcs:
            arc_key = arcs.arc_key(arc, create=True)
            if arc_key not in self.collection:
                self.collection.append(arc_key)
        pubdate = None
        if issue_data.get('store_date'):
            pubdate = parse_date(issue_data['store_date'])
        elif issue_data.get('cover_date'):
            pubdate = parse_date(issue_data['cover_date'])
        if isinstance(pubdate, date):
            self.pubdate = pubdate
        try:
            self.image = issue_data['image']['small_url']
        except (KeyError, TypeError):
            self.image = None
        if issue_data.get('date_last_updated'):
            last_update = parse_date(issue_data['date_last_updated'])
        else:
            last_update = datetime.now
        self.last_updated = last_update
        self.indexed = False

    def extract_search_fields(self):
        document_fields = []
        contributors = self.json.get('person_credits')
        if contributors:
            for person in contributors:
                if re.match(r'^[A-Za-z][A-Za-z0-9_]*$', person['role']):
                    role = person['role']
                else:
                    role = 'person'
                document_fields.append(
                    search.TextField(
                        name=role, value=person['name']
                    )
                )
        description = self.json.get('description')
        if description:
            document_fields.append(
                search.HtmlField(name='description', value=description)
            )
        volume_name = self.json.get('volume', {}).get('name')
        if volume_name:
            document_fields.append(
                search.TextField(name='volume', value=volume_name)
            )
        if not self.name:
            document_fields.append(
                search.TextField(
                    name='name', value='%s %s' % (
                        self.json.get('volume', {}).get('name'),
                        self.issue_number,
                    )
                )
            )
        if not self.volume:
            volume_id = self.json.get('volume', {}).get('id')
            if volume_id:
                document_fields.append(
                    search.NumberField(name='volume_id', value=int(volume_id))
                )
        return document_fields

    def index_document(self, batch=False):
        document_fields = [
            search.TextField(name='title', value=self.title),
            search.TextField(name='issue_number', value=self.issue_number),
            search.NumberField(name='issue_id', value=self.identifier),
        ]

        if isinstance(self.pubdate, date):
            document_fields.append(
                search.DateField(name='pubdate', value=self.pubdate)
            )

        if self.json:
            document_fields.extend(self.extract_search_fields())

        if self.name:
            document_fields.append(
                search.TextField(name='name', value=self.name)
            )

        if self.volume:
            document_fields.append(
                search.NumberField(
                    name='volume_id', value=int(self.volume.id())
                )
            )

        issue_doc = search.Document(
            doc_id=self.key.id(),
            fields=document_fields,
        )
        if batch:
            return issue_doc
        try:
            index = search.Index(name="issues")
            index.put(issue_doc)
        except search.Error as error:
            logging.exception('Put failed: %r', error)

    def has_updates(self, new_data):
        issue_data = self.json or {}
        updates = False

        new_data_date = new_data.get('date_last_updated')
        if new_data_date:
            last_update = parse_date(new_data_date)
        else:
            last_update = datetime.now()

        if last_update > self.last_updated:
            logging.debug('Issue data newer than stored %r > %r',
                          last_update, self.last_updated)
            updates = True

        if check_collection_changes(self, new_data):
            updates = True

        return updates, last_update

def check_collection_changes(issue, issue_data):
    changed = False
    if issue.volume not in issue.collection:
        changed = True
    for story_arc in issue_data.get('story_arc_credits', []):
        arc_key = arcs.arc_key(story_arc, create=True)
        if arc_key not in issue.collection:
            changed = True
    return changed

# TODO(rgh): Temporary lookup of old style pull key during transition
def check_legacy(key, volume_key):
    issue = key.get()
    if not issue:
        legacy = ndb.Key(Issue, key.id(), parent=volume_key).get()
        if legacy:
            issue = Issue(
                key=key,
                identifier=key.id(),
                volume=legacy.volume,
                last_updated=datetime.min,
                name=legacy.name,
                shard=legacy.shard,
            )
            if legacy.pubdate:
                issue.pubdate = legacy.pubdate
            if legacy.json:
                issue.apply_changes(legacy.json)
            issue.put()

def issue_key(issue_data, volume_key=None, create=True, batch=False):
    # handle empty input gracefully
    if not issue_data:
        message = 'issue_key called with false input: %r' % issue_data
        logging.info(message)
        raise NoSuchIssue(message)

    if isinstance(issue_data, basestring):
        issue_id = issue_data
    else:
        issue_id = issue_data['id']
    key = ndb.Key(Issue, str(issue_id))

    if isinstance(issue_data, dict):
        updated = False
        issue = key.get()

        if issue:
            updated, last_update = issue.has_updates(issue_data)
        elif create:
            volume_key = ndb.Key('Volume', str(issue_data['volume']['id']))
            issue = Issue(
                key=key,
                identifier=issue_data['id'],
                last_updated=datetime.min,
                volume=volume_key,
            )
            updated = True
            last_update = datetime.min

        if updated:
            issue.apply_changes(issue_data)
            logging.info(
                'Saving issue updates for %s (last update at: %s)',
                key.id(), last_update)
            if batch:
                return issue.put_async()
            issue.put()

    return key

@ndb.tasklet
def issue_context(issue):
    volume = yield issue.key.parent().get_async()
    raise ndb.Return({
        'issue': issue,
        'volume': volume,
    })
