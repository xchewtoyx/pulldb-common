# Copyright 2013 Russell Heilling
from datetime import datetime, date
from dateutil.parser import parse as parse_date
import logging

from google.appengine.api import search
from google.appengine.ext import ndb
import httplib

# pylint: disable=F0401
from pulldb.models import base
from pulldb.models import comicvine
from pulldb.models.properties import ImageProperty
from pulldb.models import volumes

# pylint: disable=W0232,C0103,E1101,R0201,R0903,R0902

class NoSuchIssue(base.PullDBModelException):
    pass

class Issue(ndb.Model):
    '''Issue object in datastore.

    Holds issue data.  Parent key should be a volume.
    '''
    # These are properties of the comicvine issue
    identifier = ndb.IntegerProperty()
    cover = ndb.BlobProperty()
    image = ImageProperty()
    issue_number = ndb.StringProperty()
    last_updated = ndb.DateTimeProperty(default=datetime.min)
    pubdate = ndb.DateProperty()
    title = ndb.StringProperty()
    site_detail_url = ndb.StringProperty()
    volume = ndb.KeyProperty(kind='Volume')
    # These are local properties
    file_path = ndb.StringProperty()
    shard = ndb.IntegerProperty(default=-1)
    json = ndb.JsonProperty(indexed=False)
    name = ndb.StringProperty()
    changed = ndb.DateTimeProperty(auto_now=True)
    indexed = ndb.BooleanProperty(default=False)

    def apply_changes(self, issue_data):
        self.json = issue_data
        self.name='%s %s' % (
            issue_data['volume']['name'],
            issue_data['issue_number'],
        )
        self.title = issue_data.get('name')
        self.issue_number = issue_data.get('issue_number', '')
        self.site_detail_url = issue_data.get('site_detail_url')
        if issue_data.get('store_date'):
            pubdate = parse_date(issue_data['store_date'])
        elif issue_data.get('cover_date'):
            pubdate = parse_date(issue_data['cover_date'])
        if isinstance(pubdate, date):
            self.pubdate=pubdate
        if 'image' in issue_data:
            self.image = issue_data['image'].get('small_url')
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
                document_fields.append(
                    search.TextField(
                        name=person['role'], value=person['name']
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
                documents_fields.append(
                    search.NumberField(name='volume_id', value=int(volume_id))
                )
        return document_fields

    def index_document(self, batch):
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
            document_fields.extend(self.extract_fields())

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

        new_data_date = new_data.get('date_last_updated', '')
        if new_data_date:
            last_update = parse_date(new_data_date)
        else:
            last_update = datetime.now()

        if new_data_date > self.last_updated:
            updates = True

        if set(new_data.keys()) - set(issue_data.keys()):
            # keys differ between stored and fetched
            updated = True

        return updates, last_update

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
                issue.pubdate=legacy.pubdate
            if legacy.json:
                issue.apply_changes(legacy.json)
            issue.put()

def issue_key(issue_data, create=True, batch=False):
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
        if create and not issue:
            if not volume_key:
                volume_key = volumes.volume_key(comicvine_issue['volume'])
            issue = Issue(
                key = key,
                identifier=comicvine_issue['id'],
                last_updated=datetime.min,
            )
        updated, last_update = issue.has_changes(comicvine_issue)
        if updated:
            issue.apply_changes(comicvine_issue)
            updated = True

        if updated:
            logging.info('Saving issue updates for %s', comicvine_issue['id'])
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
