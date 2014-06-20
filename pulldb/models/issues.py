# Copyright 2013 Russell Heilling
from datetime import datetime, date
from dateutil.parser import parse as parse_date
import logging

from google.appengine.api import search
from google.appengine.ext import ndb
import httplib

# pylint: disable=F0401
from pulldb.models import comicvine
from pulldb.models.properties import ImageProperty
from pulldb.models import volumes

# pylint: disable=W0232,C0103,E1101,R0201,R0903,R0902

class Issue(ndb.Model):
    '''Issue object in datastore.

    Holds issue data.  Parent key should be a volume.
    '''
    # These are properties of the comicvine issue
    identifier = ndb.IntegerProperty()
    pubdate = ndb.DateProperty()
    cover = ndb.BlobProperty()
    image = ImageProperty()
    issue_number = ndb.StringProperty()
    last_updated = ndb.DateTimeProperty(default=datetime.min)
    title = ndb.StringProperty()
    site_detail_url = ndb.StringProperty()
    # These are local properties
    file_path = ndb.StringProperty()
    shard = ndb.IntegerProperty(default=-1)
    json = ndb.JsonProperty(indexed=False)
    name = ndb.TextProperty()
    changed = ndb.DateTimeProperty(auto_now=True)
    indexed = ndb.BooleanProperty(default=False)

@ndb.tasklet
def refresh_issue_shard(shard, shard_count, subscription, cv=None):
  volume = yield subscription.volume.get_async()
  if volume.identifier % shard_count == shard:
    volume_issues = []
    for issue in cv.fetch_issue_batch(volume.identifier, filter_attr='volume'):
        volume_issues.append(
            issue_key(
                issue, volume_key=volume.key, create=True, reindex=True
            )
        )
    raise ndb.Return(volume_issues)

@ndb.tasklet
def refresh_issue_volume(volume, cv=None):
  try:
    comicvine_volume = cv.fetch_volume(volume.identifier)
  except httplib.HTTPException as e:
    logging.exception(e)
    return
  comicvine_issues = comicvine_volume['issues']
  issues = []
  logging.debug('Found %d issues: %r',
                len(comicvine_issues), comicvine_issues)
  for index in range(0, len(comicvine_issues), 100):
    logging.debug('processing issues %d to %d of %d',
                  index, index+99, len(comicvine_issues))
    issue_ids = []
    for issue in comicvine_issues[
        index:min([len(comicvine_issues), index+100])]:
      issue_ids.append(issue['id'])
    try:
      issue_page = cv.fetch_issue_batch(issue_ids)
    except httplib.HTTPException as e:
      logging.exception(e)
      return
    for issue in issue_page:
      issues.append(issue_key(
        issue, volume_key=volume.key, create=True, reindex=True))

  raise ndb.Return(issues)

def issue_updated(issue, comicvine_issue):
    updated = False
    issue_dict = issue.json or {}

    cv_update = comicvine_issue.get('date_last_updated', '')
    if cv_update:
        last_update = parse_date(comicvine_issue['date_last_updated'])
    else:
        last_update = datetime.now()

    if last_update > issue.last_updated:
        updated = True

    if set(comicvine_issue.keys()) - set(issue_dict.keys()):
        # keys differ between stored and fetched
        updated = True

    return updated, last_update

def issue_key(comicvine_issue, volume_key=None, create=True,
              reindex=False, batch=False):
    if not comicvine:
        return
    changed = False
    volume_id = comicvine_issue['volume']['id']
    issue_id = comicvine_issue['id']
    key = ndb.Key(volumes.Volume, str(volume_id), Issue, str(issue_id))
    issue = key.get()

    if create and not issue:
        if not volume_key:
            volume_key = volumes.volume_key(comicvine_issue['volume'])
        issue = Issue(
            key = key,
            identifier=comicvine_issue['id'],
            last_updated=datetime.min,
        )
    updated, last_update = issue_updated(issue, comicvine_issue)
    if updated:
        issue.json = comicvine_issue
        issue.name='%s %s' % (
            comicvine_issue['volume']['name'],
            comicvine_issue['issue_number'],
        )
        issue.title = comicvine_issue.get('name')
        issue.issue_number = comicvine_issue.get('issue_number', '')
        issue.site_detail_url = comicvine_issue.get('site_detail_url')
        if comicvine_issue.get('store_date'):
            pubdate = parse_date(comicvine_issue.get('store_date'))
        elif comicvine_issue.get('cover_date'):
            pubdate = parse_date(comicvine_issue.get('cover_date'))
        if isinstance(pubdate, date):
            issue.pubdate=pubdate
        if 'image' in comicvine_issue:
            issue.image = comicvine_issue['image'].get('small_url')
        issue.last_updated = last_update
        issue.indexed = False
        changed = True

    if changed:
        logging.info('Saving issue updates: %r', comicvine_issue)
        if batch:
            issue.put_async()
        else:
            issue.put()

    return key

def index_issue(key, issue, batch=False):
    document_fields = [
        search.TextField(name='title', value=issue.title),
        search.TextField(name='issue_number', value=issue.issue_number),
        search.NumberField(name='issue_id', value=issue.identifier),
    ]
    if isinstance(issue.pubdate, date):
        document_fields.append(
            search.DateField(name='pubdate', value=issue.pubdate)
        )
    if issue.json:
        contributors = issue.json.get('person_credits')
        if contributors:
            for person in contributors:
                document_fields.append(
                    search.TextField(
                        name=person['role'], value=person['name']
                    )
                )
        description = issue.json.get('description')
        if description:
            document_fields.append(
                search.HtmlField(name='description', value=description)
            )
        if not issue.name:
            search.TextField(name='name', value='%s %s' % (
                issue.json.get('volume', {}).get('name'),
                issue.issue_number
            ))
    if issue.name:
        search.TextField(name='name', value=issue.name)

    issue_doc = search.Document(
        doc_id = key.urlsafe(),
        fields = document_fields
    )
    if batch:
        return issue_doc
    try:
        index = search.Index(name="issues")
        index.put(issue_doc)
    except search.Error as error:
        logging.exception('Put failed: %r', error)

@ndb.tasklet
def issue_context(issue):
  volume = yield issue.key.parent().get_async()
  raise ndb.Return({
    'issue': issue,
    'volume': volume,
  })
