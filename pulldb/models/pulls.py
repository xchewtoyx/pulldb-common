# Copyright 2013 Russell Heilling
from datetime import datetime
import logging

from google.appengine.ext import ndb

from pulldb.models import base
from pulldb.models import issues
from pulldb.models import streams
from pulldb.models import subscriptions
from pulldb.models import users

class NoSuchPull(base.PullDBModelException):
    pass

class NoSuchIssue(base.PullDBModelException):
    pass

class Pull(ndb.Model):
    '''Pulled Issue object in datastore.

    Holds pulled issue data.  Parent key should be a subscription.
    '''
    collection = ndb.KeyProperty(repeated=True)
    identifier = ndb.IntegerProperty()
    ignored = ndb.BooleanProperty(default=False)
    issue = ndb.KeyProperty(kind='Issue')
    name = ndb.StringProperty()
    pubdate = ndb.DateProperty(default=datetime.min)
    publisher = ndb.KeyProperty(kind='Publisher')
    pulled = ndb.BooleanProperty(default=False)
    read = ndb.BooleanProperty(default=False)
    shard = ndb.IntegerProperty(default=-1)
    stream = ndb.KeyProperty(kind='Stream')
    subscription = ndb.KeyProperty(kind='Subscription')
    volume = ndb.KeyProperty(kind='Volume')
    weight = ndb.FloatProperty(default=1.0)

def pull_key(data, user=None, create=True, batch=False):
    if not user:
        user = users.user_key()
    if not data:
        message = 'Pull key cannot be found for: %r' % data
        logging.warn(message)
        raise NoSuchPull(message)
    issue = None
    if isinstance(data, basestring):
        pull_id = data
    if isinstance(data, ndb.Key) and data.kind() == 'Issue':
        pull_id = data.id()
    if isinstance(data, issues.Issue):
        pull_id = data.key.id()
        issue = data
    key = ndb.Key(Pull, pull_id, parent=user)

    if not isinstance(data, basestring) and create:
        issue_key = issues.issue_key(pull_id)
        issue = issue_key.get()
        if not issue:
            raise NoSuchIssue('Cannot add pull for bad issue: %r' % pull_id)
        subscription_key = subscriptions.subscription_key(
            issue.volume, user=user, create=False)

        pull = key.get()
        changed = False
        if not pull and create:
            pull = Pull(
                key=key,
                collection=issue.collection,
                identifier=int(pull_id),
                issue=issue_key,
                name=issue.name,
                pubdate=issue.pubdate,
                volume=issue.volume,
            )
            volume = issue.volume.get()
            if volume:
                pull.publisher = volume.publisher
            if subscription_key.get():
                pull.subscription = subscription_key
            changed = True

        if pull.pubdate != issue.pubdate:
            pull.pubdate = issue.pubdate
            changed = True
        logging.info('Updating pull for issue %s', pull_id)
        if batch:
            return pull
        pull.put()

    return key
