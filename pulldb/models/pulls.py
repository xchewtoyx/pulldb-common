# Copyright 2013 Russell Heilling
from datetime import datetime
import logging

from google.appengine.ext import ndb

from pulldb.models import base
from pulldb.models import issues
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
    identifier = ndb.IntegerProperty()
    issue = ndb.KeyProperty(kind='Issue')
    pulled = ndb.BooleanProperty(default=False)
    pubdate = ndb.DateProperty(default=datetime.min)
    read = ndb.BooleanProperty(default=False)
    subscription = ndb.KeyProperty(kind='Subscription')
    volume = ndb.KeyProperty(kind='Volume')

# TODO(rgh): Temporary lookup of old style pull key during transition
def check_legacy(key, subscription_key):
    pull = key.get()
    if not pull:
        legacy_pull = ndb.Key(Pull, key.id(), parent=subscription_key).get()
        if legacy_pull:
            issue_key = issues.issue_key(pull.key.id())
            pull = Pull(
                key=key,
                identifier=key.id(),
                issue=issue_key.id(),
                subscription=subscription_key,
            )
            if legacy_pull.pubdate:
                pull.pubdate=legacy_pull.pubdate
            logging.info('Converted legacy pull: %r -> %r', legacy_pull.key,
                         pull.key)
            pull.put()

def pull_key(data, user_key=None, create=True):
    if not user_key:
        user_key = users.user_key()
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
    key = ndb.Key(Pull, pull_id, parent=user_key)

    if not isinstance(data, basestring) and create:
        issue_key = issues.issue_key(pull_id)
        issue = issue_key.get()
        if not issue:
            raise NoSuchIssue('Cannot add pull for bad issue: %r' % pull_id)
        subscription_key = subscriptions.subscription_key(
            issue.volume, user=user_key)

        pull = key.get()
        changed = False
        if not pull and create:
            pull = Pull(
                key=key,
                identifier=int(pull_id),
                issue=issue_key,
                pubdate=issue.pubdate,
                subscription=subscription_key,
                volume=issue.volume,
            )
            changed = True

        if pull.pubdate != issue.pubdate:
            pull.pubdate = issue.pubdate
            changed = True
        logging.info('Updating pull for issue %s', pull_id)
        if batch:
            return pull
        pull.put()

    return key
