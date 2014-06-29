# Copyright 2013 Russell Heilling
from datetime import datetime

from google.appengine.ext import ndb

# pylint: disable=F0401

from pulldb.models import base
from pulldb.models import users
from pulldb.models import volumes

# pylint: disable=W0232,E1101,R0903,R0201,C0103,W0201

class Subscription(ndb.Model):
    '''Subscription object in datastore.

    Holds subscription data. Parent should be User.
    '''
    changed = ndb.DateTimeProperty(auto_now=True)
    identifier = ndb.IntegerProperty()
    shard = ndb.IntegerProperty(default=-1)
    start_date = ndb.DateProperty(default=datetime.min)
    stream = ndb.KeyProperty(kind='Stream')
    volume = ndb.KeyProperty(kind=volumes.Volume)
    volume_first_issue = ndb.KeyProperty(kind='Issue')
    volume_first_issue_date = ndb.DateTimeProperty()
    volume_last_issue = ndb.KeyProperty(kind='Issue')
    volume_last_issue_date = ndb.DateTimeProperty()

@ndb.tasklet
def refresh_subscription(subscription):
    volume = yield subscription.volume.get_async()
    changed = False
    if volume.first_issue:
        if subscription.volume_first_issue != volume.first_issue:
            subscription.volume_first_issue = volume.first_issue
            changed = True
        if subscription.volume_first_issue_date != volume.first_issue_date:
            subscription.volume_first_issue_date = volume.first_issue_date
            changed = True
    if volume.last_issue:
        if subscription.volume_last_issue != volume.last_issue:
            subscription.volume_last_issue = volume.last_issue
            changed = True
        if subscription.volume_last_issue_date != volume.last_issue_date:
            subscription.volume_last_issue_date = volume.last_issue_date
            changed = True

    if changed:
        yield subscription.put_async()

    raise ndb.Return(changed)

@ndb.tasklet
def subscription_context(subscription):
    volume = yield subscription.volume.get_async()
    publisher = yield volume.publisher.get_async()
    raise ndb.Return({
        'subscription': subscription,
        'volume': volume,
        'publisher': publisher,
    })

def subscription_key(volume_data, user=None, create=False, batch=False):
    if isinstance(volume_data, basestring):
        subscription_id = volume_data
        volume_key = volumes.volume_key(volume_data)
    if isinstance(volume_data, ndb.Key):
        subscription_id = volume_data.id()
        volume_key = volume_data
    if isinstance(volume_data, volumes.Volume):
        subscription_id = volume_data.key.id()
        volume_key = volume_data.key
    if not user:
        user = users.user_key()
    key = ndb.Key(
        Subscription, volume_key.id(),
        parent=user,
    )
    subscription = key.get()
    if not subscription and create:
        if not volume_key.get():
            message = 'Cannot add subscription for invalid volume: %r' % (
                volume_key.id()
            )
            raise volumes.NoSuchVolume(message)
        subscription = Subscription(
            key=key,
            volume=volume_key
        )
        if batch:
            return subscription
        subscription.put()

    return key
