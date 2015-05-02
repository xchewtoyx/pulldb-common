# Copyright 2013 Russell Heilling
# pylint: disable=missing-docstring
from datetime import datetime
import logging

from google.appengine.ext import ndb # pylint: disable=import-error

from pulldb.models import arcs
from pulldb.models import users
from pulldb.models import volumes

class NoSuchCollection(Exception):
    pass


class WatchList(ndb.Model):
    '''WatchList object in datastore.

    '''
    # pylint: disable=no-init,too-few-public-methods
    changed = ndb.DateTimeProperty(auto_now=True)
    collection = ndb.KeyProperty()
    fresh = ndb.BooleanProperty(default=False)
    shard = ndb.IntegerProperty(default=-1)
    start_date = ndb.DateProperty(default=datetime.min)
    user = ndb.KeyProperty(kind='User')


class Subscription(ndb.Model):
    '''Subscription object in datastore.

    Holds subscription data. Parent should be User.
    '''
    # pylint: disable=no-init,too-few-public-methods
    changed = ndb.DateTimeProperty(auto_now=True)
    identifier = ndb.IntegerProperty()
    fresh = ndb.BooleanProperty(default=False)
    shard = ndb.IntegerProperty(default=-1)
    start_date = ndb.DateProperty(default=datetime.min)
    volume = ndb.KeyProperty(kind=volumes.Volume)
    # TODO(rgh): These fields are deprecated
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

def arc_watch_key(arc_data, **kwargs):
    if isinstance(arc_data, (int, basestring, dict)):
        arc_key = arcs.arc_key(arc_data)
    if isinstance(arc_data, ndb.Key):
        arc_key = arc_data
    if isinstance(arc_data, arcs.StoryArc):
        arc_key = arc_data.key
    return watch_key(arc_key, **kwargs)

def volume_watch_key(volume_data, **kwargs):
    if isinstance(volume_data, (int, basestring, dict)):
        volume_key = volumes.volume_key(volume_data)
    if isinstance(volume_data, ndb.Key):
        volume_key = volume_data
    if isinstance(volume_data, volumes.Volume):
        volume_key = volume_data.key
    return watch_key(volume_key, **kwargs)

@ndb.tasklet
def watch_key(collection_data, user=None, create=False, batch=False):
    if isinstance(collection_data, ndb.Key):
        collection_key = collection_data
    if not user:
        user = users.user_key()
    watch_query = WatchList.query( # pylint: disable=no-member
        WatchList.user == user,
        WatchList.collection == collection_key)
    watches = yield watch_query.fetch_async()
    if len(watches) > 1:
        logging.error(
            'Too many watches for %r [%r]', collection_data, watches)
    if watches:
        watch = watches[0]
    if not watch and create:
        # Pylint doesn't know about model methods
        # pylint: disable=no-member
        collection = yield collection_key.get_async()
        if not collection:
            message = 'Cannot add subscription for invalid collection: %r' % (
                collection_key,)
            logging.error(message)
            raise NoSuchCollection(message)
        watch = WatchList(
            user=user,
            collection=collection_key)
        if batch:
            raise ndb.Return(watch.put_async())
        yield watch.put_async()

    raise ndb.Return(watch.key) # pylint: disable=no-member

def subscription_key(volume_data, user=None, create=False, batch=False):
    if isinstance(volume_data, int):
        subscription_id = str(volume_data)
        volume_key = volumes.volume_key(subscription_id)
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
        # Pylint doesn't know about model methods
        # pylint: disable=no-member
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
