# Copyright 2013 Russell Heilling
from datetime import datetime

from google.appengine.ext import ndb

# pylint: disable=F0401

from pulldb.models import users
from pulldb.models import volumes

# pylint: disable=W0232,E1101,R0903,R0201,C0103,W0201

class Subscription(ndb.Model):
    '''Subscription object in datastore.

    Holds subscription data. Parent should be User.
    '''
    identifier = ndb.IntegerProperty()
    start_date = ndb.DateProperty(default=datetime.min)
    stream = ndb.KeyProperty(kind='Stream')
    volume = ndb.KeyProperty(kind=volumes.Volume)

@ndb.tasklet
def subscription_context(subscription):
    volume = yield subscription.volume.get_async()
    publisher = yield volume.publisher.get_async()
    raise ndb.Return({
        'subscription': subscription,
        'volume': volume,
        'publisher': publisher,
    })

def subscription_key(volume_data, create=False):
    if isinstance(volume_data, basestring):
        subscription_id = volume_data
    if isinstance(volume_data, ndb.Key):
        subscription_id = volume_data.id()
    if isinstance(volume_data, volumes.Volume):
        subscription_id = volume_data.key.id()
    user = users.user_key()
    key = ndb.Key(
        Subscription, volume_key.id(),
        parent=user,
    )
    subscription = key.get()
    if not subscription and create:
        subscription = Subscription(
            key=key,
            volume=volume_key
        )
        key = subscription.put()
    if subscription:
        return key
