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
    start_date = ndb.DateProperty(default=datetime.min)
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

def subscription_key(volume_key, create=False):
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
