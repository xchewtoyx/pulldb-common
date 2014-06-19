# Copyright 2013 Russell Heilling
from datetime import datetime

from google.appengine.ext import ndb

from pulldb.models import volumes

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
