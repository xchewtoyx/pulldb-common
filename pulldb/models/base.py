'''Helper functions for models'''
import datetime
import json

from google.appengine.ext import ndb

class PullDBModelException(Exception):
    pass

def model_to_dict(model):
    'Convert a model instance to a serialisable dict'
    model_dict = {
        'key': model.key.urlsafe(),
    }
    for key, value in model.to_dict().iteritems():
        if isinstance(value, ndb.Key):
            model_dict[key] = value.urlsafe()
        elif isinstance(value, datetime.date):
            model_dict[key] = value.isoformat()
        else:
            model_dict[key] = unicode(value)
    return model_dict

def model_to_json(model):
    'Convert a model instance to json'
    return json.dumps(model_to_dict(model))
