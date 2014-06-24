'''Helper functions for models'''
import datetime
import json

from google.appengine.ext import ndb

class PullDBModelException(Exception):
    pass

def model_to_dict(model, json=False):
    'Convert a model instance to a serialisable dict'
    if not model:
        return {}
    model_dict = {
        'key': model.key.urlsafe(),
        'id': model.key.id(),
    }
    for key, value in model.to_dict().iteritems():
        if key == 'json' and not json:
            continue
        if isinstance(value, ndb.Key):
            model_dict[key] = value.urlsafe()
            model_dict['%s_id' % key] = value.id()
        elif isinstance(value, datetime.date):
            model_dict[key] = value.isoformat()
        else:
            model_dict[key] = unicode(value)
    return model_dict

def model_to_json(model):
    'Convert a model instance to json'
    return json.dumps(model_to_dict(model))
