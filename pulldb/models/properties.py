from google.appengine.ext import ndb

class ImageProperty(ndb.StringProperty):
    def _from_base_type(self, value):
        if value.startswith('/'):
            return 'http://static.comicvine.com' + value
