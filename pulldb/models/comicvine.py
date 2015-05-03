#pylint: disable=missing-docstring
from functools import partial
import json
import logging
from math import ceil
from random import random
from time import time, sleep
from urllib import urlencode

from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.api.urlfetch_errors import DeadlineExceededError
from google.appengine.api.urlfetch_errors import DownloadError
from google.appengine.ext import ndb
from google.appengine.ext.ndb import tasklets

from pulldb.models.admin import Setting
from pulldb.varz import VarzContext

_API = None

class AsyncFuture(tasklets.Future):
    def __init__(self, future):
        super(AsyncFuture, self).__init__()
        self.future = future
        self.future.add_callback(self.set_result)

    def get_result(self):
        content = self.future.get_result()
        reply = json.loads(content)
        return reply.get('results', [])


class Comicvine(object):
    #pylint: disable=too-few-public-methods
    def __init__(self):
        self.api_base = 'https://www.comicvine.com/api'
        self.api_key = Setting.query(
            Setting.name == 'comicvine_api_key').get().value
        self.count = 0
        self.types = self._fetch_types()

    def _split_method(self, method_name):
        method = None
        tokens = method_name.split('_')
        if tokens[0] == 'fetch':
            if tokens[-1] == 'batch':
                method = self._fetch_batch
                tokens.pop()
            elif tokens[-1] == 'async':
                method = self._fetch_single_async
                tokens.pop()
            else:
                method = self._fetch_single
        if tokens[0] == 'search':
            method = self._search_resource
        resource = '_'.join(tokens[1:])
        return method, resource

    def __getattr__(self, attribute):
        method, resource = self._split_method(attribute)
        if method and resource in self.types:
            return partial(method, resource)
        else:
            raise AttributeError('%r object has no attribute %r' % (
                type(self), attribute))

    @ndb.tasklet
    def _fetch_async(self, url, **kwargs):
        context = ndb.get_context()
        response = yield context.urlfetch(url, **kwargs)
        logging.debug('_fetch_async got %r', response)
        raise ndb.Return(response)

    @VarzContext('cvstats')
    def _fetch_with_retry(self, url, retries=3, **kwargs):
        self.varz.url = url.replace(self.api_key, 'XXXX')
        for i in range(retries):
            try:
                self.varz.retries = i
                logging.info('Fetching comicvine resource %r (%d/%d)',
                             url, i, retries)
                start = time()
                response = urlfetch.fetch(url, **kwargs)
                self.count += 1
            except (DeadlineExceededError, DownloadError) as err:
                self.varz.status = 500
                logging.exception(err)
            else:
                self.varz.latency = time() - start
                self.varz.size = len(response.content)
                self.varz.status = response.status_code
                break
            # Exponential backoff with random delay in case of error
            sleep(2**i * 0.1 + random())
        return response

    def _fetch_url(self, path, deadline=5, async=False, **kwargs):
        query = {
            'api_key': self.api_key,
            'format': 'json',
        }
        query.update(**kwargs)
        query_string = urlencode(query)
        resource_url = '%s/%s?%s' % (
            self.api_base, path, query_string)
        logging.debug('Fetching comicvine resource: %s', resource_url)
        if async:
            return self._fetch_async(resource_url, deadline=deadline)
        response = self._fetch_with_retry(resource_url, deadline=deadline)
        try:
            reply = json.loads(response.content)
        except ValueError as err:
            logging.exception(err)
        else:
            if reply['error'] == 'OK':
                logging.debug('Success: %r', reply)
                return reply
            logging.error('Error: %r', reply)

    def _fetch_types(self):
        types = memcache.get('types', namespace='comicvine')
        if types:
            types = json.loads(types)
        else:
            response = self._fetch_url('types')
            types = response['results']
            if types:
                type_dict = {}
                for resource_type in types:
                    resource_name = resource_type['detail_resource_name']
                    type_dict[resource_name] = resource_type
                types = type_dict
                # Types don't change often. cache for a week
                memcache.set('types', json.dumps(types), 604800,
                             namespace='comicvine')
        return types

    def _fetch_single_async(self, resource, identifier, **kwargs):
        resource_path = self.types[resource]['detail_resource_name']
        resource_type = self.types[resource]['id']
        path = '%s/%s-%d' % (resource_path, resource_type, identifier)
        response = self._fetch_url(path, async=True, **kwargs)
        return AsyncFuture(response)

    def _fetch_single(self, resource, identifier, **kwargs):
        resource_path = self.types[resource]['detail_resource_name']
        resource_type = self.types[resource]['id']
        path = '%s/%s-%d' % (resource_path, resource_type, identifier)
        response = self._fetch_url(path, **kwargs)
        return response['results']

    def _fetch_batch(
            self, resource, identifiers, filter_attr='id', **kwargs):
        path = self.types[resource]['list_resource_name']
        filter_string = '%s:%s' % (
            filter_attr,
            '|'.join(str(id) for id in identifiers),
        )
        response = self._fetch_url(path, filter=filter_string, **kwargs)
        pages = self._response_pages(response)
        for index in range(2, pages+1):
            expected_offset = (index-1) * response['limit']
            response_page = self._fetch_url(
                path, filter=filter_string, page=index,
                offset=expected_offset, **kwargs)
            if response_page['offset'] != expected_offset:
                logging.warn('Possible API Error: '
                             'page=%r, offset=%r, expected_offset=%r',
                             index, response_page['offset'], expected_offset)
            response['results'].extend(response_page['results'])
        return response['results']

    def _response_pages(self, response):
        #pylint: disable=no-self-use
        total_results = response['number_of_total_results']
        limit = response['limit']
        pages = int(ceil(1.0*total_results/limit))
        logging.debug('%d results with %d per page.  Fetching %d pages',
                      total_results, limit, pages)
        return pages

    def _search_resource(self, resource, query, **kwargs):
        path = 'search'
        response = self._fetch_url(
            path, query=query, resources=resource, **kwargs)
        count = response['number_of_total_results']
        logging.debug('Found %d results', count)
        return int(count), response['results']

def load():
    if not _API:
        globals()['_API'] = Comicvine()
    return _API
