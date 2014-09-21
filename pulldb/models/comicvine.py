from functools import partial
import json
import logging
from math import ceil
from random import random
from time import sleep
from urllib import urlencode

from google.appengine.api import memcache
from google.appengine.api import urlfetch, urlfetch_errors
from google.appengine.ext import ndb

from pulldb.models.admin import Setting

_API = None

class Comicvine(object):
    def __init__(self):
        self.api_base = 'https://www.comicvine.com/api'
        self.api_key = Setting.query(
            Setting.name == 'comicvine_api_key').get().value
        self.types = self._fetch_types()

    def __getattr__(self, attribute):
        if attribute.startswith('fetch_'):
            tokens = attribute.split('_')
            if len(tokens) > 2 and tokens[2] == 'batch':
                method = self._fetch_batch
            else:
                method = self._fetch_single
            resource = tokens[1]
            return partial(method, resource)
        if attribute.startswith('search_'):
            tokens = attribute.split('_')
            resource = tokens[1]
            return partial(self._search_resource, resource)

    def _fetch_with_retry(self, url, retries=3, *args, **kwargs):
        for i in range(retries):
            try:
                response = urlfetch.fetch(url, *args, **kwargs)
            except urlfetch_errors.DeadlineExceededError as e:
                logging.exception(e)
            else:
                break
            # Exponential backoff with random delay in case of error
            sleep(2**i * 0.1 + random())
        return response

    def _fetch_url(self, path, deadline=5, **kwargs):
        query = {
            'api_key': self.api_key,
            'format': 'json',
        }
        query.update(**kwargs)
        query_string = urlencode(query)
        resource_url = '%s/%s?%s' % (
            self.api_base, path, query_string)
        logging.debug('Fetching comicvine resource: %s', resource_url)
        response = self._fetch_with_retry(resource_url, deadline=deadline)
        try:
            reply = json.loads(response.content)
        except ValueError as e:
            logging.exception(e)
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
                logging.warn(
                    'Possible API Error: page=%r, offset=%r, '
                    'expected_offset=%r' % (
                        index, results_page['offset'], expected_offset))
            response['results'].extend(response_page['results'])
        return response['results']

    def _response_pages(self, response):
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
