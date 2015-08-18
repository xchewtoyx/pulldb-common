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

class ApiError(Exception):
    def __init__(self, status, message):
        super(ApiError, self).__init__(message)
        self.status = status

    def __repr__(self):
        return '%r(%s, %s)' % (
            type(self), self.status, self.message
        )


class AsyncFuture(tasklets.Future):
    def __init__(self, future):
        super(AsyncFuture, self).__init__()
        self.future = future
        self.varz_context = VarzContext('cvstats')
        self.varz_context.start()
        self.varz = self.varz_context.varz
        self.future.add_callback(self._result_available)
        self.start = time()

    def _result_available(self):
        result = self.future.get_result()
        self.varz.latency = time() - self.start
        if result:
            logging.debug('Async fetch complete[%s]: %r',
                          result.status_code, result.content)
            try:
                reply = json.loads(result.content)
                if reply['status_code'] >= 100:
                    raise ApiError(reply['status_code'], reply['error'])
            except (TypeError, ValueError) as err:
                logging.warn('No JSON found in response: %r', result)
        self.set_result(result)

    def get_result(self):
        response = super(AsyncFuture, self).get_result()
        self.response = response
        self.varz.http_status = response.status_code
        self.varz.size = len(response.content)
        try:
            reply = json.loads(response.content)
            self.varz.status = reply.get('status_code', 0)
        except ValueError:
            self.varz.status = 500
        self.varz_context.stop()
        return reply.get('results', [])


class BatchFuture(tasklets.MultiFuture):
    #pylint: disable=too-many-instance-attributes
    def __init__(self, method, path, filter=None, **kwargs):
        super(BatchFuture, self).__init__()
        self.method = method
        self.path = path
        self.filter_string = filter
        self.kwargs = kwargs
        self.done = False
        self.fetch_first()

    def _page_available(self, future):
        result = future.get_result()
        self.putq(result)
        self.fetch_remaining(json.loads(future.response.content))

    def _fetch_page(self, page=1, offset=0):
        return self.method(self.path, filter=self.filter_string,
                           page=page, offset=offset, **self.kwargs)

    def fetch_first(self):
        first = AsyncFuture(self._fetch_page())
        first.add_callback(self._page_available, first)

    def fetch_remaining(self, first_page):
        pages = response_pages(first_page)
        for index in range(2, pages+1):
            expected_offset = (index-1) * first_page['limit']
            page_future = self._fetch_page(page=index, offset=expected_offset)
            self.add_dependent(AsyncFuture(page_future))
        self.complete()


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
            async = False
            if tokens[-1] == 'async':
                async = True
                tokens.pop()
            if tokens[-1] == 'batch':
                if async:
                    method = self._fetch_batch_async
                else:
                    method = self._fetch_batch
                tokens.pop()
            else:
                if async:
                    method = self._fetch_single_async
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
    def _fetch_async(self, url, **kwargs): # pylint: disable=no-self-use
        context = ndb.get_context()
        try:
            response = yield context.urlfetch(url, **kwargs)
        except DeadlineExceededError as err:
            logging.warn("Error fetching url %r: %r", url, err)
        else:
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
                self.varz.http_status = response.status_code
                try:
                    result = json.loads(response.content)
                    status_code = result.get('status_code', 0)
                    self.varz.status = status_code
                    if status_code >= 100:
                        raise ApiError(result['status_code'], result['error'])
                except (TypeError, ValueError):
                    self.varz.status = 500
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
            else:
                logging.error('Error: %r', reply)
            return reply

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
        response = AsyncFuture(self._fetch_url(path, async=True, **kwargs))
        response.varz.url = path
        return response

    def _fetch_single(self, resource, identifier, **kwargs):
        resource_path = self.types[resource]['detail_resource_name']
        resource_type = self.types[resource]['id']
        path = '%s/%s-%d' % (resource_path, resource_type, identifier)
        response = self._fetch_url(path, **kwargs)
        if response:
            return response['results']
        else:
            return {}

    def _fetch_batch_async(
            self, resource, identifiers, filter_attr='id', **kwargs):
        logging.info('Fetching %s resources where %r is in %r',
                     resource, filter_attr, identifiers)
        path = self.types[resource]['list_resource_name']
        filter_string = '%s:%s' % (
            filter_attr,
            '|'.join(str(id) for id in identifiers),
        )
        response = BatchFuture(
            self._fetch_url, path, filter=filter_string, async=True, **kwargs)
        return response

    def _fetch_batch(
            self, resource, identifiers, filter_attr='id', **kwargs):
        path = self.types[resource]['list_resource_name']
        filter_string = '%s:%s' % (
            filter_attr,
            '|'.join(str(id) for id in identifiers),
        )
        response = self._fetch_url(path, filter=filter_string, **kwargs)
        pages = response_pages(response)
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

    def _search_resource(self, resource, query, **kwargs):
        path = 'search'
        response = self._fetch_url(
            path, query=query, resources=resource, **kwargs)
        if response:
            count = response.get('number_of_total_results', 0)
            logging.debug('Found %d results', count)
            return int(count), response['results']
        else:
            logging.error("No response to search request: %r",
                          (resource, query, kwargs))
            return 0, []

def response_pages(response):
    total_results = response['number_of_total_results']
    limit = response['limit']
    if limit:
        pages = int(ceil(1.0*total_results/limit))
    else:
        pages = 1
    logging.debug('%d results with %d per page.  Fetching %d pages',
                  total_results, limit, pages)
    return pages

def load():
    if not _API:
        globals()['_API'] = Comicvine()
    return _API
