# Copyright 2013 Russell Heilling

import logging
import os
import sys
import urllib
import urlparse

import pulldb

def StripParam(url, param, replacement=None):
  urlparts = urlparse.urlsplit(url)
  query = urlparse.parse_qs(urlparts.query)
  logging.debug('Query params: %r', query)
  if replacement:
    query[param] = replacement
  elif param in query:
    del(query[param])
  stripped_url = urlparts._replace(
    query=urllib.urlencode(query, doseq=True))
  logging.debug('Stripped url is: %r', stripped_url)
  return stripped_url.geturl()
