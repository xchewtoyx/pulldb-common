#pylint: disable=missing-docstring
import logging
from time import time

class VarzContext(object):
    '''Decorate a hander within a Varz context.

    This class behaves both as a decorator and a context handler.  The
    handler method decorated will have a context varz installed into
    its instance.  When the handler method returns the content of the
    varz will be logged.
    '''
    #pylint: disable=too-few-public-methods
    def __init__(self, context):
        self.context = context
        self.varz = None

    def __call__(self, method, *args, **kwargs):
        logging.debug('Entering Varz context %r', self.context)
        def wrap(instance, *args, **kwargs):
            with self:
                self.varz = Varz(name=self.context)
                instance.varz = self.varz
                return method(instance, *args, **kwargs)

        return wrap

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.varz.status=500
        logging.info('varz: %r', self.varz)

    def start(self):
        self.varz = Varz(name=self.context)

    def stop(self):
        return self.__exit__(None, None, None)


class Varz(object):
    #pylint: disable=too-few-public-methods
    def __init__(self, **kwargs):
        self._start_time = time()
        self._varz = kwargs

    def __getattr__(self, attribute):
        if attribute in self._varz:
            return self._varz[attribute]
        else:
            return None

    def __setattr__(self, attribute, value):
        if attribute.startswith('_'):
            super(Varz, self).__setattr__(attribute, value)
        else:
            self._varz[attribute] = value

    def __repr__(self):
        self._varz['elapsed'] = time() - self._start_time
        stats = ['%s=%s' % item for item in self._varz.items()]
        return ' '.join(stats)
