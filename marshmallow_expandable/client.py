import logging
import requests, json
from urllib.parse import urljoin
from decimal import Decimal

logger = logging.getLogger(__name__)

class Request(dict):
    def __init__(self, uri, method, body=None, headers=None):
        super(Request, self).__init__()

        self._uri = uri
        self._method = method
        self._body = body

        if headers is None:
            headers = {}

        self._headers = headers

        self.update(dict([(k.title().replace('_', '-'), v) for k, v in headers.items()]))

    @property
    def headers(self):
        """
        Return the actual headers.
        """
        return self._headers

    @property
    def uri(self):
        return self._uri

    @property
    def method(self):
        return self._method

    @property
    def body(self):
        return self._body


class Response(dict):
    def __init__(self, client, response, request):
        super(Response, self).__init__()

        self.client = client
        self.headers = response.headers
        self.raw_content = response.content
        self.content = response.content
        self.request = request

        # Make headers consistently accessible.
        self.update(dict([(k.title().replace('_', '-'), v) for k, v in response.headers.items()]))

        # Set status code on its own property.
        # assert isinstance(response, requests.Response), response
        self.status_code = response.status_code


class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)

        from restorm.rest import RestObject
        if isinstance(o, RestObject):
            return o._obj

        super(CustomEncoder, self).default(o)

class ClientMixin(object):
    """
    This mixin contains the attribute ``MIME_TYPE`` which is ``None`` by
    default. Subclasses can set it to some mime-type that will be used as
    ``Content-Type`` and ``Accept`` header in requests.

    If the ``MIME_TYPE`` is also found in the ``Content-Type`` response headers,
    the response contents will be deserialized.
    """
    root_uri = ''

    MIME_TYPE = None

    def serialize(self, data):
        """
        Produces a serialized version suitable for transfer over the wire.

        Subclasses should override this function to implement their own
        serializing scheme. This implementation simply returns the data passed
        to this function.

        Data from the serialize function passed to the deserialize function,
        and vice versa, should return the same value.

        :param data: Data.

        :return: Serialized data.
        """
        if self.MIME_TYPE == 'application/json':
            if data is None:
                return ''
            return json.dumps(data, cls=CustomEncoder)
        return data

    def deserialize(self, data, object_hook=None):
        """
        Deserialize the data from the raw data.

        Subclasses should override this function to implement their own
        deserializing scheme. This implementation simply returns the data
        passed to this function.

        Data from the serialize function passed to the deserialize function,
        and vice versa, should return the same value.

        :param data: Serialized data.

        :return: Data.
        """
        if self.MIME_TYPE == 'application/json':
            if data == '':
                return None
            if object_hook:
                return json.loads(data,
                                parse_float=Decimal,
                                object_hook=object_hook)
            return json.loads(data, parse_float=Decimal)
        return data

    def create_request(self, uri, method, body=None, headers=None):
        """
        Returns a ``Request`` object.
        """
        if not uri.startswith(self.root_uri):
            uri = urljoin(self.root_uri, uri)

        if headers is None:
            headers = {}

        if self.MIME_TYPE:
            headers.update({
                'Accept': self.MIME_TYPE,
                'Content-Type': self.MIME_TYPE,
            })

        data = self.serialize(body)

        return Request(uri, method, data, headers)

    def create_response(self, response, request):
        """
        Returns a ``Response`` object.
        """
        wrapped_response = Response(self, response, request)

        if not self.MIME_TYPE or ('Content-Type' in response.headers and
                                  response.headers['Content-Type'].startswith(self.MIME_TYPE)):
            wrapped_response.content = self.deserialize(response.content)

        return wrapped_response

    def get(self, uri):
        """
        Convenience method that performs a GET-request.
        """
        return self.request(uri, 'GET')

    def post(self, uri, data):
        """
        Convenience method that performs a POST-request.
        """
        return self.request(uri, 'POST', data)

    def put(self, uri, data):
        """
        Convenience method that performs a PUT-request.
        """
        return self.request(uri, 'PUT', data)

    def delete(self, uri):
        """
        Convenience method that performs a DELETE-request.
        """
        return self.request(uri, 'DELETE')


class BaseClient(object):
    """
    Simple RESTful client based on ``httplib2.Http``.
    """
    def __init__(self, *args, **kwargs):
        """
        Takes one additional argument ``root_uri``. All other arguments are
        passed to the ``httplib2.Http`` constructor.
        """
        if 'root_uri' in kwargs:
            self.root_uri = kwargs.pop('root_uri')

        if 'type' in kwargs:
            self.MIME_TYPE = kwargs.pop('type')

        super(BaseClient, self).__init__(*args, **kwargs)

    def request(self, uri, method='GET', body=None, headers=None):
        """
        Creates a ``Request`` object by calling
        ``self.create_request(uri, method, body, headers)`` and performs the low
        level HTTP-request using this request object. A ``Response`` object is
        created with the data returned from the request, by calling
        ``self.create_response(response_headers, response_content, request)``
        and is returned.
        """
        # Create request.
        request = self.create_request(uri, method, body, headers)

        # Perform an HTTP-request with ``httplib2``.
        try:
            response = requests.request(
                url=request.uri,
                method=request.method.lower(),
                data=request.body,
                headers=request)
        except Exception as e:
            # Logging.
            logger.critical(
                '%(method)s %(uri)s\n%(headers)s\n\n%(body)s\n\n\nException: %(exception)s', {
                    'method': request.method,
                    'uri': request.uri,
                    'headers': '\n'.join(['%s: %s' % (k, v) for k, v in request.items()]),
                    'body': request.body if request.body else '',
                    'exception': unicode(e),
                })
            raise
        else:
            wrapped_response = self.create_response(response, request)
            # Logging.
            if logger.level > logging.DEBUG:
                logger.info('%(method)s %(uri)s (HTTP %(response_status)s)' % {
                    'method': request.method,
                    'uri': request.uri,
                    'response_status': response.status_code
                })
            else:
                logger.debug(
                    '%(method)s %(uri)s\n%(headers)s\n\n%(body)s\n\n\nHTTP %(response_status)s\n'
                    '%(response_headers)s\n\n%(response_content)s', {
                        'method': request.method,
                        'uri': request.uri,
                        'headers': '\n'.join(['%s: %s' % (k, v) for k, v in request.items()]),
                        'body': request.body if request.body else '',
                        'response_status': response.status_code,
                        'response_headers': '\n'.join(
                            ['%s: %s' % (k, v) for k, v in response.headers.items()]),
                        # Show the actual content, not response.content
                        'response_content': response.content
                    })

            return wrapped_response


class Client(BaseClient, ClientMixin):
    pass
