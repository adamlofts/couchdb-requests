# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

import requests
import json

from . import __version__

from .exceptions import RequestError, ResourceError
from .utils import make_uri

USER_AGENT = 'couchdbreq/%s' % __version__

class ResponseStream(object):
    
    def __init__(self, it):
        self.iter = it
        self.buf = ""

    def __enter__(self):
        return self

    def __exit__(self, with_type, value, traceback):
        pass # FIXME: Tidy up ??
    
    def __iter__(self):
        return self.iter
    
    def read(self, size=None):
        buf = self.buf
        
        # If we have enought data already then we are done
        if size and len(buf) >= size:
            ret = buf[:size]
            self.buf = buf[size:]
            return ret
        
        # Look for more data
        for chunk in self:
            buf += chunk
            
            if size and len(buf) >= size:
                break

        # Return at most size bytes
        ret = buf[:size]
        self.buf = buf[size:]
        return ret
        
class CouchDBResponse(object):

    def __init__(self, response):
        self.response = response
        self.status_int = response.status_code
        self.headers = response.headers

    @property
    def json_body(self):
        body = self.response.content
        return json.loads(body)

    def body_string(self):
        return self.response.content
 
    def body_stream(self, chunk_size):
        return ResponseStream(self.response.iter_content(chunk_size=chunk_size))

class CouchdbResource(object):

    safe = ":/%" # FIXME: Remove?
    encode_keys = True # FIXME: Remove?
    charset = 'utf-8' # FIXME: Remove?
    response_class = CouchDBResponse

    def __init__(self, session, uri="http://127.0.0.1:5984"):
        self.session = session
        self.uri = uri

    def copy(self, path=None, headers=None, params=None, stream=False):
        """ add copy to HTTP verbs """
        return self.request('COPY', path=path, headers=headers, params=params, stream=stream)

    def get(self, path=None, headers=None, params=None, stream=False):
        return self.request("GET", path=path, headers=headers, params=params, stream=stream)

    def head(self, path=None, headers=None, params=None, stream=False):
        return self.request("HEAD", path=path, headers=headers,
                params=params, stream=stream)

    def delete(self, path=None, headers=None, params=None, stream=False):
        return self.request("DELETE", path=path, headers=headers,
                params=params, stream=stream)

    def post(self, path=None, payload=None, headers=None, params=None, stream=False):
        return self.request("POST", path=path, payload=payload, 
                        headers=headers, params=params, stream=stream)

    def put(self, path=None, payload=None, headers=None, params=None, stream=False):
        return self.request("PUT", path=path, payload=payload,
                        headers=headers, params=params, stream=stream)

    def request(self, method, path=None, payload=None, headers=None, params=None, stream=False):
        """
        Perform HTTP call to the couchdb server and manage

        @param method: str, the HTTP action to be performed:
            'GET', 'HEAD', 'POST', 'PUT', or 'DELETE'
        @param path: str or list, path to add to the uri
        @param headers: dict, optional headers that will
            be added to HTTP request.
        @param params: Optional parameters added to the request.
        @param stream Should the request be streamed
        @return: response object
        """

        headers = headers or {}
        headers.setdefault('Accept', 'application/json')
        headers.setdefault('User-Agent', USER_AGENT)

        if payload is not None:
            #TODO: handle case we want to put in payload json file.
            if not hasattr(payload, 'read') and not isinstance(payload, basestring):
                payload = json.dumps(payload).encode('utf-8')
                headers.setdefault('Content-Type', 'application/json')

            if isinstance(payload, unicode):
                payload = payload.encode(self.charset)
        
        params = self._encode_params(params)
        uri = make_uri((self.uri, path), params=params, charset=self.charset, 
                       safe=self.safe, encode_keys=self.encode_keys)

        try:
            resp = self.session.request(method, url=uri,
                             data=payload, headers=headers, prefetch=not stream)
        except requests.ConnectionError as e:
            raise RequestError(e)

        status_code = resp.status_code

        if status_code >= 400:
            raise ResourceError.create_from_response(resp)

        return self.response_class(resp)
    
    def __call__(self, path, safe=None):
        """ Create a sub resource using the same session. """

        if not safe:
            safe = self.safe

        new_uri = make_uri((self.uri, path), charset=self.charset, 
                        safe=safe, encode_keys=self.encode_keys)

        return CouchdbResource(self.session, new_uri)
    
    
    _JSON_PARAMS = (
        'startkey',
        'endkey',
        'key',
        'keys',
    )
    
    _BOOLEAN_PARAMS = (
        'descending',
        'group',
        'reduce',
        'include_docs',
        'inclusive_end',
        'update_seq',
        'cancel',
        'continuous',
        'create_target',
    )
    
    def _encode_params(self, params):
        """ encode parameters in json if needed """
        
        if isinstance(params, dict):
            _params = {}
            for name, value in params.items():
                if value is None:
                    continue
                
                if name in CouchdbResource._JSON_PARAMS:
                    value = json.dumps(value)
                elif name in CouchdbResource._BOOLEAN_PARAMS:
                    if value:
                        value = 'true'
                    else:
                        value = 'false'
                    
                _params[name] = value
            return _params
        
        return params
