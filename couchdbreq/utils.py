# -*- coding: utf-8 -
#
# This file is part of couchdb-requests released under the MIT license. 
# See the NOTICE for more information.

import urllib

def url_quote(s, charset='utf-8', safe='/:'):
    """ URL encode a single string with a given encoding. """
    if isinstance(s, unicode):
        s = s.encode(charset)
    elif not isinstance(s, str):
        s = str(s)
    return urllib.quote(s, safe=safe)

def url_encode(obj, charset="utf8", encode_keys=False):
    """ Encode params of a url where obj is a dict or a list of 2-tuples """
    ret = []
    if isinstance(obj, dict):
        it = obj.iteritems()
    else:
        it = iter(obj)

    for k, v in it:
        if encode_keys: 
            k = encode(k, charset)
        
        if not isinstance(v, (tuple, list)):
            v1 = encode(v, charset)
            ret.append('%s=%s' % (urllib.quote(k), urllib.quote_plus(v1)))
        else:
            for vi in v:
                v1 = encode(vi, charset)
                ret.append('%s=%s' % (urllib.quote(k), urllib.quote_plus(v1)))
    return '&'.join(ret)
                
def encode(v, charset="utf8"):
    if isinstance(v, unicode):
        v = v.encode(charset)
    else:
        v = str(v)
    return v

def make_uri(segments, params=None, charset="utf-8", safe="/:", encode_keys=True):
    """
    Assemble a uri based on a base, any number of path segments, and query string parameters.
    @params A list of 2-tuples or a dict
    """
    # build the path
    _path = []
    segments_len = len(segments)
    is_first_slash = False
    if segments_len:
        is_first_slash = not segments[0].endswith('/')

    count = 0
    for s in segments:
        if not s:
            continue
        
        if count == 0:
            _path.append(s.encode(charset))
        else:
            _path.append(url_quote(s, charset, safe))
        
        if count < segments_len - 1:
            if count > 0 or  is_first_slash:
                _path.append("/")
        
        count += 1

    if params is not None:
        params_str = url_encode(params, charset, encode_keys)
        if params_str:
            _path.extend(['?', params_str])

    return ''.join(_path)
