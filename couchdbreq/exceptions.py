# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license. 
# See the NOTICE for more information.

"""
All exceptions used in couchdbkit.

FIXME: All exceptions should derive from the same base class and the package should
never throw any other exceptions. 
"""

class CouchException(Exception):
    """
    Base class
    
    All exceptions thrown by couchdb-requests entry points should derive from this exception
    """

class InvalidDatabaseNameError(CouchException):
    """ Thrown when an invalid database name is used """
    pass

class CompactError(CouchException):
    """ unkown exception raised by the designe """

class ResourceError(CouchException):
    """ default error class """
    
    status_int = None
    
    def __init__(self, msg=None, http_code=None, response=None):
        self.msg = msg or ''
        self.status_int = http_code or self.status_int
        self.response = response
        Exception.__init__(self)
        
    def _get_message(self):
        return self.msg
    def _set_message(self, msg):
        self.msg = msg or ''
    message = property(_get_message, _set_message)    
    
    def __str__(self):
        if self.msg:
            return self.msg
        try:
            return str(self.__dict__)
        except (NameError, ValueError, KeyError), e:
            return 'Unprintable exception %s: %s' \
                % (self.__class__.__name__, str(e))
        
    @staticmethod
    def create_from_response(resp):
        status_code = resp.status_code
        error_type = _ExceptionMap.get(status_code, RequestFailed)
        raise error_type(resp.content, http_code=status_code, response=resp)

class ResourceNotFound(ResourceError):
    """Exception raised when no resource was found at the given url. 
    """
    status_int = 404

class Unauthorized(ResourceError):
    """Exception raised when an authorization is required to access to
    the resource specified.
    """

class ResourceGone(ResourceError):
    """
    http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.11
    """
    status_int = 410

class RequestFailed(ResourceError):
    """Exception raised when an unexpected HTTP error is received in response
    to a request.
    

    The request failed, meaning the remote HTTP server returned a code 
    other than success, unauthorized, or NotFound.

    The exception message attempts to extract the error

    You can get the status code by e.http_code, or see anything about the 
    response via e.response. For example, the entire result body (which is 
    probably an HTML error page) is e.response.body.
    """

class ResourceConflict(ResourceError):
    """ Exception raised when there is conflict while updating"""

class PreconditionFailed(ResourceError):
    """ Exception raised when 412 HTTP error is received in response
    to a request """

_ExceptionMap = {
    401: Unauthorized,
    403: Unauthorized,
    404: ResourceNotFound,
    409: ResourceConflict,
    410: ResourceGone,
    412: PreconditionFailed,
}

class DatabaseExistsException(CouchException):
    """ Exception raised when a database already exists """

class RedirectLimit(Exception):
    """Exception raised when the redirection limit is reached."""

class RequestError(Exception):
    """Exception raised when a request is malformed"""

class RequestTimeout(Exception):
    """ Exception raised on socket timeout """
    
class InvalidUrl(Exception):
    """
    Not a valid url for use with this software.
    """

class ResponseError(Exception):
    """ Error raised while getting response or decompressing response stream"""

class ProxyError(Exception):
    """ raised when proxy error happend"""
    
class BadStatusLine(Exception):
    """ Exception returned by the parser when the status line is invalid"""
    pass

class InvalidAttachment(Exception):
    """ raised when an attachment is invalid """

class DuplicatePropertyError(Exception):
    """ exception raised when there is a duplicate 
    property in a model """

class BadValueError(Exception):
    """ exception raised when a value can't be validated 
    or is required """

class MultipleResultsFound(Exception):
    """ exception raised when more than one object is
    returned by the get_by method"""
    
class NoResultFound(Exception):
    """ exception returned when no results are found """
    
class ReservedWordError(Exception):
    """ exception raised when a reserved word
    is used in Document schema """
    
class DocsPathNotFound(Exception):
    """ exception raised when path given for docs isn't found """

class BulkSaveError(Exception):
    """ exception raised when bulk save contain errors.
    error are saved in `errors` property.
    """
    def __init__(self, errors, results, *args):
        self.errors = errors
        self.results = results

class ViewServerError(Exception):
    """ exception raised by view server"""

class MacroError(Exception):
    """ exception raised when macro parsiing error in functions """

class DesignerError(Exception):
    """ unkown exception raised by the designer """
