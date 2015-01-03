# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license. 
# See the NOTICE for more information.

class CouchException(Exception):
    """
    Base class
    
    All exceptions thrown by couchdb-requests entry points should derive from this exception.
    """

class InvalidDatabaseNameError(CouchException):
    """ Thrown when an invalid database name is used """
    pass

class InvalidDocNameError(CouchException):
    """ Thrown when an invalid doc name is used e.g. the empty string """
    pass

class RequestError(CouchException):
    """
    Exception raised when error making the request to the resource
    """
    
    def __init__(self, ex):
        self._ex = ex
    
    def __str__(self):
        return str(self._ex)

class Timeout(RequestError):
    """
    Timeout
    """
    
    def __init__(self, ex, session, uri):
        RequestError.__init__(self, ex)
        self.timeout = session.timeout
        self.uri = uri
    
    def __str__(self):
        return "Timeout(timeout=%s, uri='%s')" % (self.timeout, self.uri)
    
class ResourceError(CouchException):
    """
    General http exception
    """

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
        return error_type(resp.content, http_code=status_code, response=resp)

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

class InvalidAttachment(CouchException):
    """ raised when an attachment is invalid """

class MultipleResultsFound(CouchException):
    """ exception raised when more than one object is
    returned by the get_by method"""
    
class NoResultFound(CouchException):
    """ exception returned when no results are found """
    
class BulkSaveError(CouchException):
    """ exception raised when bulk save contain errors.
    error are saved in `errors` property.
    """
    def __init__(self, errors, results):
        self.errors = errors
        self.results = results
    
    def __str__(self):
        return '<BulkSaveError> %s results, %s errors' % (len(self.results), len(self.errors))
