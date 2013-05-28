# -*- coding: utf-8 -
#
# This file is part of couchdb-requests released under the MIT license.
# See the NOTICE for more information.
#

from .exceptions import MultipleResultsFound, NoResultFound

class View(object):
    """
    An iterable object representing a query.

    Do not construct directly. Use :meth:`couchdbreq.Database.view`, 
    :meth:`couchdbreq.Database.all_docs` or :meth:`couchdbreq.view.View.filter`.
    """

    UNDEFINED_VALUE = object()
    
    def __init__(self, db, view_path, schema=None, params=None):
        """
        Do not construct directly. Use :meth:`couchdbreq.Database.view`, 
        :meth:`couchdbreq.Database.all_docs` or :meth:`couchdbreq.view.View.filter`.
        """
        self._params = params
        
        self._db = db
        self._view_path = view_path
        self._schema = schema

    def _iterator(self, **params):
        
        mparams = {}
        for k, v in self._params.iteritems():
            if v == View.UNDEFINED_VALUE:
                continue
            mparams[k] = v
        for k, v in params.iteritems():
            if v == View.UNDEFINED_VALUE:
                continue
            mparams[k] = v
        
        keys = None
        if 'keys' in mparams:
            keys = mparams.pop('keys')
        
        if keys != None:
            resp = self._db._res.post(self._view_path, payload={ 'keys': keys }, params=mparams)
        else:
            resp = self._db._res.get(self._view_path, params=mparams)
        
        schema = self._schema
        for row in resp.json_body['rows']:
            if schema is not None:
                yield schema.wrap_row(row)
            else:
                yield row

    def first(self, is_null_exception=False):
        """
        Return the first result of this query or None if the result doesn’t contain any rows.
        
        :param is_null_exception: If True then raise :class:`couchdbreq.exceptions.NoResultFound` if no
                results are found.
        :return: A dict representing the row result or None
        """
        try:
            return self._iterator(limit=1).next()
        except StopIteration:
            if is_null_exception:
                raise NoResultFound()

            return None

    def one(self, is_null_exception=False):
        """
        Return exactly one result or raise an exception if multiple results are found.
        
        :param is_null_exception: If True then raise :class:`couchdbreq.exceptions.NoResultFound` if no
                results are found.
        :return: A dict representing the row result or None
        """

        row1 = None
        for row in self._iterator(limit=2):
            if row1:
                raise MultipleResultsFound()
            row1 = row
        
        if not row1 and is_null_exception:
            raise NoResultFound()
        
        return row1

    def all(self):
        """
        Get a list of all rows

        :return: :py:class:`list`
        """
        return list(self._iterator())

    def count(self):
        """
        Return the number of results

        :return: :py:class:`int`
        """
        # FIXME: Implement better
        count = 0
        for _ in self._iterator():
            count += 1
        return count

    def __len__(self):
        return self.count()

    def __nonzero__(self):
        return bool(self.count())
    
    def __iter__(self):
        return self._iterator()
    
    def filter(self,
         startkey=UNDEFINED_VALUE, endkey=UNDEFINED_VALUE,
         keys=UNDEFINED_VALUE, key=UNDEFINED_VALUE,
         startkey_docid=UNDEFINED_VALUE, endkey_docid=UNDEFINED_VALUE,
         skip=UNDEFINED_VALUE, limit=UNDEFINED_VALUE,
         inclusive_end=UNDEFINED_VALUE):
        """
        Return a new View object with updated query parameters.
        The original View object remains unchanged.
        
        :return: A new :class:`couchdbreq.view.View` object
        """

        params = self._params.copy()
        
        if startkey != View.UNDEFINED_VALUE:
            params['startkey'] = startkey
        if endkey != View.UNDEFINED_VALUE:
            params['endkey'] = endkey
        if keys != View.UNDEFINED_VALUE:
            params['keys'] = keys
        if key != View.UNDEFINED_VALUE:
            params['key'] = key
            
        if startkey_docid != View.UNDEFINED_VALUE:
            params['startkey_docid'] = startkey_docid
        if endkey_docid != View.UNDEFINED_VALUE:
            params['endkey_docid'] = endkey_docid
            
        if skip != View.UNDEFINED_VALUE:
            params['skip'] = skip
        if limit != View.UNDEFINED_VALUE:
            params['limit'] = limit
        if inclusive_end != View.UNDEFINED_VALUE:
            params['inclusive_end'] = inclusive_end
            
        return View(self._db, self._view_path, self._schema, params=params)
