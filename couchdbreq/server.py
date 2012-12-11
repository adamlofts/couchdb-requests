# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.
import requests

from collections import deque

from .exceptions import ResourceNotFound 
from .utils import url_quote
from .database import Database
from .resource import CouchdbResource

class Session(requests.Session):
    """
    The http session for a server to use.

    See `http://docs.python-requests.org/en/latest/api/#sessionapi`.
    """

class Server(object):
    """
    A Server object represents the connection to the CouchDB database.

    :param uri: URI of the server
    :param session: A :class:`couchdbreq.Session` object. Use this to configure the
            connection parameters such as timeout, connection pool size and authentication.
    """
    uuid_batch_count = 1000
    
    def __init__(self, uri='http://127.0.0.1:5984', session=None):

        if uri.endswith("/"):
            uri = uri[:-1]

        self.uri = uri
        
        self._uuids = deque()

        if not session:
            session = Session()

        self._res = CouchdbResource(session, uri)

    def info(self):
        """
        Get server info
        """
        return self._res.get().json_body

    def get_db_names(self):
        """
        Get all database names on the server

        :return: List of unicode database names
        """
        return self._res.get('_all_dbs').json_body

    def get_db(self, dbname, is_verify_existance=True):
        """
        Get a :class:`couchdbreq.Database` object for an existing database

        If the database does not exist and is_verify_existance=True then :class:`couchdbreq.exceptions.ResourceNotFound`
        will be raised. 
        
        :param dbname: unicode name of the db
        :param is_verify_existance: bool Set to false to avoid a HEAD request checking that the database exists
        :return: :class:`couchdbreq.Database`
        :raise: :class:`couchdbreq.exceptions.ResourceNotFound` If the database does not exist
        """
        return Database(self, dbname, is_verify_existance=is_verify_existance)

    def create_db(self, dbname):
        """
        Create a database on the server
        
        :param dbname: unicode name of the db
        :return: :class:`couchdbreq.Database`
        :raise: :class:`couchdbreq.exceptions.DatabaseExistsException` If the database already exists
        """
        return Database(self, dbname, create=True)

    def get_or_create_db(self, dbname):
        """
        Get a database or create new database if missing.

        :param dbname: unicode name of the db
        :return: :class:`couchdbreq.Database`
        """
        return Database(self, dbname, get_or_create=True)

    def delete_db(self, dbname):
        ret = self._res.delete('%s/' % url_quote(dbname,
            safe=":")).json_body
        return ret

    def replicate(self,
        source,
        target,
        cancel=False,
        continuous=False,
        create_target=False,
        doc_ids=None,
        filter=None,
        proxy=None,
        query_params=None):
        """
        Replicate a database
        
        More info about replication here:
        `http://wiki.apache.org/couchdb/Replication`

        :param source: URI or dbname of the source
        :param target: URI or dbname of the target
        """
        params = {
            "source": source,
            "target": target,
            "cancel": cancel,
            "continuous": continuous,
            "create_target": create_target,
        }
        if doc_ids:
            params["doc_ids"] = doc_ids
        if filter:
            params["filter"] = filter
        if proxy:
            params["proxy"] = proxy
        if query_params:
            params["query_params"] = query_params
        
        resp = self._res.post('_replicate', payload=params)
        return resp.json_body

    def active_tasks(self):
        """ return active tasks """
        resp = self._res.get('_active_tasks')
        return resp.json_body

    def generate_uuid(self):
        try:
            return self._uuids.pop()
        except IndexError:
            response = self._res.get('_uuids', params={ 'count': self.uuid_batch_count })
            self._uuids.extend(response.json_body["uuids"])
            return self._uuids.pop()

    def __contains__(self, dbname):
        try:
            self._res.head('%s/' % url_quote(dbname, safe=":"))
        except ResourceNotFound:
            return False
        return True

    def __iter__(self):
        for dbname in self.get_db_names():
            yield Database(self, dbname)

    def __len__(self):
        return len(self.get_db_names())

    def __nonzero__(self):
        return (len(self) > 0)
