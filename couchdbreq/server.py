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
    pass

class Server(object):
    """ Server object that allows you to access and manage a couchdb node.
    A Server object can be used like any `dict` object.
    """

    uuid_batch_count = 1000
    
    def __init__(self, uri='http://127.0.0.1:5984', session=None):

        if uri.endswith("/"):
            uri = uri[:-1]

        self.uri = uri
        
        self._uuids = deque()

        if not session:
            session = Session()

        self.res = CouchdbResource(session, uri)

    def info(self):
        return self.res.get().json_body

    def get_db_names(self):
        """ get list of databases in CouchDb host

        """
        return self.res.get('_all_dbs').json_body

    def get_db(self, dbname):
        return Database(self, dbname)

    def create_db(self, dbname):
        """
        Create a database on CouchDb host
        
        If the database already exists then DatabaseExistsException is raised

        @param dname: str, name of db
        @return: Database instance
        """
        return Database(self, dbname, create=True)

    def get_or_create_db(self, dbname):
        """
        Get a database or create a database if missing.
        @return: Database instance
        """
        return Database(self, dbname, get_or_create=True)

    def delete_db(self, dbname):
        ret = self.res.delete('%s/' % url_quote(dbname,
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
        simple handler for replication

        @param source: str, URI or dbname of the source
        @param target: str, URI or dbname of the target
        @param params: replication options

        More info about replication here :
        http://wiki.apache.org/couchdb/Replication

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
        
        resp = self.res.post('_replicate', payload=params)
        return resp.json_body

    def active_tasks(self):
        """ return active tasks """
        resp = self.res.get('_active_tasks')
        return resp.json_body

    def generate_uuid(self):
        try:
            return self._uuids.pop()
        except IndexError:
            response = self.res.get('_uuids', params={ 'count': self.uuid_batch_count })
            self._uuids.extend(response.json_body["uuids"])
            return self._uuids.pop()

    def __contains__(self, dbname):
        try:
            self.res.head('%s/' % url_quote(dbname, safe=":"))
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
