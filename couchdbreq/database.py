# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

import re
import urllib
import base64

from mimetypes import guess_type

from .exceptions import InvalidAttachment, ResourceNotFound, BulkSaveError, DatabaseExistsException
from .exceptions import InvalidDatabaseNameError, ResourceError

from .utils import url_quote
from .view import View

class Database(object):
    """
    Provides access to a CouchDB database
    
    Do not construct directly. Use :meth:`couchdbreq.Server.get_db` or 
    :meth:`couchdbreq.Server.create_db`.
    """

    VALID_DB_NAME = re.compile(r'^[a-z][a-z0-9_$()+-/]*$')
    SPECIAL_DBS = ("_users", "_replicator",)
    
    @staticmethod
    def _validate_dbname(name):
        """ validate dbname """
        if name in Database.SPECIAL_DBS:
            return True
        elif not Database.VALID_DB_NAME.match(urllib.unquote(name)):
            raise InvalidDatabaseNameError()
        return True
    
    @staticmethod
    def _escape_docid(docid):
        if docid.startswith('/'):
            docid = docid[1:]
        if docid.startswith('_design'):
            docid = '_design/%s' % url_quote(docid[8:], safe='')
        else:
            docid = url_quote(docid, safe='')
        return docid
    
    re_sp = re.compile('\s')
    
    @staticmethod
    def _encode_attachments(attachments):
        for v in attachments.itervalues():
            if v.get('stub', False):
                continue
            else:
                v['data'] = Database.re_sp.sub('', base64.b64encode(v['data']))
        return attachments

    def __init__(self, server, name, create=False, get_or_create=False, is_verify_existance=True):
        """
        Internal constructor for Database

        :param server: A Server instance
        :param dbname: The name of the database
        :param create: boolean, False by default, if True try to create the database.
        :param ger_or_create: boolean, False by default, if True try to create the database.
        :raise: :class:`couchdbreq.exceptions.InvalidDatabaseNameError` if dbname is invalid
        """

        Database._validate_dbname(name)

        self.name = name
        
        self._server = server
        self._res = server._res(name, ":") # / is not safe for the dbname

        if not is_verify_existance:
            return
        
        try:
            self._res.head()
            
            if create:
                raise DatabaseExistsException()
        except ResourceNotFound:
            if not create and not get_or_create:
                raise

            self._res.put()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.name)
    
    def get_server(self):
        """
        Get the :class:`couchdbreq.server.Server` object hosting this database
        :return: :class:`couchdbreq.server.Server`
        """
        return self._server

    def info(self):
        """
        Get database information

        :return: dict
        """
        return self._res.get().json_body

    def compact(self):
        """
        Compact database
        """
        res = self._res.post("_compact", headers={"Content-Type": "application/json"})
        if res.status_int == 202:
            return True
        raise ResourceError.create_from_response(res)
    
    def compact_view(self, view_name):
        """
        Compact a view
        
        :param view_name: The name of the view _design/<view_name>
        """
        
        path = "_compact/%s" % Database._escape_docid(view_name)
        res = self._res.post(path, headers={"Content-Type": "application/json"})
        if res.status_int == 202:
            return True
        raise ResourceError.create_from_response(res)

    def view_cleanup(self):
        """
        Clean up views
        """
        res = self._res.post('_view_cleanup', headers={"Content-Type": "application/json"})
        return res.json_body

    def __contains__(self, docid):
        """
        Test if document exists in a database

        :param docid: str, document id
        :return: boolean, True if document exist
        """

        try:
            self._res.head(Database._escape_docid(docid))
        except ResourceNotFound:
            return False
        return True

    def get_doc(self, docid, rev=None, schema=None):
        """
        Get document from database

        :param docid: str, document id to retrieve
        :param rev: Get a specific revision of a document
        :param schema: A schema to pass. This is an object with a function wrap_doc(doc)
        which will be used to map the response.
        
        :return: dict, representation of CouchDB document as a dict.
        """

        params = {}
        if rev:
            params['rev'] = rev

        docid = Database._escape_docid(docid)
        doc = self._res.get(docid, params=params).json_body
        if schema is not None:
            return schema.wrap_doc(doc)
        return doc

    def get_rev(self, docid):
        """
        Get last revision from docid (the '_rev' member)

        :param docid: str, undecoded document id.
        :return rev: str, the last revision of document.
        """
        response = self._res.head(Database._escape_docid(docid))
        return response.headers['etag'].strip('"')

    def save_doc(self, doc=None, encode_attachments=True, batch=False):
        """
        Save a document. It will use the `_id` member of the document
        or request a new uuid from CouchDB. IDs are attached to
        documents on the client side because POST has the curious property of
        being automatically retried by proxies in the event of network
        segmentation and lost responses.

        :param doc: dict. doc is updated with doc '_id' and '_rev' properties returned by CouchDB server when you save.
        :param batch: If true then use reduced guarantee that the document has been saved. The _rev field
                will not be updated.
        :return: doc updated with '_id' and '_rev'
        :raise: :class:`couchdbreq.exceptions.ResourceConflict` if the save generated a conflict
        """
        if doc is None:
            doc1 = {}
        else:
            doc1 = doc

        if '_attachments' in doc1 and encode_attachments:
            doc1['_attachments'] = Database._encode_attachments(doc['_attachments'])
            
        params = None
        if batch:
            params = { 'batch': 'ok' }

        if '_id' in doc:
            docid = doc1['_id']
        else:
            docid = self._server.generate_uuid()
            
        docid1 = Database._escape_docid(docid)
        res = self._res.put(docid1, payload=doc1, params=params).json_body

        if batch:
            doc1.update({ '_id': res['id']})
        else:
            doc1.update({'_id': res['id'], '_rev': res['rev']})

        doc.update(doc1)
        return res

    def save_docs(self, docs, use_uuids=True, all_or_nothing=False):
        """
        Save multiple docs at once

        :param docs: list of docs
        :param use_uuids: add _id in doc who don't have it already set.
        :param all_or_nothing: In the case of a power failure, when the database
        restarts either all the changes will have been saved or none of them.
        However, it does not do conflict checking.

        .. seealso:: `HTTP Bulk Document API <http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API>`
        """

        if use_uuids:
            for doc in docs:
                if '_id' not in doc:
                    doc['_id'] = self._server.generate_uuid()

        payload = { "docs": docs }
        if all_or_nothing:
            payload["all_or_nothing"] = True

        results = self._res.post('_bulk_docs', payload=payload).json_body

        errors = []
        for i, res in enumerate(results):
            if 'error' in res:
                errors.append(res)
            else:
                docs[i].update({
                    '_id': res['id'],
                    '_rev': res['rev']
                })
        if errors:
            raise BulkSaveError(errors, results)
        return results

    def delete_docs(self, docs, all_or_nothing=False):
        """
        Delete many docs at once.
        
        It adds '_deleted' member to doc then uses bulk_save to save them.

        :param all_or_nothing: In the case of a power failure, when the database
        restarts either all the changes will have been saved or none of them.
        However, it does not do conflict checking, so the documents will

        .. seealso:: `HTTP Bulk Document API <http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API>`
        """
        for doc in docs:
            doc['_deleted'] = True

        return self.save_docs(docs, use_uuids=False, all_or_nothing=all_or_nothing)

    def delete_doc(self, doc):
        """
        Delete a document
        
        The document will have a _deleted field set to true.
        
        :param doc: The doc
        :return: dict like: {"ok":true,"rev":"2839830636"}
        """

        if not '_id' or not '_rev' in doc:
            raise KeyError('_id and _rev are required to delete a doc')

        docid = Database._escape_docid(doc['_id'])
        result = self._res.delete(docid, params={ 'rev': doc['_rev'] }).json_body
        doc.update({
            "_rev": result['rev'],
            "_deleted": True
        })
        return result

    def copy_doc(self, doc, dest=None):
        """
        Copy an existing document to a new id. If dest is None, a new uuid will be requested
        
        :param doc: dict or string, document or document id
        :param dest: basestring or dict. if _rev is specified in dict it will override the doc
        """

        if isinstance(doc, basestring):
            docid = doc
        else:
            if not '_id' in doc:
                raise KeyError('_id is required to copy a doc')
            docid = doc['_id']

        if dest is None:
            destination = self._server.generate_uuid()
        elif isinstance(dest, basestring):
            if dest in self:
                dest = self.get(dest)
                destination = "%s?rev=%s" % (dest['_id'], dest['_rev'])
            else:
                destination = dest
        elif isinstance(dest, dict):
            if '_id' in dest and '_rev' in dest and dest['_id'] in self:
                destination = "%s?rev=%s" % (dest['_id'], dest['_rev'])
            else:
                raise KeyError("dest doesn't exist or this not a document ('_id' or '_rev' missig).")

        if destination:
            headers = { "Destination": str(destination) }
            result = self._res.copy('%s' % docid, headers=headers).json_body
            return result

        return { 'ok': False }

    def view(self, view_name, schema=None,
             startkey=View.UNDEFINED_VALUE, endkey=View.UNDEFINED_VALUE,
             keys=None, key=View.UNDEFINED_VALUE,
             startkey_docid=View.UNDEFINED_VALUE, endkey_docid=View.UNDEFINED_VALUE,
             stale=None,
             descending=False,
             skip=0, limit=None,
             group=View.UNDEFINED_VALUE, group_level=View.UNDEFINED_VALUE,
             reduce=View.UNDEFINED_VALUE,
             include_docs=False,
             inclusive_end=True,
             update_seq=False):
        """
        Query for view results
        
        :param view_name: 'designname/viewname'
        :param schema: A schema to pass. This is an object with a function wrap_row(row)
        which will be used to map the response.
        
        :return: :class:`couchreq.view.View`
        """
        design_name, view_name = view_name.split('/', 1)
        view_path = '_design/%s/_view/%s' % (design_name, view_name)
        
        params = {
            'startkey': startkey,
            'endkey': endkey,
            'keys': keys,
            'key': key,
            'startkey_docid': startkey_docid,
            'endkey_docid': endkey_docid,
            'stale': stale,
            'descending': descending,
            'skip': skip,
            'limit': limit,
            'group': group,
            'group_level': group_level,
            'reduce': reduce,
            'include_docs': include_docs,
            'inclusive_end': inclusive_end,
            'update_seq': update_seq
        }
        return View(self, view_path, schema=schema, params=params)

    def all_docs(self, schema=None,
                 startkey=View.UNDEFINED_VALUE, endkey=View.UNDEFINED_VALUE,
                 keys=None, key=View.UNDEFINED_VALUE,
                 startkey_docid=View.UNDEFINED_VALUE, endkey_docid=View.UNDEFINED_VALUE,
                 stale=None,
                 descending=False,
                 skip=0, limit=None,
                 group=View.UNDEFINED_VALUE, group_level=View.UNDEFINED_VALUE,
                 reduce=View.UNDEFINED_VALUE,
                 include_docs=False,
                 inclusive_end=True,
                 update_seq=False):
        """
        Get all documents from a database
        
        You can use all(), one(), first() on the returned View object just like a View from :meth:`couchdbreq.database.view`.
        
        :return: :class:`couchreq.view.View`
        """
        params = {
            'startkey': startkey,
            'endkey': endkey,
            'keys': keys,
            'key': key,
            'startkey_docid': startkey_docid,
            'endkey_docid': endkey_docid,
            'stale': stale,
            'descending': descending,
            'skip': skip,
            'limit': limit,
            'group': group,
            'group_level': group_level,
            'reduce': reduce,
            'include_docs': include_docs,
            'inclusive_end': inclusive_end,
            'update_seq': update_seq
        }
        return View(self, '_all_docs', schema=schema, params=params)

    def put_attachment(self, doc, content, name=None, content_type=None, content_length=None):
        """
        Add attachment to a document

        If you are storing unicode text then you must encode before passing it to this function. e.g.
        db.put_attachment(doc, u"Some unicode £".encode("utf8"), "My unicode attachment", "text/plain")

        :param doc: dict
        :param content: str or file like object.
        :param name: name of attachment (unicode or str) encoded as utf8
        :param content_type: string, mimetype of attachment. If you don't set it, it will be autodetected.
        :param content_lenght: int, size of attachment in bytes

        :return: bool, True if everything was ok.
        """

        if not (isinstance(content, bytes) or
            isinstance(content, str) or 
            hasattr(content, 'read')):
            raise InvalidAttachment("Attachment must be a str, bytes or a stream.")

        if name is None:
            name = 'attachment'

        if content_type is None:
            content_type = ';'.join(filter(None, guess_type(name)))

        headers = {
            'Content-Type': content_type,
        }

        if content_length != None:
            headers['Content-Length'] = str(content_length)

        docid = Database._escape_docid(doc['_id'])
        res = self._res(docid).put(name,
                headers=headers, params={ 'rev': doc['_rev'] }, payload=content).json_body

        if res['ok']:
            new_doc = self.get_doc(doc['_id'], rev=res['rev'])
            doc.update(new_doc)
        return res['ok']

    def delete_attachment(self, doc, name):
        """
        Delete attachment on the document

        :param doc: dict
        :param name: name of attachment (unicode or str)

        :return: dict, with member ok set to True if delete was ok.
        """

        docid = Database._escape_docid(doc['_id'])
        resp = self._res(docid).delete(name, params={ 'rev': doc['_rev'] })
        res = resp.json_body

        if res['ok']:
            new_doc = self.get_doc(doc['_id'], rev=res['rev'])
            doc.update(new_doc)

            if not new_doc.has_key('_attachments') and doc.has_key('_attachments'):
                del doc['_attachments']

        return res['ok']

    def fetch_attachment(self, id_or_doc, name, stream=False, stream_chunk_size=16 * 1024):
        """
        Get an attachment in a document
        
        Note: If you have stored text e.g. utf8 in the attachment you will need to decode the response
        to this call using .decode('utf8').

        :param id_or_doc: str or dict, doc id or document dict
        :param name: name of attachment (unicode or str)
        :param stream: boolean, if True return a file object
        :param stream_chunk_size: Size in bytes to return per stream chunk (default 16 * 1024)
        
        :return: Bytestring or file like iterable if stream=True
        """

        if isinstance(id_or_doc, basestring):
            docid = id_or_doc
        else:
            docid = id_or_doc['_id']

        docid = Database._escape_docid(docid)
        name = url_quote(name, safe="")
        resp = self._res(docid).get(name, stream=stream)
        if stream:
            return resp.body_stream(chunk_size=stream_chunk_size)
        
        content = resp.body_string()
        return content

    def ensure_full_commit(self):
        """
        Commit all docs in memory
        
        This can be used after saving docs with batch=True to ensure they are all saved to disk.
        """
        return self._res.post('_ensure_full_commit', headers={
            "Content-Type": "application/json"
        }).json_body

    def length(self):
        """
        Count the number of documents in the database.
        
        This is implemented with a HEAD request to the database.
        """
        return self.info()['doc_count']
    
    def changes(self,
        since=None,
        limit=None,
        descending=False,
        filter=None,
        include_docs=False,
        style="main_only"):
        """
        Get changes from the db
        
        Only feed=normal is supported because other feed types involve integration with a mainloop
        """
        
        params = {
            'since': since,
            'limit': limit,
            'descending': descending,
            'filter': filter,
            'include_docs': include_docs,
            'style': style,
        }
        response = self._res.get("_changes", params=params).json_body
        for row in response['results']:
            yield row
