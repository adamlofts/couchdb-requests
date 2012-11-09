# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

import re
import urllib
import time
import base64

from itertools import groupby
from mimetypes import guess_type

from .exceptions import InvalidAttachment, ResourceNotFound, ResourceConflict, BulkSaveError
from .utils import url_quote
from .view import View

def _maybe_serialize(doc):
    if hasattr(doc, "to_json"):
        # try to validate doc first
        try:
            doc.validate()
        except AttributeError:
            pass

        return doc.to_json(), True
    elif isinstance(doc, dict):
        return doc.copy(), False

    return doc, False

class Database(object):
    """ Object that abstract access to a CouchDB database
    A Database object can act as a Dict object.
    """

    VALID_DB_NAME = re.compile(r'^[a-z][a-z0-9_$()+-/]*$')
    SPECIAL_DBS = ("_users", "_replicator",)
    
    @staticmethod
    def _validate_dbname(name):
        """ validate dbname """
        if name in Database.SPECIAL_DBS:
            return True
        elif not Database.VALID_DB_NAME.match(urllib.unquote(name)):
            raise ValueError("Invalid db name: '%s'" % name) # FIXME: Don't use a generic exception here
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
    def encode_attachments(attachments):
        for v in attachments.itervalues():
            if v.get('stub', False):
                continue
            else:
                v['data'] = Database.re_sp.sub('', base64.b64encode(v['data']))
        return attachments

    def __init__(self, server, dbname, create=False):
        """
        Constructor for Database

        @param server: A Server instance
        @param dbname: The name of the database
        @param create: boolean, False by default, if True try to create the database.
        """
        self.dbname = dbname
        self.server = server
        
        #self.server_uri, self.dbname = uri.rsplit("/", 1)
        Database._validate_dbname(self.dbname)
        
        self.res = server.res(self.dbname, ":") # / is not safe for the dbname
        if create:
            try:
                self.res.head()
            except ResourceNotFound:
                self.res.put()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.dbname)

    def info(self):
        """
        Get database information

        @return: dict
        """
        return self.res.get().json_body

    def compact(self, dname=None):
        """ compact database
        @param dname: string, name of design doc. Usefull to
        compact a view.
        """
        path = "/_compact"
        if dname is not None:
            path = "%s/%s" % (path, Database._escape_docid(dname))
        res = self.res.post(path, headers={"Content-Type":
            "application/json"})
        return res.json_body

    def view_cleanup(self):
        res = self.res.post('_view_cleanup', headers={"Content-Type":
            "application/json"})
        return res.json_body

    def flush(self):
        """ Remove all docs from a database
        except design docs."""
        # save ddocs
        all_ddocs = self.all_docs(startkey="_design",
                            endkey="_design/"+u"\u9999",
                            include_docs=True)
        ddocs = []
        for ddoc in all_ddocs:
            ddoc['doc'].pop('_rev')
            ddocs.append(ddoc['doc'])

        # delete db
        self.server.delete_db(self.dbname)

        # we let a chance to the system to sync
        time.sleep(0.2)

        # recreate db + ddocs
        self.server.create_db(self.dbname)
        self.bulk_save(ddocs)

    def __contains__(self, docid):
        """Test if document exists in a database

        @param docid: str, document id
        @return: boolean, True if document exist
        """

        try:
            self.res.head(Database._escape_docid(docid))
        except ResourceNotFound:
            return False
        return True

    def get_doc(self, docid, rev=None, schema=None):
        """Get document from database

        @param docid: str, document id to retrieve
        @param rev: Get a specific revision of a document
        @param schema: A schema to pass. This is an object with a function wrap_doc(doc)
        which will be used to map the response.
        
        @return: dict, representation of CouchDB document as a dict.
        """

        params = {}
        if rev:
            params['rev'] = rev

        docid = Database._escape_docid(docid)
        doc = self.res.get(docid, params=params).json_body
        if schema is not None:
            return schema.wrap_doc(doc)
        return doc

    def get_rev(self, docid):
        """ Get last revision from docid (the '_rev' member)
        @param docid: str, undecoded document id.
        @return rev: str, the last revision of document.
        """
        response = self.res.head(Database._escape_docid(docid))
        return response.headers['etag'].strip('"')

    def save_doc(self, doc, encode_attachments=True, force_update=False,
            **params):
        """ Save a document. It will use the `_id` member of the document
        or request a new uuid from CouchDB. IDs are attached to
        documents on the client side because POST has the curious property of
        being automatically retried by proxies in the event of network
        segmentation and lost responses. (Idee from `Couchrest <http://github.com/jchris/couchrest/>`)

        @param doc: dict.  doc is updated
        with doc '_id' and '_rev' properties returned
        by CouchDB server when you save.
        @param force_update: boolean, if there is conlict, try to update
        with latest revision
        @param params, list of optionnal params, like batch="ok"

        @return res: result of save. doc is updated in the mean time
        """
        if doc is None:
            doc1 = {}
        else:
            doc1, schema = _maybe_serialize(doc)

        if '_attachments' in doc1 and encode_attachments:
            doc1['_attachments'] = Database.encode_attachments(doc['_attachments'])

        if '_id' in doc:
            docid = doc1['_id']
            docid1 = Database._escape_docid(doc1['_id'])
            try:
                res = self.res.put(docid1, payload=doc1,
                        params=params).json_body
            except ResourceConflict:
                if force_update:
                    doc1['_rev'] = self.get_rev(docid)
                    res =self.res.put(docid1, payload=doc1,
                            params=params).json_body
                else:
                    raise
        else:
            try:
                doc['_id'] = self.server.generate_uuid()
                res =  self.res.put(doc['_id'], payload=doc1,
                                    params=params).json_body
            except:
                res = self.res.post(payload=doc1, params=params).json_body

        if 'batch' in params and 'id' in res:
            doc1.update({ '_id': res['id']})
        else:
            doc1.update({'_id': res['id'], '_rev': res['rev']})


        if schema:
            doc._doc = doc1
        else:
            doc.update(doc1)
        return res

    def save_docs(self, docs, use_uuids=True, all_or_nothing=False,
            **params):
        """ bulk save. Modify Multiple Documents With a Single Request

        @param docs: list of docs
        @param use_uuids: add _id in doc who don't have it already set.
        @param all_or_nothing: In the case of a power failure, when the database
        restarts either all the changes will have been saved or none of them.
        However, it does not do conflict checking, so the documents will

        .. seealso:: `HTTP Bulk Document API <http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API>`

        """

        docs1 = []
        docs_schema = []
        for doc in docs:
            doc1, schema = _maybe_serialize(doc)
            docs1.append(doc1)
            docs_schema.append(schema)

        def is_id(doc):
            return '_id' in doc

        if use_uuids:
            noids = []
            for k, g in groupby(docs1, is_id):
                if not k:
                    noids = list(g)

            for doc in noids:
                nextid = self.server.generate_uuid()
                doc['_id'] = nextid

        payload = { "docs": docs1 }
        if all_or_nothing:
            payload["all_or_nothing"] = True

        # update docs
        results = self.res.post('_bulk_docs',
                payload=payload, params=params).json_body

        errors = []
        for i, res in enumerate(results):
            if 'error' in res:
                errors.append(res)
            else:
                if docs_schema[i]:
                    docs[i]._doc.update({
                        '_id': res['id'],
                        '_rev': res['rev']
                    })
                else:
                    docs[i].update({
                        '_id': res['id'],
                        '_rev': res['rev']
                    })
        if errors:
            raise BulkSaveError(errors, results)
        return results
    bulk_save = save_docs

    def delete_docs(self, docs, all_or_nothing=False,
            empty_on_delete=False, **params):
        """ bulk delete.
        It adds '_deleted' member to doc then uses bulk_save to save them.

        @param empty_on_delete: default is False if you want to make
        sure the doc is emptied and will not be stored as is in Apache
        CouchDB.
        @param all_or_nothing: In the case of a power failure, when the database
        restarts either all the changes will have been saved or none of them.
        However, it does not do conflict checking, so the documents will

        .. seealso:: `HTTP Bulk Document API <http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API>`


        """

        if empty_on_delete:
            for doc in docs:
                new_doc = {"_id": doc["_id"],
                        "_rev": doc["_rev"],
                        "_deleted": True}
                doc.clear()
                doc.update(new_doc)
        else:
            for doc in docs:
                doc['_deleted'] = True

        return self.bulk_save(docs, use_uuids=False,
                all_or_nothing=all_or_nothing, **params)

    bulk_delete = delete_docs

    def delete_doc(self, doc):
        """ delete a document
        @param doc: dict,  full doc.
        @return: dict like:

        .. code-block:: python

            {"ok":true,"rev":"2839830636"}
        """

        #doc1, schema = _maybe_serialize(doc)
        #if isinstance(doc1, dict):
        if not '_id' or not '_rev' in doc:
            raise KeyError('_id and _rev are required to delete a doc')

        docid = Database._escape_docid(doc['_id'])
        result = self.res.delete(docid, params={ 'rev': doc['_rev'] }).json_body
        """
        if schema:
            doc._doc.update({
                "_rev": result['rev'],
                "_deleted": True
            })
        elif isinstance(doc, dict):
        """
        doc.update({
            "_rev": result['rev'],
            "_deleted": True
        })
        return result

    def copy_doc(self, doc, dest=None, headers=None):
        """ copy an existing document to a new id. If dest is None, a new uuid will be requested
        @param doc: dict or string, document or document id
        @param dest: basestring or dict. if _rev is specified in dict it will override the doc
        """

        if not headers:
            headers = {}

        doc1, _ = _maybe_serialize(doc)
        if isinstance(doc1, basestring):
            docid = doc1
        else:
            if not '_id' in doc1:
                raise KeyError('_id is required to copy a doc')
            docid = doc1['_id']

        if dest is None:
            destination = self.server.generate_uuid()
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
            headers.update({"Destination": str(destination)})
            result = self.res.copy('%s' % docid, headers=headers).json_body
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
        Get the view results
        
        @param view_name 'designname/viewname'
        @param schema: A schema to pass. This is an object with a function wrap_row(row)
        which will be used to map the response.
        @param params: params of the view
        
        @return A View object
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

    def all_docs(self, by_seq=False, schema=None,
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
        """Get all documents from a database
        You can use all(), one(), first() just like views

        Args:
        @param by_seq: bool, if True the "_all_docs_by_seq" is passed to
        couchdb. It will return an updated list of all documents.
        @return: View
        """
        if by_seq:
            view_path = '_all_docs_by_seq'
        else:
            view_path = '_all_docs'

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

    def put_attachment(self, doc, content, name=None, content_type=None,
            content_length=None):
        """ Add attachement to a document. All attachments are streamed.

        @param doc: dict, document object
        @param content: string or :obj:`File` object.
        @param name: name or attachment (file name).
        @param content_type: string, mimetype of attachment.
        If you don't set it, it will be autodetected.
        @param content_lenght: int, size of attachment.

        @return: bool, True if everything was ok.
        """

        if not content:
            content = ""
            content_length = 0

        if hasattr(content, 'read'):
            payload = content.read()
        else:
            payload = content

        if name is None:
            if hasattr(content, "name"):
                name = content.name
            else:
                raise InvalidAttachment('You should provide a valid attachment name')

        name = url_quote(name, safe="")
        if content_type is None:
            content_type = ';'.join(filter(None, guess_type(name)))

        headers = {}
        if content_type:
            headers['Content-Type'] = content_type

        # add appropriate headers
        if content_length and content_length is not None:
            headers['Content-Length'] = unicode(content_length)

        doc1, _ = _maybe_serialize(doc)

        docid = Database._escape_docid(doc1['_id'])
        res = self.res(docid).put(name, payload=payload,
                headers=headers, params={ 'rev': doc1['_rev'] }).json_body

        if res['ok']:
            new_doc = self.get_doc(doc1['_id'], rev=res['rev'])
            doc.update(new_doc)
        return res['ok']

    def delete_attachment(self, doc, name):
        """ delete attachement to the document

        @param doc: dict, document object in python
        @param name: name of attachement

        @return: dict, with member ok set to True if delete was ok.
        """
        doc1, _ = _maybe_serialize(doc)

        docid = Database._escape_docid(doc1['_id'])
        name = url_quote(name, safe="")
        res = self.res(docid).delete(name, params={ 'rev': doc1['_rev'] }).json_body
        # FIXME: This is an extra round-trip
        if res['ok']:
            new_doc = self.get_doc(doc1['_id'], rev=res['rev'])
            doc.update(new_doc)
        return res['ok']

    def fetch_attachment(self, id_or_doc, name, stream=False, stream_chunk_size=16 * 1024):
        """ Get an attachment in a document

        @param id_or_doc: str or dict, doc id or document dict
        @param name: name of attachment
        @param stream: boolean, if True return a file object
        @param stream_chunk_size: Size in bytes to return per stream chunk (default 16 * 1024)
        
        @return: Bytestring or file like iterable
        """

        if isinstance(id_or_doc, basestring):
            docid = id_or_doc
        else:
            doc, _ = _maybe_serialize(id_or_doc)
            docid = doc['_id']

        docid = Database._escape_docid(docid)
        name = url_quote(name, safe="")

        resp = self.res(docid).get(name, stream=stream)
        if stream:
            return resp.body_stream(chunk_size=stream_chunk_size)
        
        content = resp.body_string()
        
        # Decode the attachment if content type is text
        content_type = resp.headers['content-type']
        if content_type.startswith("text/"):
            return content.decode('utf-8')
        
        return content

    def ensure_full_commit(self):
        """ commit all docs in memory """
        return self.res.post('_ensure_full_commit', headers={
            "Content-Type": "application/json"
        }).json_body

    def __len__(self):
        return self.info()['doc_count']

    def __nonzero__(self):
        return (len(self) > 0)
    
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
        response = self.res.get("_changes", params=params).json_body
        for row in response['results']:
            yield row
