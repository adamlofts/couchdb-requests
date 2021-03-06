About
-----

Robust CouchDB Python interface using python-requests.

Documentation: https://couchdb-requests.readthedocs.org/en/latest/

Goals:
 * Only one way to do something
 * Fast and stable (connection pooled)
 * Explicit is better than implicit. Buffer sizes, connection pool size.
 * Specify query parameters, no **params in query functions
 * Not configurable with multiple backends but one single well tested backend
 * Google App Engine support
 * Cloudant support
 
Non-goals:
 * Full api coverage (In my view async support requires mainloop integration).
 
Plan to implement schema support in the future but right now it is not supported.

Dependencies
-----

python-requests >= 2.5.1

Getting Started
-----

::

  from couchdbreq import Server
 
  server = Server("http://127.0.0.1:5984", timeout=30) # 30 second timeout on all requests
  
  # List all database names
  server.get_db_names()

  # Create a db
  db = server.create_db("my_new_db")

  # Save a doc with an explicit id
  doc1 = { "_id": "doc1", "value": 3.1415 }
  db.save_doc(doc1)

  # doc1 now has a _rev
  print doc1["_rev"]

  # Save a doc with no id
  doc2 = { "value": 1.14 }
  db.save_doc(doc2)

  # doc2 has an _id and a _rev
  print doc2["_id"], doc2["_rev"]

  # List all docs in the db
  view = db.all_docs()
  for row in view:
    print row["id"], row["key"], row["value"]

  # Create a view based on the old view
  view_limited = view.filter(limit=1)
  print len(view_limited) # == 1

  # Create a design doc
  design_doc = {
    '_id': '_design/test',
    'language': 'javascript',
    'views': {
      'all': {
        "map": """function(doc) { emit(doc._id, doc.value); }"""
      }
    }
  }
  db.save_doc(design_doc)

  # Query the view
  view = db.view('test/all', descending=True)
  for row in view:
    print row['value']

  view2 = view.filter(startkey="d")
  for row in view2:
    print row['value']

  server.delete_db("my_new_db")

Authentication (Cloudant)
-----

You can use a cloudant couch like so:
::

  from couchdbreq import Server, Session
  from requests.auth import HTTPBasicAuth

  session = Session(auth=HTTPBasicAuth('username', 'password'))
  server = Server("https://username.cloudant.com", session=session)
