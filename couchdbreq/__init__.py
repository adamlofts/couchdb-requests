# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

from .version import version_info, __version__

try:
    from .resource import  CouchdbResource
    from .exceptions import InvalidAttachment, DuplicatePropertyError,\
BadValueError, MultipleResultsFound, NoResultFound, ReservedWordError,\
DocsPathNotFound, BulkSaveError, ResourceNotFound, ResourceConflict, \
PreconditionFailed, RequestFailed

    from .server import Server, Session
    from .database import Database

    from .schema import Property, Property, IntegerProperty,\
DecimalProperty, BooleanProperty, FloatProperty, DateTimeProperty,\
DateProperty, TimeProperty, dict_to_json, dict_to_json, dict_to_json,\
value_to_python, dict_to_python, DocumentSchema, DocumentBase, Document,\
StaticDocument, QueryMixin, AttachmentMixin, SchemaProperty, SchemaListProperty,\
SchemaDictProperty, \
ListProperty, DictProperty, StringListProperty, contain, StringProperty, \
SetProperty

except ImportError:
    import traceback
    traceback.print_exc()

import logging

LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG
}

def set_logging(level, handler=None):
    """
    Set level of logging, and choose where to display/save logs
    (file or standard output).
    """
    if not handler:
        handler = logging.StreamHandler()

    loglevel = LOG_LEVELS.get(level, logging.INFO)
    logger = logging.getLogger('couchdbkit')
    logger.setLevel(loglevel)
    format = r"%(asctime)s [%(process)d] [%(levelname)s] %(message)s"
    datefmt = r"%Y-%m-%d %H:%M:%S"

    handler.setFormatter(logging.Formatter(format, datefmt))
    logger.addHandler(handler)

