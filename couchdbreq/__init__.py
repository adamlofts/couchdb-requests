# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license.
# See the NOTICE for more information.

from .version import version_info, __version__

from .exceptions import InvalidAttachment, DuplicatePropertyError, BadValueError, MultipleResultsFound, NoResultFound, ReservedWordError
from .exceptions import DocsPathNotFound, BulkSaveError, ResourceNotFound, ResourceConflict, PreconditionFailed, RequestFailed

from .server import Server, Session

"""
    from .schema import Property, Property, IntegerProperty,\
DecimalProperty, BooleanProperty, FloatProperty, DateTimeProperty,\
DateProperty, TimeProperty, dict_to_json, dict_to_json, dict_to_json,\
value_to_python, dict_to_python, DocumentSchema, DocumentBase, Document,\
StaticDocument, QueryMixin, AttachmentMixin, SchemaProperty, SchemaListProperty,\
SchemaDictProperty, \
ListProperty, DictProperty, StringListProperty, contain, StringProperty, \
SetProperty
"""
