import itertools
import functools
import cursor
import dson


class FieldsGroup(object):
    
    def __init__(self, fields):
        field_names = []
        virtual_fields = []
        
        for f in fields:
            if isinstance(f, tuple):
                field_names.append(f[0])
                virtual_fields.append(f)
            else:
                field_names.append(f)
        
        self.names = tuple(field_names)
        self.virtual = dict(virtual_fields)
        
        
    def __getitem__(self, i):
        return self.names[i]
        
        
    def __iter__(self):
        return iter(self.names)
        
        
    def __str__(self):
        return str(self.names)
        
        
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, str(self.names))
        
        
    def __eq__(self, other):
        if isinstance(other, FieldsGroup):
            return self.names == other.names
        elif isinstance(other, tuple):
            return other == self.names
        return False
        
        
    def __hash__(self):
        return hash(self.names)


class IndexCollection(object):
    
    def __init__(self, dbm, name):
        self.dbm = dbm
        self.name = name
        self.indexes = {}
        self.dbm.add_namespace('_indexes')
        
        
    def add(self, *fields, **kwargs):
        fields = FieldsGroup(fields)
        assert fields not in self.indexes, "Attempting to redefine index %s" % str(fields)
        self.indexes[fields] = Index(self.dbm, self.name, fields, 
            unique=kwargs.get('unique', False), filter=kwargs.get('filter', None))
        
        
    def __contains__(self, fields):
        if not isinstance(fields, tuple):
            fields = (fields,)
        return fields in self.indexes
        
        
    def get(self, fields):
        if not isinstance(fields, tuple):
            fields = (fields,)
        index = self.indexes.get(fields)
        if not index:
            raise KeyError, "No index for %s" % str(fields)
        return index
        
        
    def __getitem__(self, fields):
        return self.get(fields)
        
        
    def __iter__(self):
        for k in self.indexes:
            yield k
        
        
    def update(self, old_doc, new_doc):
        for index in self.indexes.values():
            index.update(old_doc, new_doc)
            
            
    def remove(self, doc):
        for index in self.indexes.values():
            index.remove(doc)
            
            
    def sync(self):
        self.dbm.transaction_start(writable=False)
        try:
            if self.dbm.count(self.name) == 0:
                self.dbm.transaction_commit()
                return
            
            data = self.dbm.get('_indexes', self.name)
        except:
            self.dbm.transaction_abort()
            raise
        else:
            self.dbm.transaction_commit()
            
            if data:
                field_groups = dson.loads(data)
            else:
                field_groups = []
            
            self.dbm.transaction_start(writable=True)
            
            try:
                indexes_to_sync = []
                new_field_groups = []
                for fields, index in self.indexes.items():
                    new_field_groups.append(fields.names)
                    if fields not in field_groups:
                        index.remove_all()
                        indexes_to_sync.append(index)
                
                self.dbm.put('_indexes', self.name, dson.dumps(new_field_groups))
                if len(indexes_to_sync) > 0:
                    c = cursor.Cursor(self.dbm, self.name)
                    for doc in c:
                        for index in indexes_to_sync:
                            index.update(None, doc)
            except:
                self.dbm.transaction_abort()
                raise
            else:
                self.dbm.transaction_commit()
        
        
    
class Index(object):
    
    def __init__(self, dbm, table_name, fields, unique=False, filter=None):
        self.dbm = dbm
        self.table_name = table_name
        self.fields = fields
        self.name = '%s.%s' % (self.table_name, ','.join(self.fields.names))
        self.dbm.add_namespace(self.name, duplicate_keys=(not unique))
        self.filter = filter
        
        
    def get_fields(self):
        return self.fields
        
        
    def update(self, old_doc, new_doc):
        if self.filter and not self.filter(new_doc):
            if old_doc and self.filter(old_doc):
                self.remove(old_doc)
            return
            
        value = dson.dumpone(new_doc['id'])
        new_keys = self.make_keys(new_doc)
        
        if old_doc:
            old_keys = self.make_keys(old_doc)
            if old_keys == new_keys:
                return
            for k in old_keys:
                self.dbm.delete(self.name, k, value)
        for k in new_keys:
            self.dbm.put(self.name, k, value)
        
        
    def get(self, key):
        string_key = self.get_key(key)
        value = self.dbm.get(self.name, string_key)
        if value:
            doc_value = self.dbm.get(self.table_name, value)
            if doc_value:
                return dson.loads(doc_value)
    
    def all(self, key):
        string_key = self.get_key(key)
        cur = self.dbm.cursor(self.name)
        cur.jump(string_key)
        while cur.key() == string_key:
            doc_key = cur.value()
            doc_value = self.dbm.get(self.table_name, doc_key)
            if doc_value:
                yield dson.loads(doc_value)
            cur.next()
        
        
    def remove(self, doc):
        key = self.get_key(doc)
        value = dson.dumpone(doc['id'])
        self.dbm.delete(self.name, key, value=value)
        
        
    def remove_all(self):
        self.dbm.delete_all(self.name)
        
        
    def cursor(self, reverse=False):
        return IndexCursor(self, reverse)
        
        
    def count(self):
        return self.dbm.count(self.name)
        
        
    def make_keys(self, doc):
        rows = []
        for f in self.fields.names:
            if f in self.fields.virtual:
                values = self.fields.virtual[f](doc)
                if len(values) > 0:
                    rows.append(values)
            else:
                try:
                    value = self.get_value(doc, f)
                except KeyError:
                    continue
                else:
                    if isinstance(value, list):
                        rows.append([value] + value)
                    else:
                        rows.append([value])
        keys = []
        for row in itertools.product(*rows):
            keys.append(self.dump_key(row))
        return keys
        
        
    def get_value(self, doc, field):
        if '.' in field:
            parts = field.split('.')
            child_doc = doc.get(parts[0])
            if not child_doc:
                raise KeyError, field
            return self.get_value(child_doc, '.'.join(parts[1:]))
        else:
            return doc.get(field)
        
        
    def get_key(self, doc):
        if isinstance(doc, dict):
            parts = []
            for f in self.fields:
                assert f in doc, "Missing value for field '%s' in index %s" % (f, str(self.fields))
                parts.append(doc[f])
        else:
            parts = (doc,)
            
        return self.dump_key(parts)
        
        
    def dump_key(self, parts):
        return ''.join(map(dson.dumpone, parts))
        
        
        
class IndexCursor(cursor.Cursor):
    
    def __init__(self, index, reverse=False):
        super(IndexCursor, self).__init__(index.dbm, index.name, reverse)
        self.index = index
        
        
    def dump_key(self, key):
        return self.index.get_key(key)
        
        
    def dump_prefix(self, prefix):
        parts = []
        for f in self.index.fields:
            if f not in prefix:
                break
            parts.append(prefix[f])
            
        assert len(parts) > 0, "Prefix is missing indexed fields or has out-of-order fields"
        return self.index.dump_key(parts)
        
        
    def load(self, data):
        doc_value = self.index.dbm.get(self.index.table_name, data)
        if doc_value:
            return dson.loads(doc_value)


def make_key(base_key, extension_fields, key=None):
    if key is None:
        key = {}
    if isinstance(key, dict):
        key.update(dict(base_key))
    else:
        if len(extension_fields) > 0:
            key = {
                extension_fields[0]: key
            }
            key.update(base_key)
        else:
            key = dict(base_key)
    return key


class BaseKeyIndexCollectionProxy(object):
    
    def __init__(self, index_collection, base_key):
        self._index_collection = index_collection
        self._base_key = base_key
        
        
    def add(self, *fields):
        complete_fields = self.get_complete_fields(fields)
        return self._index_collection.add(*complete_fields)
        
        
    def get(self, fields=tuple()):
        if not isinstance(fields,tuple):
            fields = (fields,)
        index = self._index_collection[self.get_complete_fields(fields)]
        return BaseKeyIndexProxy(index, self._base_key, fields)
        
    def __getitem__(self, fields):
        return self.get(fields)
        
        
    def __contains__(self, fields):
        return self.get_complete_fields(fields) in self._index_collection
        
        
    def __iter__(self):
        for fields in self._index_collection:
            yield fields[len(self._base_key):]
        
        
    def get_complete_fields(self, fields):
        base_fields = tuple([x[0] for x in self._base_key])
        if not isinstance(fields, tuple):
            fields = (fields,)
        return base_fields + fields
        
        
class BaseKeyIndexProxy(object):
    
    def __init__(self, index, base_key, extension_fields):
        self.index = index
        self.base_key = base_key
        self.extension_fields = extension_fields
        self.make_key = functools.partial(make_key, base_key, extension_fields)
        
        
    def get(self, key):
        return self.index.get(
            self.make_key(key)
        )
        
        
    def all(self, key):
        return self.index.all(
            self.make_key(key)
        )
        
        
    def count(self):
        return self.index.cursor().count_prefix(self.make_key())
        
        
    def cursor(self, reverse=False):
        return BaseKeyIndexCursorProxy(self.index.cursor(reverse=reverse), self.base_key, self.extension_fields)
            
            
class BaseKeyIndexCursorProxy(object):
    
    def __init__(self, cursor, base_key, extension_fields):
        self.cursor = cursor
        self.make_key = functools.partial(make_key, base_key, extension_fields)
        
        
    def first(self):
        try:
            return self.cursor.prefix(self.make_key()).next()
        except StopIteration:
            pass
            
            
    def last(self):
        try:
            old_reverse = self.cursor.get_reverse()
            self.cursor.set_reverse(not old_reverse)
            result = self.cursor.prefix(self.make_key()).next()
            self.cursor.set_reverse(old_reverse)
            return result
        except StopIteration:
            pass
        
        
    def __iter__(self):
        return self.cursor.prefix(self.make_key())
        
        
    def range(self, start=None, end=None):
        start, end = [self.make_key(k) if k is not None else None for k in (start, end)]
        return self.cursor(start, end)
        
        
    def prefix(self, key):
        return self.cursor.prefix(self.make_key(key))
        
        
    def key(self, key):
        return self.cursor.key(self.make_key(key))
        
        
    def count_prefix(self, key):
        return self.cursor.count_prefix(self.make_key(key))
        
        
    def count_key(self, key):
        return self.cursor.count_key(self.make_key(key))
        