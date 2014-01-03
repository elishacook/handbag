import itertools
import cursor
import dson

class IndexCollection(object):
    
    def __init__(self, dbm, name):
        self.dbm = dbm
        self.name = name
        self.indexes = {}
        
        
    def add(self, *fields, **kwargs):
        assert fields not in self.indexes, "Attempting to redefine index %s" % str(fields)
        self.indexes[fields] = Index(self.dbm, self.name, fields, kwargs.get('unique', False))
        
        
    def __getitem__(self, fields):
        if not isinstance(fields, tuple):
            fields = (fields,)
        index = self.indexes.get(fields)
        if not index:
            raise KeyError, fields
        return index
        
        
    def __iter__(self):
        for k in self.indexes:
            yield k
        
        
    def update(self, old_doc, new_doc):
        for index in self.indexes.values():
            index.update(old_doc, new_doc)
            
            
    def remove(self, doc):
        for index in self.indexes.values():
            index.remove(doc)
        
        
    
class Index(object):
    
    def __init__(self, dbm, table_name, fields, unique=False):
        self.dbm = dbm
        self.table_name = table_name
        self.fields = fields
        self.name = '%s-%s' % (self.table_name, '_'.join(fields))
        self.dbm.add_namespace(self.name, duplicate_keys=(not unique))
        
        
    def update(self, old_doc, new_doc):
        if old_doc == new_doc:
            return
        new_keys = self.make_keys(new_doc)
        if old_doc:
            old_keys = self.make_keys(old_doc)
            if old_keys == new_keys:
                return
            for k in old_keys:
                self.dbm.delete(self.name, k)
        value = dson.dumpone(new_doc['id'])
        for k in new_keys:
            self.dbm.put(self.name, k, value)
        
        
    def get(self, key, duplicates=False):
        string_key = self.get_key(key)
        if duplicates:
            cur = self.dbm.cursor(self.name)
            cur.jump(string_key)
            docs = []
            while cur.key() == string_key:
                doc_key = cur.value()
                doc_value = self.dbm.get(self.table_name, doc_key)
                if doc_value:
                    docs.append(dson.loads(doc_value))
                cur.next()
            return docs
        else:
            value = self.dbm.get(self.name, string_key)
            if value:
                doc_value = self.dbm.get(self.table_name, value)
                if doc_value:
                    return dson.loads(doc_value)
        
        
    def remove(self, doc):
        key = self.get_key(doc)
        value = dson.dumpone(doc['id'])
        self.dbm.delete(self.name, key, value=value)
        
        
    def cursor(self, reverse=False):
        return IndexCursor(self, reverse)
        
        
    def make_keys(self, doc):
        rows = []
        for f in self.fields:
            value = self.get_value(doc, f)
            if isinstance(value, list):
                rows.append([value] + value)
            else:
                rows.append([value])
        keys = []
        for row in itertools.product(*rows):
            key = ''.join(map(dson.dumpone, row))
            keys.append(key)
        return keys
        
        
    def get_value(self, doc, field):
        if '.' in field:
            parts = field.split('.')
            child_doc = doc.get(parts[0])
            if not child_doc:
                return None
            return self.get_value(child_doc, '.'.join(parts[1:]))
        else:
            return doc.get(field)
        
        
    def get_key(self, doc):
        key = ''
        for f in self.fields:
            assert f in doc, "Missing value for field '%s' in index %s" % (f, str(self.fields))
            key += dson.dumpone(doc[f])
        return key
        
        
        
class IndexCursor(cursor.Cursor):
    
    def __init__(self, index, reverse=False):
        super(IndexCursor, self).__init__(index.dbm, index.name, reverse)
        self.index = index
        
        
    def dump_key(self, key):
        return self.index.get_key(key)
        
        
    def load(self, data):
        doc_value = self.index.dbm.get(self.index.table_name, data)
        if doc_value:
            return dson.loads(doc_value)
            