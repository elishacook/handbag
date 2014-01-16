import dson
import cursor
import uniqueid
import index


class Table(object):
    
    def __init__(self, dbm, name):
        self.dbm = dbm
        self.name = name
        self.dbm.add_namespace(name)
        self.indexes = index.IndexCollection(self.dbm, self.name)
        
        
    def save(self, doc):
        assert self.dbm.is_transaction_writable(), "Transaction is read-only"
        if 'id' in doc:
            old_doc = self.get(doc['id'])
        else:
            doc['id'] = uniqueid.create()
            old_doc = None
        key = dson.dumpone(doc['id'])
        value = dson.dumps(doc)
        self.dbm.put(self.name, key, value)
        self.indexes.update(old_doc, doc)
        return doc
        
        
    def remove(self, id):
        assert self.dbm.is_transaction_writable(), "Transaction is read-only"
        doc = self.get(id)
        self.indexes.remove(doc)
        key = dson.dumpone(id)
        self.dbm.delete(self.name, key)
        
        
    def get(self, id):
        key = dson.dumpone(id)
        value = self.dbm.get(self.name, key)
        
        if value is None:
            return None
        else:
            return dson.loads(value)
            
            
    def count(self):
        return self.dbm.count(self.name)
        
            
    def cursor(self, reverse=False):
        return cursor.Cursor(self.dbm, self.name, reverse=reverse)
        