import dson
import cursor
import uniqueid
import index
from query import Query


class Table(object):
    
    def __init__(self, dbm, name):
        self.dbm = dbm
        self.name = name
        self.dbm.add_namespace(name)
        self.indexes = index.IndexCollection(self.dbm, self.name)
        
        
    def save(self, doc):
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
        
        
    def find_one(self, query):
        q = Query(self, query)
        try:
            return q.next()
        except StopIteration:
            return None
        
        
    def find(self, query):
        return Query(self, query)
        