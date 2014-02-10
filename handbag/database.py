import threading
import dbm
from table import Table


def open(url):
    return Database(dbm.open(url))


class Database(object):
    
    def __init__(self, dbm):
        self.dbm = dbm
        self.tables = {}
        self.indexes_synced = False
        
        
    def read(self):
        self.ensure_indexes_synced()
        return DatabaseContext(self.dbm)
        
        
    def write(self):
        self.ensure_indexes_synced()
        return DatabaseContext(self.dbm, writable=True)
        
        
    def __getattr__(self, name):
        return self.get_table(name)
        
        
    def __getitem__(self, name):
        return self.get_table(name)
        
        
    def get_table(self, name):
        if name not in self.tables:
            self.tables[name] = Table(self.dbm, name)
        return self.tables[name]
        
        
    def close(self):
        self.dbm.close()
        
        
    def ensure_indexes_synced(self):
        if self.indexes_synced:
            return
        self.indexes_synced = True
        
        for table in self.tables.values():
            table.indexes.sync()
        
        
class DatabaseContext(object):
    
    def __init__(self, dbm, writable=False):
        self.dbm = dbm
        self.writable = writable
        self.is_dummy = False
        
        
    def __enter__(self):
        if self.dbm.in_transaction() and not self.dbm.is_transaction_writable():
            self.is_dummy = True
            return
        self.dbm.transaction_start(writable=self.writable)
        
        
    def __exit__(self, type, value, exception):
        if self.is_dummy:
            return
        if value:
            self.dbm.transaction_abort()
        else:
            self.dbm.transaction_commit()
            