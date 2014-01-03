import threading
import dbm
from table import Table


def open(url):
    return Database(dbm.open(url))


class Database(object):
    
    def __init__(self, dbm):
        self.dbm = dbm
        self.tables = {}
        
        
    def read(self):
        return DatabaseContext(self.dbm)
        
        
    def write(self):
        return DatabaseContext(self.dbm, writable=True)
        
        
    def __getattr__(self, name):
        return self.get_table(name)
        
        
    def __getitem__(self, name):
        return self.get_table(name)
        
        
    def get_table(self, name):
        if name not in self.tables:
            self.tables[name] = Table(self.dbm, name)
        return self.tables[name]
        
        
        
class DatabaseContext(object):
    
    def __init__(self, dbm, writable=False):
        self.dbm = dbm
        self.writable = writable
        
        
    def __enter__(self):
        self.dbm.transaction_start(writable=self.writable)
        
        
    def __exit__(self, type, value, exception):
        if value:
            self.dbm.transaction_abort()
        else:
            self.dbm.transaction_commit()
            