class AbstractDBM(object):
    
    def __init__(self, path):
        raise NotImplementedError
        
    
    def add_namespace(self, namespace, duplicate_keys=False):
        raise NotImplementedError
        
    
    def transaction_start(self, writable=False):
        raise NotImplementedError
        
        
    def transaction_commit(self):
        raise NotImplementedError
        
        
    def transaction_abort(self):
        raise NotImplementedError
        
        
    def in_transaction(self):
        raise NotImplementedError
        
        
    def is_transaction_writable(self):
        raise NotImplementedError
        
        
    def put(self, namespace, key, value):
        raise NotImplementedError
        
        
    def delete(self, namespace, key, value=None):
        raise NotImplementedError
        
        
    def delete_all(self, namespace):
        raise NotImplementedError
        
        
    def get(self, namespace, key):
        raise NotImplementedError
        
        
    def cursor(self, namespace):
        raise NotImplementedError
        
        
    def count(self, namespace):
        raise NotImplementedError
        
        
    def close(self):
        raise NotImplementedError
        
        
        
class AbstractDBMCursor(object):
    
    def first(self):
        raise NotImplementedError
        
        
    def last(self):
        raise NotImplementedError
        
        
    def next(self):
        raise NotImplementedError
        
        
    def prev(self):
        raise NotImplementedError
        
        
    def jump(self, key):
        raise NotImplementedError
        
        
    def key(self):
        raise NotImplementedError
        
        
    def value(self):
        raise NotImplementedError
        
        
    def iternext(self):
        raise NotImplementedError
        
        
    def iterprev(self):
        raise NotImplementedError
        