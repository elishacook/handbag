import lmdb
import threading
from abstract import AbstractDBM, AbstractDBMCursor

class LMDBDBM(AbstractDBM):
    
    def __init__(self, url):
        self._path = url.path
        self._env = None
        self._dbs = {}
        self._local = threading.local()
        self._local.transactions = []
        
        
    def add_namespace(self, namespace, duplicate_keys=False):
        assert self._env is None, "Can't add a namespace after the environment has been opened"
        if namespace not in self._dbs:
            self._dbs[namespace] = {
                'duplicate_keys': duplicate_keys
            }
        
    
    def transaction_start(self, writable=False):
        if len(self._local.transactions) > 0:
            parent = self._local.transactions[-1][1]
        else:
            parent = None
        txn = lmdb.Transaction(self._get_env(), write=writable, parent=parent)
        self._local.transactions.append((writable, txn))
        return txn
        
        
    def transaction_commit(self):
        self._require_transaction()
        txn = self._local.transactions.pop()[1]
        txn.commit()
        
        
    def transaction_abort(self):
        self._require_transaction()
        txn = self._local.transactions.pop()[1]
        txn.abort()
        
        
    def is_transaction_writable(self):
        self._require_transaction()
        return self._local.transactions[-1][0]
        
        
    def put(self, namespace, key, value):
        db = self._dbs[namespace]
        txn = self._current_transaction()
        txn.put(key, value, db=db)
        
        
    def delete(self, namespace, key, value=None):
        if value is None:
            value = ''
        db = self._dbs[namespace]
        txn = self._current_transaction()
        txn.delete(key, value=value, db=db)
        
        
    def delete_all(self, namespace):
        db = self._dbs[namespace]
        txn = self._current_transaction()
        txn.drop(db, delete=False)
        
        
    def get(self, namespace, key):
        db = self._dbs[namespace]
        txn = self._current_transaction()
        value = txn.get(key, db=db)
        return value
        
        
    def cursor(self, namespace):
        db = self._dbs[namespace]
        txn = self._current_transaction()
        cur = txn.cursor(db=db)
        return LMDBCursor(cur)
        
        
    def count(self, namespace):
        db = self._dbs[namespace]
        txn = self._current_transaction()
        return txn.stat(db)['entries']
        
        
    def close(self):
        if self._env:
            self._env.close()
        
        
    def _current_transaction(self):
        self._require_transaction()
        return self._local.transactions[-1][1]
        
        
    def _require_transaction(self):
        assert len(self._local.transactions) > 0, "An active transaction is required"
        
        
    def _get_env(self):
        if not self._env:
            self._env = lmdb.Environment(self._path, subdir=True, map_size=2147483648, max_dbs=len(self._dbs))
            for name,options in self._dbs.items():
                self._dbs[name] = self._env.open_db(name, 
                    dupsort=options.get('duplicate_keys', False))
        return self._env
        
        
class LMDBCursor(AbstractDBMCursor):
    
    def __init__(self, dbm_cur):
        super(LMDBCursor, self).__setattr__('_dbm_cur', dbm_cur)
        
        
    def __getattribute__(self, name):
        if name == 'jump':
            return super(LMDBCursor, self).__getattribute__('_dbm_cur').set_range
        return getattr(super(LMDBCursor, self).__getattribute__('_dbm_cur'), name)
        