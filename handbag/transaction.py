class Transaction(object):
    
    def __init__(self, dbm, write=False):
        self.dbm = dbm
        self.write = write
        
        
    def __enter__(self):
        self.dbm_txn = self.dbm.transaction(write=self.write)
        return self
        
        
    def __exit__(self, type, value, traceback):
        if value:
            self.dbm_txn.abort()
        else:
            self.dbm_txn.commit()