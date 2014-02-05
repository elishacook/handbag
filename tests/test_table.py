import unittest
import os.path
import shutil
import threading
from handbag import database

TEST_PATH = "/tmp/handbag-test.db"
TEST_URL = "lmdb://%s" % TEST_PATH


class TestTable(unittest.TestCase):
    
    def setUp(self):
        if os.path.exists(TEST_PATH):
            shutil.rmtree(TEST_PATH)
        self.db = database.open(TEST_URL)
        
        
    def test_add(self):
        foos = self.db.foos
        
        with self.db.write():
            foo = foos.save({'skidoo':23})
        
        with self.db.read():
            foo = foos.get(foo['id'])
            self.assertEqual(foo['skidoo'], 23)
        
        
    def test_update(self):
        foos = self.db.foos
        
        with self.db.write():
            foo = foos.save({'skidoo':23})
        
        with self.db.read():
            foo = foos.get(foo['id'])
            self.assertEqual(foo['skidoo'], 23)
            
        with self.db.write():
            foo['skidoo'] = 24
            foos.save(foo)
            
        with self.db.read():
            foo = foos.get(foo['id'])
            self.assertEqual(foo['skidoo'], 24)
        
        
    def test_remove(self):
        foos = self.db.foos
        
        with self.db.write():
            foo = foos.save({'skidoo':23})
        
        with self.db.read():
            foo = foos.get(foo['id'])
            self.assertEqual(foo['skidoo'], 23)
            
        with self.db.write():
            foos.remove(foo['id'])
        
        with self.db.read():
            foo = foos.get(foo['id'])
            self.assertEqual(foo, None)
        
        
    def test_count(self):
        foos = self.db.foos
        
        with self.db.write():
            foo = foos.save({'skidoo':23})
            
        with self.db.read():
            self.assertEqual(foos.count(), 1)
            
        with self.db.write():
            for i in range(0,10):
                foos.save({'skidoo': i * 23})
        
        with self.db.read():
            self.assertEqual(foos.count(), 11)
        
        
    def test_table_threads(self):
        foos = self.db.foos
        
        def add_one():
            with self.db.write():
                foos.save({'foo':'bar'})
        
        t1 = threading.Thread(target=add_one)
        t2 = threading.Thread(target=add_one)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        with self.db.read():
            self.assertEqual(foos.count(), 2)