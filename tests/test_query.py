import unittest
import os.path
import shutil
import string
from handbag import database

TEST_PATH = "/tmp/handbag-test.db"
TEST_URL = "lmdb://%s" % TEST_PATH


class TestQuery(unittest.TestCase):
    
    def setUp(self):
        if os.path.exists(TEST_PATH):
            shutil.rmtree(TEST_PATH)
        self.db = database.open(TEST_URL)
        
        
    def test_equal(self):
        foos = self.db.foos
        
        with self.db.write():
            for i in range(0,10):
                foos.save(dict(name="Foo#%d" % i, things=[i, i*100, i*1000]))
        
        with self.db.read():
            foo = foos.find_one({'name':'Foo#3'})
            self.assertEqual(foo['name'], 'Foo#3')
            foo = foos.find_one({'name':'Zoo'})
            self.assertEqual(foo, None)
            foo = foos.find_one({'things':3})
            self.assertEqual(foo['name'], 'Foo#3')
            
            
    def test_not_equal(self):
        foos = self.db.foos
            
        with self.db.write():
            for i in range(0,10):
                foos.save(dict(name="Foo#%d" % i))
        
        with self.db.read():
            results = foos.find({'name':{'$ne':'Foo#3'}})
            self.assertEqual(results.count(), 9)
            results = foos.find({'name':{'$ne':'Zoo'}})
            self.assertEqual(results.count(), 10)
            
            
    def test_less_than(self):
        foos = self.db.foos
            
        with self.db.write():
            for i in range(0,10):
                foos.save({'value':i})
        
        with self.db.read():
            results = foos.find({'value':{'$lt':5}})
            self.assertEqual(results.count(), 5)
            results = foos.find({'value':{'$lte':5}})
            self.assertEqual(results.count(), 6)
            results = foos.find({'value':{'$lt':0}})
            self.assertEqual(results.count(), 0)
            results = foos.find({'value':{'$lte':0}})
            self.assertEqual(results.count(), 1)
            
            
    def test_greater_than(self):
        foos = self.db.foos
            
        with self.db.write():
            for i in range(0,10):
                foos.save({'value':i})
        
        with self.db.read():
            results = foos.find({'value':{'$gt':5}})
            self.assertEqual(results.count(), 4)
            results = foos.find({'value':{'$gte':5}})
            self.assertEqual(results.count(), 5)
            results = foos.find({'value':{'$gt':0}})
            self.assertEqual(results.count(), 9)
            results = foos.find({'value':{'$gte':7}})
            self.assertEqual(results.count(), 3)
            
            
    def test_in(self):
        foos = self.db.foos
            
        with self.db.write():
            for i in range(0,10):
                foos.save(dict(name="Foo#%d" % i))
        
        with self.db.read():
            results = foos.find({'name':{'$in':['Foo#6','Foo#2']}})
            self.assertEqual(results.count(), 2)
            results = foos.find({'name':{'$nin':['Foo#6','Foo#2']}})
            self.assertEqual(results.count(), 8)
            
            
    def test_all(self):
        foos = self.db.foos
            
        with self.db.write():
            for i in range(0,10):
                foos.save(dict(name="Foo#%d" % i, things=[i,i*100,i*1000,i*10000]))
        
        with self.db.read():
            foo = foos.find_one({'things':{'$all':[2,200,20000]}})
            self.assertEqual(foo['name'], 'Foo#2')
            with self.assertRaises(AssertionError):
                foo = foos.find_one({'things':{'$all':2}})
                
                
    def test_regex(self):
        foos = self.db.foos
            
        with self.db.write():
            for i in range(0,10):
                foos.save(dict(name="Foo#%d" % i))
        
        with self.db.read():
            results = foos.find({'name':{'$regex':'oo#3'}})
            self.assertEqual(results.count(), 1)
            self.assertEqual(results.next()['name'], 'Foo#3')
            results = foos.find({'name':{'$regex':'oo#'}})
            self.assertEqual(results.count(), 10)
            results = foos.find({'name':{'$regex':'4$'}})
            self.assertEqual(results.count(), 1)
            self.assertEqual(results.next()['name'], 'Foo#4')
            self.assertEqual(results.count(), 1)
            results = foos.find({'name':{'$regex':'oo#(3|7)'}})
            self.assertEqual(results.count(), 2)
            
            
    def test_and(self):
        foos = self.db.foos
            
        with self.db.write():
            for i in range(0,10):
                foos.save(dict(name="Foo#%d" % i, value=i*100))
        
        with self.db.read():
            results = foos.find({'$and':[{'name':'Foo#5'}, {'value':500}]})
            self.assertEqual(results.count(), 1)
            results = foos.find({'$and':[{'name':'Foo#5'}, {'value':700}]})
            self.assertEqual(results.count(), 0)
            
            
    def test_or(self):
        foos = self.db.foos
            
        with self.db.write():
            for i in range(0,10):
                foos.save(dict(name="Foo#%d" % i, value=i*100))
        
        with self.db.read():
            results = foos.find({'$or':[{'name':'Foo#5'}, {'value':500}]})
            self.assertEqual(results.count(), 1)
            results = foos.find({'$or':[{'name':'Foo#5'}, {'value':700}]})
            self.assertEqual(results.count(), 2)
            
            
    def test_nor(self):
        foos = self.db.foos
            
        with self.db.write():
            for i in range(0,10):
                foos.save(dict(name="Foo#%d" % i, value=i*100))
        
        with self.db.read():
            results = foos.find({'$nor':[{'name':'Foo#5'}, {'value':500}]})
            self.assertEqual(results.count(), 9)
            results = foos.find({'$nor':[{'name':'Foo#5'}, {'value':700}]})
            self.assertEqual(results.count(), 8)
            
            
    def test_not(self):
        foos = self.db.foos
            
        with self.db.write():
            for i in range(0,10):
                foos.save(dict(name="Foo#%d" % i))
        
        with self.db.read():
            results = foos.find({'$not':{'name':'Foo#3'}})
            self.assertEqual(results.count(), 9)
            results = foos.find({'$not':{'name':'Zoo'}})
            self.assertEqual(results.count(), 10)
            