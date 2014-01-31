import unittest
import os.path
import shutil
import string
from handbag import database

TEST_PATH = "/tmp/handbag-test.db"
TEST_URL = "lmdb://%s" % TEST_PATH


class TestIndex(unittest.TestCase):
    
    def setUp(self):
        if os.path.exists(TEST_PATH):
            shutil.rmtree(TEST_PATH)
        self.db = database.open(TEST_URL)
                
                
    def test_create(self):
        foos = self.db.foos
        foos.indexes.add('skidoo')
        
        with self.db.write():
            for i in range(0,20):
                foos.save({'skidoo':i,'foo':'bar'})
        
        with self.db.read():
            result = foos.indexes['skidoo'].get({'skidoo':5})
            self.assertEqual(result['skidoo'], 5)
            
            
    def test_remove(self):
        foos = self.db.foos
        foos.indexes.add('skidoo')
        
        with self.db.write():
            for i in range(0,20):
                foos.save({'skidoo':i,'foo':'bar'})
        
        with self.db.read():
            result = foos.indexes['skidoo'].get({'skidoo':5})
            self.assertEqual(result['skidoo'], 5)
            
        with self.db.write():
            foos.remove(result['id'])
            
        with self.db.read():
            result = foos.indexes['skidoo'].get({'skidoo':5})
            self.assertEqual(result, None)
            
            
    def test_dupes(self):
        foos = self.db.foos
        foos.indexes.add('skidoo')
        
        with self.db.write():
            foos.save({'skidoo':23,'foo':'bar'})
            foos.save({'skidoo':23,'foo':'baz'})
        
        with self.db.read():
            result = foos.indexes['skidoo'].get({'skidoo':23})
            self.assertEqual(result['foo'], 'bar')
            
            results = list(foos.indexes['skidoo'].all({'skidoo':23}))
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0]['foo'], 'bar')
            self.assertEqual(results[1]['foo'], 'baz')
            
        with self.db.write():
            foos.remove(results[0]['id'])
            
        with self.db.read():
            results = list(foos.indexes['skidoo'].all({'skidoo':23}))
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]['foo'], 'baz')
            
            
    def test_list(self):
        foos = self.db.foos
        foos.indexes.add('things')
        
        with self.db.write():
            foos.save({'things':[1,2,3]})
            foos.save({'things':['a','b','c']})
            
            
        with self.db.read():
            result = foos.indexes['things'].get({'things':[1,2,3]})
            self.assertEqual(result['things'], [1,2,3])
            result = foos.indexes['things'].get({'things':2})
            self.assertEqual(result['things'], [1,2,3])
            
            
    def test_dict(self):
        foos = self.db.foos
        foos.indexes.add('thing.name')
        
        with self.db.write():
            foos.save({'thing':{'name':'foo'}, 'skidoo':23})
            foos.save({'thing':{'name':'bar'}, 'skidoo':24})
            
        with self.db.read():
            result = foos.indexes['thing.name'].get({'thing.name':'bar'})
            self.assertEqual(result['skidoo'], 24)
            result = foos.indexes['thing.name'].get({'thing.name':'foo'})
            self.assertEqual(result['skidoo'], 23)
            
            
    def test_cursor(self):
        foos = self.db.foos
        foos.indexes.add('skidoo')
        foo_list = []
        
        with self.db.write():
            for i in range(0,20):
                foo = foos.save({'skidoo':i,'foo':'bar'})
                foo_list.append(foo)
                
        with self.db.read():
            cur = foos.indexes['skidoo'].cursor()
            results = list(cur.range({'skidoo':5}, {'skidoo':13}))
            self.assertEqual(results, foo_list[5:13])
            
            
    def test_count(self):
        foos = self.db.foos
        foos.indexes.add('skidoo')
        
        with self.db.write():
            for i in range(0,20):
                foos.save({'skidoo':i})
                
        with self.db.read():
            self.assertEqual(foos.indexes['skidoo'].count(), 20)
            
            
    def test_sync(self):
        foos = self.db.foos
        foos.indexes.add('skidoo')
        
        with self.db.write():
            for i in range(0,20):
                foos.save({'skidoo':i})
        
        self.db.close()
        self.db = database.open(TEST_URL)
        foos = self.db.foos
        foos.indexes.add('skidoo')
        
        with self.db.read():
            self.assertEqual(foos.indexes['skidoo'].count(), 20)
            
            
    def test_fn_keys(self):
        def gen_keys(doc):
            if 'skidoo' in doc:
                return [doc['skidoo'] * 100]
            else:
                return []
            
        foos = self.db.foos
        foos.indexes.add(('times-one-hundred', gen_keys))
        
        with self.db.write():
            for i in range(1,21):
                foos.save({'skidoo':i})
        
        with self.db.read():
            doc = foos.indexes['times-one-hundred'].get({'times-one-hundred':300})
            self.assertEqual(doc['skidoo'], 3)
                    