import unittest
import os.path
import shutil
from handbag import environment
from handbag.validators import *

TEST_PATH = "/tmp/handbag-test.db"
TEST_URL = "lmdb://%s" % TEST_PATH


class TestModel(unittest.TestCase):
    
    def setUp(self):
        if os.path.exists(TEST_PATH):
            shutil.rmtree(TEST_PATH)
        self.env = environment.open(TEST_URL)
        
        
    def test_retrieve(self):
        class Foo(self.env.Model):
            name = Text()
        
        with self.env.write():
            foo = Foo(name="I'm boring today")
            foo_id = foo.id
        
        with self.env.read():
            foo = Foo.get(foo_id)
            self.assertEquals(foo.name, "I'm boring today")
            
            
    def test_modify(self):
        class Foo(self.env.Model):
            name = Text()
        
        with self.env.write():
            foo = Foo(name="I'm boring today")
            foo_id = foo.id
        
        with self.env.read():
            foo = Foo.get(foo_id)
            self.assertEquals(foo.name, "I'm boring today")
            
        with self.env.write():
            foo = Foo.get(foo_id)
            foo.name = 'Excitement Pants!'
            
        with self.env.read():
            foo = Foo.get(foo_id)
            self.assertEquals(foo.name, 'Excitement Pants!')
            
            
    def test_remove(self):
        class Foo(self.env.Model):
            name = Text()
            
        with self.env.write():
            foo = Foo(name="Snu Fu")
            foo_id = foo.id
        
        with self.env.read():
            foo = Foo.get(foo_id)
            self.assertEquals(foo.name, "Snu Fu")
        
        with self.env.write():
            foo = Foo.get(foo_id)
            foo.remove()
            
        with self.env.read():
            foo = Foo.get(foo_id)
            self.assertEquals(foo, None)
            
        
    def test_abort_on_fail(self):
        class Foo(self.env.Model):
            name = Text()
        
        foo_id = None
        with self.assertRaises(InvalidGroupError):
            with self.env.write():
                foo = Foo()
                foo_id = foo.id
        
        with self.env.read():
            self.assertEquals(Foo.get(foo_id), None)
        
        
    def test_read_fail_outside_txn(self):
        class Foo(self.env.Model):
            pass
        
        with self.env.write():
            foo = Foo()
            foo_id = foo.id
        
        with self.assertRaises(AssertionError):
            foo = Foo.get(foo_id)
        
            
    def test_write_fail_outside_txn(self):
        class Foo(self.env.Model):
            name = Text()
            
        with self.assertRaises(AssertionError):
            foo = Foo(name='blah')
        
        with self.assertRaises(AssertionError):
            with self.env.read():
                foo = Foo(name='blah')
        
        with self.env.write():
            foo = Foo(name="Snark Jones")
            foo_id = foo.id
        
        with self.env.read():
            foo = Foo.get(foo_id)
            with self.assertRaises(AssertionError):
                foo.name = "Armchair Cowboy"
            
            
    def test_iterate(self):
        class Foo(self.env.Model):
            name = Text()
        
        names = []
        with self.env.write():
            for i in range(0,10):
                name = "Foo #%s" % i
                names.append(name)
                foo = Foo(name=name)
        
        fetched_names = []
        with self.env.read():
            for f in Foo.cursor():
                fetched_names.append(f.name)
                
        self.assertEquals(fetched_names, names)
        
        
    def test_iterate_backwards(self):
        class Foo(self.env.Model):
            name = Text()
            
        names = []
        with self.env.write():
            for i in range(0,10):
                name = "Foo #%s" % i
                names.append(name)
                foo = Foo(name=name)
        
        fetched_names = []
        with self.env.read():
            for f in Foo.cursor(reverse=True):
                fetched_names.append(f.name)
                
        self.assertEquals(fetched_names, list(reversed(names)))
        
        
    def test_range(self):
        class Foo(self.env.Model):
            pass
        
        ids = []
        with self.env.write():
            for i in range(0,10):
                foo = Foo()
                ids.append(foo.id)
        
        fetched_ids = []
        with self.env.read():
            for f in Foo.cursor().range(ids[5]):
                fetched_ids.append(f.id)
        
        self.assertEquals(fetched_ids, ids[5:])
        
        
    def test_range_end(self):
        class Foo(self.env.Model):
            pass
        
        ids = []
        with self.env.write():
            for i in range(0,10):
                foo = Foo()
                ids.append(foo.id)
        
        fetched_ids = []
        with self.env.read():
            for f in Foo.cursor().range(ids[5], ids[7]):
                fetched_ids.append(f.id)
        
        self.assertEquals(fetched_ids, ids[5:7])
        
        
    def test_range_backwards(self):
        class Foo(self.env.Model):
            pass
        
        ids = []
        with self.env.write():
            for i in range(0,10):
                foo = Foo()
                ids.append(foo.id)
        
        fetched_ids = []
        with self.env.read():
            for f in Foo.cursor(reverse=True).range(ids[5], ids[7]):
                fetched_ids.append(f.id)
        
        self.assertEquals(fetched_ids, list(reversed(ids[5:7])))
        
        
    def test_count(self):
        class Foo(self.env.Model):
            pass
            
        with self.env.write():
            for i in range(0,10):
                foo = Foo()
        
        with self.env.read():
            self.assertEquals(Foo.count(), 10)
            