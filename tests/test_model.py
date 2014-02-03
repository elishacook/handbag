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
            
            
    def test_simple_inheritance(self):
        class Foo(self.env.Model):
            pass
            
        class Bar(Foo):
            pass
        
        with self.env.write():
            for i in range(0,10):
                f = Foo()
            for i in range(0,7):
                b = Bar()
        
        with self.env.read():
            self.assertEquals(Foo.count(), 17)
            self.assertEquals(Bar.count(), 7)
            
            class_names = set()
            for inst in Foo.cursor():
                class_names.add(inst.__class__.__name__)
            self.assertEquals(len(class_names), 2)
            
            for b in Bar.cursor():
                self.assertEquals(b.__class__.__name__, 'Bar')
                
                
    def test_inherited_indexes(self):
        class Foo(self.env.Model):
            skidoo = TypeOf(int)
            indexes = ['skidoo']
            
        class Bar(Foo):
            pass
            
        with self.env.write():
            for i in range(0,7):
                f = Foo(skidoo=i)
            for i in range(7,10):
                b = Bar(skidoo=i)
                
        with self.env.read():
            self.assertEquals(Foo.indexes['skidoo'].count(), 10)
            self.assertEquals(Bar.indexes['skidoo'].count(), 3)
            f = Foo.indexes['skidoo'].get(3)
            self.assertEquals(f.__class__.__name__, 'Foo')
            b = Foo.indexes['skidoo'].get(7)
            self.assertEquals(b.__class__.__name__, 'Bar')
            b = Bar.indexes['skidoo'].get(3)
            self.assertEquals(b, None)
            
            
    def test_multiple_descendants(self):
        class Foo(self.env.Model):
            skidoo = TypeOf(int)
            indexes = ['skidoo']
            
        class Bar(Foo):
            pass
            
        class Baz(Foo):
            pass
            
            
        with self.env.write():
            for i in range(0,5):
                f = Foo(skidoo=i)
            for i in range(10,13):
                b = Bar(skidoo=i)
            for i in range(20,27):
                b = Baz(skidoo=i)
        
        with self.env.read():
            self.assertEquals(Foo.count(), 15)
            x = Foo.indexes['skidoo'].get(3)
            self.assertEquals(x.__class__.__name__, 'Foo')
            x = Foo.indexes['skidoo'].get(11)
            self.assertEquals(x.__class__.__name__, 'Bar')
            x = Foo.indexes['skidoo'].get(23)
            self.assertEquals(x.__class__.__name__, 'Baz')
            