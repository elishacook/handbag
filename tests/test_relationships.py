import unittest
import os.path
import shutil
from handbag import environment
from handbag.validators import *
from handbag.relationships import *

TEST_PATH = "/tmp/handbag-test.db"
TEST_URL = "lmdb://%s" % TEST_PATH


class TestRelationships(unittest.TestCase):
    
    def setUp(self):
        if os.path.exists(TEST_PATH):
            shutil.rmtree(TEST_PATH)
        self.env = environment.open(TEST_URL)
    
    
    def test_one(self):
        class Foo(self.env.Model):
            bar = One("Bar")
            
        class Bar(self.env.Model):
            pass
        
        with self.env.write():
            foo = Foo()
            bar = Bar()
            foo.bar = bar
            
        with self.env.read():
            self.assertEquals(foo.bar, bar)
            foo = Foo.get(foo.id)
            self.assertEquals(foo.bar, bar)
            
            
    def test_one_instance_specific_target(self):
        class Foo(self.env.Model):
            bar = One("Bar")
            
        class Bar(self.env.Model):
            pass
            
        with self.env.write():
            foo1 = Foo()
            bar1 = Bar()
            foo1.bar = bar1
            foo2 = Foo()
            bar2 = Bar()
            foo2.bar = bar2
        
        with self.env.read():
            self.assertEquals(foo1.bar, bar1)
            self.assertEquals(foo2.bar, bar2)
        
      
    def test_one_no_inverse(self):
        with self.assertRaises(TypeError):
            class Foo(self.env.Model):
                bar = One("Bar", inverse="foo")
                
    
    def test_one_to_one(self):
        class Foo(self.env.Model):
            bar = OneToOne("Bar", inverse="foo")
            
        class Bar(self.env.Model):
            pass
        
        with self.env.write():
            bar = Bar()
            foo = Foo()
            foo.bar = bar
        
        with self.env.read():
            self.assertEquals(bar.foo, foo)
        
        
    def test_one_to_one_overdefined(self):
        class Foo(self.env.Model):
            bar = OneToOne("Bar", inverse="foo")
        
        class Bar(self.env.Model):
            foo = OneToOne(Foo, inverse="bar")
        
        with self.env.write():
            bar = Bar()
            foo = Foo()
            foo.bar = bar
        
        with self.env.read():
            self.assertEquals(bar.foo, foo)
        

    def test_one_to_one_cascade(self):
        class Foo(self.env.Model):
            bar = OneToOne("Bar", inverse="foo", cascade=True)
        
        class Bar(self.env.Model):
            pass
        
        with self.env.write():
            bar = Bar()
            bar2 = Bar()
            foo = Foo()
            foo.bar = bar
        
        with self.env.read():
            self.assertEquals(Bar.count(), 2)
            
        with self.env.write():
            foo.remove()
            
        with self.env.read():
            self.assertEquals(Bar.count(), 1)
            b = Bar.cursor().first()
            self.assertEquals(b, bar2)
        
        
    def test_one_to_many(self):
        data = {
            'San Francisco': [
                'Alchemist', 'Bar Tartine', 'Lounge 3411', 'Tempest'
            ],
            'New York': [
                'Dead Rabbit', 'Death & Co.', 'Donna', 'Proletariat'
            ]
        }
        
        class City(self.env.Model):
            name = Text()
            bars = OneToMany("Bar", inverse="city", indexes=['name'])
            indexes = ['name']
        
        class Bar(self.env.Model):
            name = Text()
            indexes = ['name']
        
        with self.env.write():
            for city, bars in data.items():
                c = City(name=city)
                for bar in bars:
                    b = Bar(name=bar)
                    c.bars.add(b)
        
        with self.env.read():
            city = City.indexes['name'].get('San Francisco')
            self.assertEquals(city.bars.count(), 4)
            self.assertEquals([b.name for b in city.bars.indexes['name'].cursor()], data['San Francisco'])
            
            city = City.indexes['name'].get('New York')
            self.assertEquals(city.bars.count(), 4)
            self.assertEquals([b.name for b in city.bars.indexes['name'].cursor()], data['New York'])
            
            bar = Bar.indexes['name'].get('Donna')
            self.assertEquals(bar.city.name, 'New York')
            self.assertEquals(bar.city, city)
        
        with self.env.write():
            bar.remove()
            
        with self.env.read():
            self.assertEquals(city.bars.count(), 3)
            
            b = city.bars.first()
            name = b.name
            self.assertIn(name, data['New York'])
            
        with self.env.write():
            city.bars.remove(b)
            
        with self.env.read():
            self.assertEquals(city.bars.count(), 2)
            new_b = Bar.indexes['name'].get(name)
            self.assertEquals(b, new_b)
        
        
    def test_one_to_many_overdefined(self):
        data = {
            'San Francisco': [
                'Alchemist', 'Bar Tartine', 'Lounge 3411', 'Tempest'
            ],
            'New York': [
                'Dead Rabbit', 'Death & Co.', 'Donna', 'Proletariat'
            ]
        }
        
        class City(self.env.Model):
            name = Text()
            bars = OneToMany("Bar", inverse="city", indexes=['name'])
            indexes = ['name']
        
        class Bar(self.env.Model):
            name = Text()
            city = ManyToOne(City, inverse="bars")
            indexes = ['name']
        
        with self.env.write():
            for city, bars in data.items():
                c = City(name=city)
                for bar in bars:
                    b = Bar(name=bar)
                    c.bars.add(b)
        
        with self.env.read():
            city = City.indexes['name'].get('San Francisco')
            self.assertEquals(city.bars.count(), 4)
            self.assertEquals([b.name for b in city.bars.indexes['name'].cursor()], data['San Francisco'])
            
            city = City.indexes['name'].get('New York')
            self.assertEquals(city.bars.count(), 4)
            self.assertEquals([b.name for b in city.bars.indexes['name'].cursor()], data['New York'])
            
            bar = Bar.indexes['name'].get('Donna')
            self.assertEquals(bar.city.name, 'New York')
            self.assertEquals(bar.city, city)
        
        with self.env.write():
            bar.remove()
            
        with self.env.read():
            self.assertEquals(city.bars.count(), 3)
            
            b = city.bars.first()
            name = b.name
            self.assertIn(name, data['New York'])
            
        with self.env.write():
            city.bars.remove(b)
            
        with self.env.read():
            self.assertEquals(city.bars.count(), 2)
            new_b = Bar.indexes['name'].get(name)
            self.assertEquals(b, new_b)
        

    def test_one_to_many_cascade(self):
        class City(self.env.Model):
            name = Text()
            bars = OneToMany("Bar", inverse="city", cascade=True)
        
        class Bar(self.env.Model):
            name = Text()
            indexes = ['name']
        
        with self.env.write():
            city_a = City(name="Foo City")
            city_b = City(name="Qux City")
            
            for i in range(0,10):
                a = Bar(name="Bar a#%s" % str(i+1))
                city_a.bars.add(a)
                
                b = Bar(name="Bar b#%s" % str(i+1))
                city_b.bars.add(b)
        
        with self.env.read():
            self.assertEquals(Bar.count(), 20)
            self.assertEquals(city_a.bars.count(), 10)
            self.assertEquals(city_b.bars.count(), 10)
            
        with self.env.write():
            city_a.remove()
            
        with self.env.read():
            self.assertEquals(Bar.count(), 10)
            b = Bar.indexes['name'].cursor().first()
            self.assertEquals(b.name, "Bar b#1")
        
        
    def test_many_to_many(self):
        tags_by_doc = {
            "foo": ["tofu", "seitan"],
            "bar": ["seitan"],
            "baz": ["tempeh", "tofu"],
            "qux": ["seitan", "tofu"],
            "goo": ["seitan"]
        }
        docs_by_tag = {}
        
        class Document(self.env.Model):
            content = Text()
            tags = ManyToMany("Tag", inverse="documents", indexes=['name'])
            indexes = ['content']
            
        class Tag(self.env.Model):
            name = Text()
            documents = ManyToMany(Document, inverse="tags", indexes=['content'])
            indexes = ['name']
        
        tags = {}
        docs = {}
        
        with self.env.write():
            for content, tag_names in tags_by_doc.items():
                doc = Document(content=content)
                docs[content] = doc
                
                for n in tag_names:
                    if n not in tags:
                        tag = Tag(name=n)
                        tags[n] = tag
                    doc.tags.add(tags[n])
                    if n not in docs_by_tag:
                        docs_by_tag[n] = []
                    docs_by_tag[n].append(content)
        
        for d in [tags_by_doc, docs_by_tag]:
            for k in d:
                d[k].sort()
        
        with self.env.read():
            for k,v in tags_by_doc.items():
                doc = Document.indexes['content'].get(k)
                self.assertEquals([tag.name for tag in doc.tags.indexes['name'].cursor()], v)
                
            for k,v in docs_by_tag.items():
                tag = Tag.indexes['name'].get(k)
                self.assertEquals([doc.content for doc in tag.documents.indexes['content'].cursor()], v)
        
            tag = Tag.indexes['name'].get('seitan')
            self.assertEquals(tag.documents.count(), 4)
            doc = tag.documents.first()
            self.assertIn('seitan', [tag.name for tag in doc.tags])
            num_doc_tags = doc.tags.count()
            
        with self.env.write():
            tag.documents.remove(doc)
            
        with self.env.read():
            self.assertEquals(tag.documents.count(), 3)
            self.assertEquals(doc.tags.count(), num_doc_tags-1)
            
            doc = Document.indexes['content'].get('baz')
            self.assertEquals(doc.tags.indexes['name'].cursor().count_key('tofu'), 1)
        

    def test_many_to_many_cascade(self):
        class Foo(self.env.Model):
            bars = ManyToMany("Bar", inverse="foos", cascade=True)
            
        class Bar(self.env.Model):
            pass
        
        foos = []
        
        with self.env.write():
            for i in range(0,3):
                f = Foo()
                for i in range(0,10):
                    b = Bar()
                    f.bars.add(b)
                foos.append(f)
        
        with self.env.read():
            self.assertEquals(Bar.count(), 30)
        
            for foo in foos:
                self.assertEquals(foo.bars.count(), 10)
        
        with self.env.write():
            foos[0].remove()
        
        with self.env.read():
            self.assertEquals(Bar.count(), 20)
            
            for foo in foos[1:]:
                self.assertEquals(foo.bars.count(), 10)

    def test_redefine_backreference_fails(self):
        class Foo(self.env.Model):
            pass
            
        class Bar(self.env.Model):
            foos = ManyToOne(Foo, inverse="bars")
        
        with self.assertRaises(AssertionError):
            class Baz(self.env.Model):
                foos = ManyToOne(Foo, inverse="bars")
        
        class Qux(self.env.Model):
            qugs = OneToMany("Qug", inverse="zzz")
        
        with self.assertRaises(AssertionError):    
            class Quv(self.env.Model):
                qugs = OneToMany("Qug", inverse="zzz")
                
                
    def test_invalid_inverse_fails(self):
        class Foo(self.env.Model):
            bar = OneToOne("Bar", inverse="foo")
        
        with self.assertRaises(TypeError):
            class Bar(self.env.Model):
                foo = OneToMany(Foo)
                
                
    def test_adapted_index(self):
        class Foo(self.env.Model):
            bars = OneToMany("Bar", indexes=['skidoo'])
            
        class Bar(self.env.Model):
            skidoo = Text()
            
        with self.env.write():
            foo = Foo()
            for i in range(0,10):
                b = Bar(skidoo=('a' if i < 7 else 'c'))
                foo.bars.add(b)
                
        with self.env.read():
            results = foo.bars.indexes['skidoo'].cursor().range(start='a',end='b')
            self.assertEquals(['a'] * 7, [b.skidoo for b in results])
                
        
if __name__ == "__main__":
    unittest.main()
