# Handbag

**NOTE** This is pre-alpha software, i.e., cool your jets, lots of stuff is not implemented. If you like the concept, consider helping out by checking out what *is* implemented, suggesting ideas, writing code or just sending a nice note to keep me motivated. Thanks!

Handbag is an embedded database for python. Its goal is to be a viable persistence option for small-to-medium sized projects and also to be super fun to use. This is not the DB for giganto-scale applications, but if you want data persistence, consistency and convenience this might be the thing for you.

Handbag uses [lmdb](http://symas.com/mdb/) (also [py-lmdb](https://github.com/dw/py-lmdb)) so it inherits all of its excellent qualities. When you use Handbag you get:

* Multi-thread and multi-process concurrency
* ACID guarantees
* Pretty good core performance ([lmdb benchmark]([http://symas.com/mdb/microbench/]), no handbag benchmarks yet)
* Data validation
* Relationship modeling
* Lazy database creation

The backend system is pluggable making it hypothetically possible to implement other storage mechanisms in the future.

## What's it look like?

```python
import random
from handbag import *

# Create a database on the filesystem
env = Environment('/tmp/foos-and-bars.db')

# Define some models that will be stored in the database

class Foo(env.Model):
    name = Text(index=True)
    flavor = Enum('spicy', 'artichoke', 'red')
    indexes = ['name']
    
class Bar(env.Model):
    address = Text()
    foo = ManyToOne(Foo, inverse="bars")

# Do everything inside a transaction
    
with env.write():
    foo = Foo(name='Foo Boringface', flavor='spicy')
    bar = Bar(address='123 Snapchat Lane', foo=foo)
    bar_id = bar.id
    
# Some time later...

with env.read():
    bar = Bar[bar_id]
    assert bar.address == "123 Snapchat Lane"
    assert bar.foo.name == "Foo Boringface"
    assert list(foo.bars)[0] == bar
    
    # We can also look up by indexed fields ... 
    foo2 = Foo.indexes['name'].get("Foo Boringface")
    assert foo2 == foo

# Let's add some more foos

with env.write():
    for i in range(0,10):
        foo = Foo(name="Thing #%d" % i, flavor=random.choice(Foo.flavor.values))

# Indexes also support range lookup

with env.read():
    assert Foo.indexes['name'].cursor().range('Th').count() == 10
    
```
