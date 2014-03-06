import dson
import random
import math


class Cursor(object):
    
    def __init__(self, dbm, name, reverse=False):
        self.dbm = dbm
        self.name = name
        self.reverse = reverse
        self._cursor = None
        
        
    def next(self):
        if not self._cursor:
            self._cursor = self._create_cursor()
        if self._cursor.next():
            return self.load(self._cursor.value())
        
        
    def prev(self):
        if not self._cursor:
            return
        if self._cursor.prev():
            return self.load(self._cursor.value())
        
        
    def jump(self, key):
        if not self._cursor:
            self._cursor = self._create_cursor()
        self._cursor.jump(self.dump_key(key))
        
        
    def random(self):
        cursor = self._create_cursor()
        cursor.first()
        num_entries = self.dbm.count(self.name)
        current_index = 0
        indexes = range(0, num_entries)
        random.shuffle(indexes)
        
        while len(indexes) > 0:
            i = indexes.pop()
            
            if i > current_index:
                for x in range(0, i - current_index):
                    cursor.next()
            elif i < current_index:
                for x in range(0, current_index - i):
                    cursor.prev()
            current_index = i
            
            yield self.load(cursor.value())
        
        
    def get_reverse(self):
        return self.reverse
        
        
    def set_reverse(self, reverse):
        self.reverse = reverse
        
        
    def first(self):
        cursor = self._create_cursor()
        if self.reverse:
            if cursor.last():
                return self.load(cursor.value())
        elif cursor.first():
            return self.load(cursor.value())
        
        
    def last(self):
        cursor = self._create_cursor()
        if self.reverse:
            if cursor.first():
                return self.load(cursor.value())
        elif cursor.last():
            return self.load(cursor.value())
        
        
    def __iter__(self):
        return self.get_iterator('all')
        
        
    def range(self, start=None, end=None):
        if start is None and end is None:
            return self.__iter__()
            
        return self.get_iterator(
            'range',
            start if start is None else self.dump_key(start),
            end if end is None else self.dump_key(end)
        )
        
        
    def prefix(self, prefix):
        return self.get_iterator('match_prefix', self.dump_prefix(prefix))
        
        
    def key(self, key):
        return self.get_iterator('match_key', self.dump_key(key))
        
        
    def count_range(self, start=None, end=None):
        return self.get_count_with_iterator(
            'range',
            start if start is None else self.dump_key(start),
            end if end is None else self.dump_key(end)
        )
        
        
    def count_prefix(self, prefix):
        return self.get_count_with_iterator('match_prefix', self.dump_prefix(prefix))
        
        
    def count_key(self, key):
        return self.get_count_with_iterator('match_key', self.dump_key(key))
        
        
    def dump_key(self, key):
        return dson.dumpone(key)
        
        
    def dump_prefix(self, prefix):
        return self.dump_key(prefix)
        
        
    def load(self, data):
        return dson.loads(data)
        
        
    def get_iterator(self, name, *args):
        forward, backward = iterators.get(name)
        iterator = backward if self.reverse else forward
        for k,v in iterator(self._create_cursor(), *args):
            yield self.load(v)
            
            
    def get_count_with_iterator(self, name, *args):
        forward, backward = iterators.get(name)
        iterator = backward if self.reverse else forward
        c = 0
        for r in iterator(self._create_cursor(), *args):
            c += 1
        return c
        
        
    def _create_cursor(self):
        return self.dbm.cursor(self.name)
   
        

def iter_all(cursor):
    cursor.first()
    return cursor.iternext()
    
    
def iter_all_reverse(cursor):
    cursor.last()
    return cursor.iterprev()
    
    
def iter_range(cursor, start, end):
    if start is None:
        cursor.first()
    else:
        cursor.jump(start)
    if end:
        return iter_while(
            cursor.iternext(),
            lambda x: x < end
        )
    else:
        return cursor.iternext()
        
        
def iter_range_reverse(cursor, start, end):
    if end is None:
        cursor.last()
    else:
        cursor.jump(end)
        cursor.prev()
    if start:
        return iter_while(
            cursor.iterprev(), 
            lambda x: x >= start
        )
    else:
        return cursor.iterprev()
        
        
def iter_match_prefix(cursor, prefix):
    cursor.jump(prefix)
    return iter_while(
        cursor.iternext(), 
        lambda x: x.startswith(prefix)
    )
    
    
def iter_match_prefix_reverse(cursor, prefix):
    for r in iter_match_prefix(cursor, prefix):
        pass
    
    if cursor.key():
        cursor.prev()
    
    return iter_while(
        cursor.iterprev(), 
        lambda x: x.startswith(prefix)
    )
    
    
def iter_match_key(cursor, key):
    cursor.jump(key)
    return iter_while(
        cursor.iternext(), 
        lambda x: x == key
    )
    
    
def iter_match_key_reverse(cursor, key):
    key_bound = key + '\x00'
    cursor.jump(key_bound)
    cursor.prev()
    return iter_while(
        cursor.iterprev(),
        lambda x: x == key
    )
        
        
def iter_while(iterator, predicate):
    for key, value in iterator:
        if predicate(key):
            yield key, value
        else:
            break

            
iterators = {
    'all': (iter_all, iter_all_reverse),
    'range': (iter_range, iter_range_reverse),
    'match_prefix': (iter_match_prefix, iter_match_prefix_reverse),
    'match_key': (iter_match_key, iter_match_key_reverse)
}
