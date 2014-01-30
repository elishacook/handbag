import dson


class Cursor(object):
    
    def __init__(self, dbm, name, reverse=False):
        self.dbm = dbm
        self.name = name
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
        return self.get_count_with_iterator('match_prefix', self.dump_key(prefix))
        
        
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
    last_char = ord(prefix[-1])
    if last_char < 255:
        prefix_bound = prefix[:-1] + chr(last_char + 1)
    else:
        prefix_bound = prefix + '\x00'
        
    cursor.jump(prefix_bound)
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
