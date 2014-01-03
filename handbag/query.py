import re

class Filter(object):
    
    def __init__(self, value):
        self.value = value
        self.validate()
        
        
    def validate(self):
        pass
        
        
    def passes(self, value):
        raise NotImplementedError


class ReductionFilter(Filter):
    
    def validate(self):
        assert isinstance(self.value, (list,tuple)), "Expected a sequence, got %s" % str(self.value)

 
class And(ReductionFilter):
    
    def validate(self):
        if isinstance(self.value, And):
            self.value = self.value.value
            return
        super(And, self).validate()
    
    def passes(self, doc):
        for fn in self.value:
            if not fn.passes(doc):
                return False
        return True
        
        
class Or(ReductionFilter):
        
    def passes(self, doc):
        for fn in self.value:
            if fn.passes(doc):
                return True
        return False
        
        
class Nor(ReductionFilter):
    
    def passes(self, doc):
        for fn in self.value:
            if fn.passes(doc):
                return False
        return True
        
        
class Not(Filter):
    
    def passes(self, doc):
        return not self.value.passes(doc)
        
        
class ComparisonFilter(Filter):
    
    def __init__(self, key, value):
        self.key = key
        super(ComparisonFilter, self).__init__(value)
        
        
    def passes(self, doc):
        value = doc.get(self.key)
        
        if isinstance(value, list) and not isinstance(self.value, list):
            for v in value:
                if self.compare(v, self.value):
                    return True
            return False
                 
        return self.compare(value, self.value)
        
        
    def compare(self, a, b):
        raise NotImplementedError

        
class Equal(ComparisonFilter):
    
    def compare(self, x, y):
        return x == y
        
        
class NotEqual(ComparisonFilter):
    
    def compare(self, x, y):
        return x != y
        
        
class GreaterThan(ComparisonFilter):
    
    def compare(self, x, y):
        return x > y
    
    
class GreaterThanEqual(ComparisonFilter):
    
    def compare(self, x, y):
        return x >= y
    
    
class LessThan(ComparisonFilter):
    
    def compare(self, x, y):
        return x < y
    
    
class LessThanEqual(ComparisonFilter):
    
    def compare(self, x, y):
        return x <= y
    
    
class In(ComparisonFilter):
    
    def compare(self, x, y):
        return x in y
    
    
class NotIn(ComparisonFilter):
    
    def compare(self, x, y):
        return x not in y
        
        
class All(ComparisonFilter):
    
    def validate(self):
        assert isinstance(self.value, (list,tuple)), "Expected a sequence"
    
    def compare(self, x, y):
        if not isinstance(x, list):
            return False
        
        for a in y:
            if a not in x:
                return False
        
        return True
        
    
class Regex(ComparisonFilter):
    
    def __init__(self, key, value):
        if isinstance(value, basestring):
            value = re.compile(value)
        super(Regex, self).__init__(key, value)
    
    def compare(self, x, y):
        return y.search(x)


class Query(object):
    
    named_filters = {
        '$and': And,
        '$or': Or,
        '$not': Not,
        '$nor': Nor,
        '$gt': GreaterThan,
        '$gte': GreaterThanEqual,
        '$lt': LessThan,
        '$lte': LessThanEqual,
        '$ne': NotEqual,
        '$in': In,
        '$nin': NotIn,
        '$all': All,
        '$regex': Regex
    }
    
    def __init__(self, table, spec):
        self.table = table
        self.fields = {}
        self.filter = self._parse(None, spec, self.fields)
        
        
    def __iter__(self):
        for doc in self._get_cursor():
            if self.filter.passes(doc):
                yield doc
        
        
    def next(self):
        return self.__iter__().next()
        
        
    def count(self):
        c = 0
        for r in self:
            c += 1
        return c
        
        
    def pprint(self):
        self._pprint(self.filter)
        
        
    def _pprint(self, filter, depth=0):
        t = '  ' * depth
        print t + filter.__class__.__name__,
        if isinstance(filter.value, Filter):
            print
            self._pprint(filter.value, depth+1)
        elif isinstance(filter.value, list):
            if len(filter.value) > 0 and isinstance(filter.value[0], Filter):
                print
                for f in filter.value:
                    self._pprint(f, depth+1)
            else:
                print filter.value
        elif isinstance(filter, ComparisonFilter):
            print filter.key, filter.value
        else:
            print filter.value
        
        
    def _get_cursor(self):
        indexes = []
        for f in self.fields:
            if f in self.table.indexes:
                idx = self.table.indexes[f]
                c = idx.count()
                indexes.append((c, f, idx))
        if len(indexes) > 0:
            indexes.sort()
            c, k, idx = indexes[0]
            return idx.matching(self.fields[k])
        else:
            return self.table.cursor()
        
        
    def _parse(self, key, value, fields):
        if isinstance(value, dict):
            filters = []
            for k,v in value.items():
                if k in self.named_filters:
                    filter_cls = self.named_filters[k]
                    if issubclass(filter_cls, ComparisonFilter):
                        filters.append(
                            filter_cls(key, self._parse(key, v, fields))
                        )
                    else:
                        filters.append(
                            filter_cls(self._parse(key, v, fields))
                        )
                else:
                    _parsed_v = self._parse(k, v, fields)
                    if isinstance(_parsed_v, Filter):
                        filters.append(_parsed_v)
                    else:
                        fields[k] = _parsed_v
                        filters.append(Equal(k, _parsed_v))
            if len(filters) == 1:
                return filters[0]
            else:
                return And(filters)
        elif isinstance(value, list):
            return [self._parse(key, x, fields) for x in value]
        else:
            return value