from validators import Validator, GroupValidator
from relationships import Relationship
from functools import wraps


def create(env):
    return type('Model', (BaseModel,), dict(env=env))


class ModelMeta(type):
    def __init__(cls, name, bases, dict):
        super(ModelMeta, cls).__init__(name, bases, dict)
        
        if name != "BaseModel" and name != "Model":
            
            cls.env.register_model(cls)
            cls.table = cls.env.db[name]
            
            if hasattr(cls, 'indexes'):
                for fields in cls.indexes:
                    if isinstance(fields, basestring):
                        fields = (fields,)
                    cls.table.indexes.add(*fields)
            
            cls.indexes = ModelIndexCollection(cls, cls.table.indexes)
            
            for k,v in cls.__dict__.items():
                if isinstance(v, Relationship):
                    v.setup(k, cls)
        
            for name, inverse_rel in cls.env.backreferences.get_all(cls.__name__).items():
                rel = inverse_rel.get_inverse()
                setattr(cls, name, rel)
                rel.setup(name, cls)
        
    
    def get(cls, id):
        doc = cls.table.get(id)
        if doc:
            doc['_dirty'] = False
            return cls(**doc)
        
        
    def cursor(cls, reverse=False):
        return ModelCursor(cls, cls.table.cursor(reverse=reverse))
        
        
    def remove(cls, id_or_spec):
        if isinstance(id_or_spec, dict):
            for inst in cls.find(id_or_spec):
                inst.remove()
        else:
            inst = cls.get(id_or_spec)
            if inst:
                inst.remove()
        
        
    def count(cls):
        return cls.table.count()
            



class BaseModel(object):
    
    __metaclass__ = ModelMeta
        
    
    def __init__(self, **kwargs):
        sup = super(BaseModel, self)
        sup.__setattr__('_dirty', kwargs.pop('_dirty', True))
        sup.__setattr__('_dirty_fields', {})
        sup.__setattr__('_reference_fields', {})
        
        for k,v in self.__class__.__dict__.items():
            if isinstance(v, Validator):
                field_value = kwargs.get(k, v.default())
                sup.__setattr__(k, field_value)
                if self._dirty:
                    self._dirty_fields[k] = (None, field_value)
            elif isinstance(v, Relationship):
                self._reference_fields[k] = kwargs.get(k)
        
        if 'id' in kwargs:
            sup.__setattr__('id', kwargs['id'])
        else:
            sup.__setattr__('id', self.env.generate_id())
        
        self.env.watch_for_changes(self)
        
        
    def __setattr__(self, name, value):
        if hasattr(self.__class__, name) and isinstance(getattr(self.__class__, name), Validator):
            assert self.env.current_context().writable, "Transaction is read-only."
            oldvalue = getattr(self, name)
            if oldvalue != value:
                self._dirty = True
                self._dirty_fields[name] = (oldvalue, value)
                super(BaseModel, self).__setattr__(name, value)
        else:
            super(BaseModel, self).__setattr__(name, value)
        
    
    def __str__(self):
        return '<%s %s>' % (self.__class__.__name__, self.id)
    
    
    def __eq__(self, other):
        if isinstance(other, BaseModel) and other.id == self.id:
            return True
        return False
    
    
    def set_reference_field(self, name, value):
        if value != self._reference_fields.get(name):
            oldvalue = self._reference_fields.get(name)
            self._reference_fields[name] = value
            self._dirty_fields[name] = (oldvalue, value)
            self._dirty = True
            
            
    def get_reference_field(self, name):
        return self._reference_fields.get(name)
    
        
    def is_dirty(self):
        return self._dirty
        
        
    def get_dirty_fields(self):
        return self._dirty_fields
        
        
    def remove(self):
        for k,v in self.__class__.__dict__.items():
            if isinstance(v, Relationship):
                v.on_owner_remove(self)
        self.table.remove(self.id)
        self._dirty = False
        
        
    def save(self):
        if self.is_dirty():
            doc = self.validate()
            self.table.save(doc)
        
        
    def validate(self):
        validators = {}
        values = {}
        for k,v in self.__class__.__dict__.items():
            if isinstance(v, Validator):
                validators[k] = v
                values[k] = getattr(self, k)
        group_validator = GroupValidator(**validators)
        validated = group_validator.validate(values)
        validated['id'] = self.id
        validated.update(self._reference_fields)
        return validated
        
        
    def to_dict(self):
        values = {}
        for k,v in self.__class__.__dict__.items():
            if isinstance(v, Validator):
                values[k] = getattr(self, k)
        values.update(self._reference_fields)
        return values
        
        
        
class ModelIteratorWrapper(object):
    singles = []
    iterators = []
    
    def __init__(self, model, wrapper):
        self.model = model
        self.wrapper = wrapper
        for k in self.singles:
            setattr(self, k, self._make_single(getattr(self.wrapper, k)))
        for k in self.iterators:
            setattr(self, k, self._make_iterator(getattr(self.wrapper, k)))
            
            
    def __getattr__(self, name):
        return getattr(self.wrapper, name)
    
    
    def __iter__(self):
        for doc in self.wrapper:
            doc['_dirty'] = False
            yield self.model(**doc)
    
            
    def _make_single(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            doc = fn(*args, **kwargs)
            if doc:
                doc['_dirty'] = False
                return self.model(**doc)
        return wrapper
        
        
    def _make_iterator(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for doc in fn(*args, **kwargs):
                doc['_dirty'] = False
                yield self.model(**doc)
        return wrapper


class ModelCursor(ModelIteratorWrapper):
    singles = ['first', 'last']
    iterators = ['range', 'prefix', 'key']

    
class ModelIndexCollection(object):
    
    def __init__(self, model, index_collection):
        self.model = model
        self.index_collection = index_collection
        
        
    def __getitem__(self, fields):
        index = self.index_collection.__getitem__(fields)
        return ModelIndex(self.model, index)
        
        
class ModelIndex(ModelIteratorWrapper):
    singles = ['get']
    iterators = ['all']
    
    def cursor(self, reverse=False):
        return ModelCursor(self.model, self.index.cursor(reverse=reverse))
    