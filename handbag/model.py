from validators import Validator, GroupValidator
from relationships import Relationship


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
            
            cls.indexes = ModelIndexCollectionAdaptor(cls, cls.table.indexes)
            
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
        return ModelCursorAdaptor(cls, cls.table.cursor(reverse=reverse))
        
        
    def count(cls):
        return cls.table.count()



class BaseModel(object):
    
    __metaclass__ = ModelMeta
        
    
    def __init__(self, **kwargs):
        sup = super(BaseModel, self)
        sup.__setattr__('_dirty', kwargs.pop('_dirty', True))
        sup.__setattr__('_reference_fields', {})
        
        for k,v in self.__class__.__dict__.items():
            if isinstance(v, Validator):
                field_value = kwargs.get(k, v.default())
                sup.__setattr__(k, field_value)
            elif isinstance(v, Relationship):
                if k in kwargs:
                    self._reference_fields[k] = kwargs[k]
        
        if 'id' in kwargs:
            sup.__setattr__('id', kwargs['id'])
        else:
            sup.__setattr__('id', self.env.generate_id())
        
        self.env.instances.add(self)
        self.env.current_context().enqueue(self)
        
        
    def __setattr__(self, name, value):
        if hasattr(self.__class__, name) and isinstance(getattr(self.__class__, name), Validator):
            assert self.env.current_context().writable, "Transaction is read-only."
            oldvalue = getattr(self, name)
            if oldvalue != value:
                self._dirty = True
                super(BaseModel, self).__setattr__(name, value)
                self.env.current_context().enqueue(self)
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
            self._dirty = True
            self.env.current_context().enqueue(self)
            
            
    def get_reference_field(self, name):
        return self._reference_fields.get(name)
    
        
    def remove_reference_field(self, name):
        if name in self._reference_fields:
            self._reference_fields.pop(name)
            self._dirty = True
            self.env.current_context().enqueue(self)
        
        
    def is_dirty(self):
        return self._dirty
        
        
    def remove(self):
        for k,v in self.__class__.__dict__.items():
            if isinstance(v, Relationship):
                v.on_owner_remove(self)
        self.table.remove(self.id)
        self._dirty = False
        
        
    def save(self):
        if self.is_dirty():
            doc = self.validate()
            doc = self.table.save(doc)
            super(BaseModel, self).__setattr__('id', doc['id'])
        
        
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
        
        
from functools import wraps


class ModelAdaptor(object):
    
    functions = []
    iterators = []
    
    
    def __init__(self, model, adapted):
        self.model = model
        self.adapted = adapted
        
        for k in self.functions:
            setattr(self, k, self.adapt_function(getattr(self.adapted, k)))
            
        for k in self.iterators:
            setattr(self, k, self.adapt_iterator(getattr(self.adapted, k)))
        
        
    def __getattr__(self, name):
        return getattr(self.adapted, name)
        
        
    def load(self, data):
        if data:
            data['_dirty'] = False
            return self.model(**data)
            
            
    def adapt_function(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            data = fn(*args, **kwargs)
            return self.load(data)
        return wrapper
        
        
    def adapt_iterator(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for data in fn(*args, **kwargs):
                yield self.load(data)
        return wrapper
    
    
class ModelCursorAdaptor(ModelAdaptor):
    functions = ['first', 'last']
    iterators = ['range', 'prefix', 'key']
    
    def __iter__(self):
        for data in self.adapted:
            yield self.load(data)
    
    
class ModelIndexAdaptor(ModelAdaptor):
    functions = ['get']
    iterators = ['all']
    
    def cursor(self, reverse=False):
        return ModelCursorAdaptor(self.model, self.adapted.cursor(reverse=reverse))
        
        
class ModelIndexCollectionAdaptor(object):
    
    def __init__(self, model, index_collection):
        self.model = model
        self.index_collection = index_collection
        
        
    def __contains__(self, fields):
        return fields in self.index_collection
        
        
    def __getitem__(self, fields):
        index = self.index_collection.__getitem__(fields)
        return ModelIndexAdaptor(self.model, index)
        
        
    def add(self, *args, **kwargs):
        self.index_collection.add(*args, **kwargs)
        
        
