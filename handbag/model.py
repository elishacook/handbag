import inspect
from validators import Validator, GroupValidator
from relationships import Relationship
from index import BaseKeyIndexCollectionProxy


def create(env):
    return type('Model', (BaseModel,), dict(env=env))


class ModelMeta(type):
    def __init__(cls, name, bases, dict):
        super(ModelMeta, cls).__init__(name, bases, dict)
        
        if name != "BaseModel" and name != "Model":
            cls.env.register_model(cls)
            cls._setup_inheritance()
            cls._setup_indexes(dict)
            cls._setup_relationships(dict)
            cls.validators = inspect.getmembers(cls, lambda x: isinstance(x, Validator))
            cls.relationships = inspect.getmembers(cls, lambda x: isinstance(x, Relationship))
    
    
    def _setup_inheritance(cls):
        genealogy = []
        for c in cls.mro():
            if hasattr(c, '__metaclass__') and c.__metaclass__ == ModelMeta and \
                c.__name__ != "BaseModel" and c.__name__ != "Model":
                genealogy.append(c)
        
        if len(genealogy) > 1:
            cls._type = ':'.join([c.__name__ for c in reversed(genealogy)])
            cls.ancestors = genealogy[1:]
            cls.table = cls.ancestors[0].table
        else:
            cls._type = None
            cls.ancestors = []
            cls.table = cls.env.db[cls.__name__]
        
        
    def _setup_indexes(cls, dict):
        if 'indexes' in dict:
            cls.index_definitions = dict['indexes']
        else:
            cls.index_definitions = []
            
        if cls._type:
            cls.indexes = BaseKeyIndexCollectionProxy(
                ModelIndexCollectionAdaptor(cls, cls.table.indexes),
                [('_type', cls._type)]
            )
            if tuple() not in cls.indexes:
                cls.indexes.add()
            cls.primary_index = cls.indexes.get()
        else:
            cls.indexes = ModelIndexCollectionAdaptor(cls, cls.table.indexes)
            cls.primary_index = None
        
        for fields in cls.index_definitions:
            if not isinstance(fields, tuple):
                fields = (fields,)
            if fields not in cls.indexes:
                cls.indexes.add(*fields)
            
        for a in cls.ancestors:
            for fields in a.index_definitions:
                if not isinstance(fields, tuple):
                    fields = (fields,)
                if fields not in cls.indexes:
                    cls.indexes.add(*fields)
        
        
    def _setup_relationships(cls, dict):
        for k,v in dict.items():
            if isinstance(v, Relationship):
                v.setup(k, cls)
    
        for name, inverse_rel in cls.env.backreferences.get_all(cls.__name__).items():
            rel = inverse_rel.get_inverse()
            setattr(cls, name, rel) 
            rel.setup(name, cls)
    
    
    def get(cls, id):
        if cls.primary_index:
            return cls.primary_index.get(id)
        else:
            return cls.load(cls.table.get(id))
        
        
    def cursor(cls, reverse=False):
        if cls.primary_index:
            return cls.primary_index.cursor(reverse=reverse)
        else:
            return ModelCursorAdaptor(cls, cls.table.cursor(reverse=reverse))
        
        
    def count(cls):
        if cls.primary_index:
            return cls.primary_index.count()
        else:
            return cls.table.count()
            
            
    def load(cls, data):
        if data:
            if '_type' in data:
                parts = data['_type'].split(':')
                model = cls.env.models[parts[-1]]
            else:
                model = cls
            data['_dirty'] = False
            return model(**data)


class BaseModel(object):
    
    __metaclass__ = ModelMeta
        
    
    def __init__(self, **kwargs):
        sup = super(BaseModel, self)
        sup.__setattr__('_dirty', kwargs.pop('_dirty', True))
        sup.__setattr__('_reference_fields', {})
        
        for k,v in self.validators:
            field_value = kwargs.get(k, v.default())
            sup.__setattr__(k, field_value)
        for k,v in self.relationships:
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
        for k,v in self.relationships:
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
        for k,v in self.validators:
            validators[k] = v
            values[k] = getattr(self, k)
        group_validator = GroupValidator(**validators)
        validated = group_validator.validate(values)
        validated['id'] = self.id
        validated.update(self._reference_fields)
        
        if self._type:
            validated['_type'] = self._type
        
        return validated
        
        
    def to_dict(self):
        values = {}
        for k,v in self.validators:
            values[k] = getattr(self, k)
        values['id'] = self.id
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
            
            
    def adapt_function(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            data = fn(*args, **kwargs)
            return self.model.load(data)
        return wrapper
        
        
    def adapt_iterator(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for data in fn(*args, **kwargs):
                yield self.model.load(data)
        return wrapper
    
    
class ModelCursorAdaptor(ModelAdaptor):
    functions = ['first', 'last']
    iterators = ['range', 'prefix', 'key']
    
    def __iter__(self):
        for data in self.adapted:
            yield self.model.load(data)
    
    
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
        
        
