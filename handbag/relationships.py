class Relationship(object):
    
    def __init__(self, target_model, inverse=None, cascade=False):
        """Create a relationship between two models.
        
        :param target_model: The model type that is the target of the relationship.
        :param inverse: The name of the inverse relationsip on the target. This relationship will be created automatically and does not need to be defined on the target.
        :param cascade: If ``True``, when an instance is removed, the related target models will also be removed.
        """
        self.name = None
        self.model = None
        self.env = None
        self.is_setup = False
        self._target_model = target_model
        self._inverse_name = inverse
        self._inverse = None
        self._cascade = cascade
        
        
    def setup(self, name, model):
        if self.is_setup:
            return
        self.is_setup = True
        self.name = name
        self.model = model
        self.env = model.env
        self.resolve_backreferences()
        
        
    def resolve_backreferences(self):
        if not self._inverse_name:
            return
        
        if self.is_target_model_defined():
            backref = self.env.backreferences.get(self.model.__name__, self.name)
            if backref:
                self.set_inverse(backref)
            elif hasattr(self._target_model, self._inverse_name):
                inverse_rel = getattr(self._target_model, self._inverse_name)
                assert not inverse_rel.has_inverse(), \
                    "Attempt to redefine inverse relationship %s.%s" % (self._target_model.name, self._inverse_name)
                inverse_rel.set_inverse(self)
        else:
            assert self.env.backreferences.get(self.get_target_model_name(), self._inverse_name) is None, \
                "Attempt to redefine inverse relationship %s.%s" % (self.get_target_model_name(), self._inverse_name)
            self.env.backreferences.set(self.get_target_model_name(), self._inverse_name, self)
        
        
    def set_inverse(self, rel):
        if not isinstance(rel, Relationship):
            raise TypeError, "Expected a Relationship instance."
        self.validate_inverse(rel)
        self._inverse = rel
        rel._inverse = self
        self.on_inverse_set()
        rel.on_inverse_set()
        
        
    def get_inverse(self):
        if not self._inverse:
            self._inverse = self.create_inverse()
            self._inverse._target_model = self.model
            self._inverse.model = self.get_target_model()
            self._inverse._inverse_name = self.name
            self._inverse._inverse = self
            self._inverse.name = self._inverse_name
        return self._inverse
        
        
    def has_inverse(self):
        return self._inverse is not None
        
        
    def validate_inverse(self, rel):
        inverse_rel = self.create_inverse()
        if rel.__class__ != inverse_rel.__class__:
            raise TypeError, "Expected %s to be of type %s" % (rel.get_full_name(), inverse_rel.__class__.__name__)
        if rel.get_target_model() != inverse_rel.get_target_model():
            raise TypeError, "Expected %s to have a target of type %s" % (rel.get_full_name(), inverse_rel.get_target_model_name())
        
        
    def create_inverse(self):
        raise NotImplementedError
        
        
    def on_owner_remove(self, owner):
        if self._cascade:
            self.cascade(owner)
            
            
    def on_inverse_set(self):
        pass
        
        
    def is_target_model_defined(self):
        return not isinstance(self._target_model, basestring)
        
        
    def get_target_model_name(self):
        if self.is_target_model_defined():
            return self._target_model.__name__
        else:
            return self._target_model
        
        
    def get_target_model(self):
        if not self.is_target_model_defined():
            name = self.get_target_model_name()
            try:
                self._target_model = self.env.models[name]
            except KeyError:
                raise Exception, "Could not find a model called '%s'" % name
        return self._target_model
        
        
    def get_full_name(self):
        return "%s.%s" % (self.model.__name__, self.name)
        
        
    def __get__(self, instance, cls):
        if instance:
            return self.get(instance)
        else:
            return self
            
            
    def __set__(self, instance, value):
        model = self.get_target_model()
        assert isinstance(value, model), "Expected an instance of %s" % model.__name__
        self.set(instance, value)
        
        
    def get(self, owner):
        raise NotImplementedError
        
        
    def set(self, owner, target, update_inverse=True):
        raise NotImplementedError
        
        
    def cascade(self, owner):
        raise NotImplementedError



class One(Relationship):
    
    def __init__(self, target_model, inverse=None):
        if inverse is not None:
            raise TypeError, "One-type relationships don't have an inverse."
        super(One, self).__init__(target_model)
        
        
    def get(self, owner):
        if not hasattr(self, '_cache'):
            id = owner.get_reference_field(self.name)
            if id:
                self._cache = self.get_target_model().get(id)
            else:
                self._cache = None
        return self._cache
        
        
    def set(self, owner, target, update_inverse=True):
        self._cache = target
        
        for obj in self.env.instances[owner.id]:
            obj.set_reference_field(self.name, target.id)
        
        
    def cascade(self, owner):
        target = self.get(owner)
        if target:
            target.remove()

        
class OneToOne(One):
    
    def __init__(self, target_model, inverse=None, cascade=False):
        Relationship.__init__(self, target_model, inverse, cascade)
        
        
    def create_inverse(self):
        return OneToOne(self.model)
        
        
    def set(self, owner, target, update_inverse=True):
        One.set(self, owner, target)
        if update_inverse and self._inverse:
            self._inverse.set(target, owner, update_inverse=False)
            
            
class ManyToOne(One):
    
    def __init__(self, target_model, inverse=None):
        Relationship.__init__(self, target_model, inverse)
        
    
    def create_inverse(self):
        return OneToMany(self.model)


class Many(Relationship):
    
    
    def __init__(self, *args, **kwargs):
        self.indexes = kwargs.pop('indexes', [])
        super(Many, self).__init__(*args, **kwargs)
    
    
    def setup(self, name, model):
        if not self._inverse_name:
            self._inverse_name = model.__name__.lower()
            
        super(Many, self).setup(name, model)
        
        
    def on_inverse_set(self):
        self.setup_indexes()
        
        
    def setup_indexes(self):
        raise NotImplementedError
        
        
    def iter(self, owner):
        raise NotImplementedError
        
        
    def get(self, owner):
        return ManyProxy(self, owner)
        
        
    def cascade(self, owner):
        self.remove(owner)
        
        
    def add(self, owner, target):
        raise NotImplementedError
        
        
    def remove(self, owner, target):
        raise NotImplementedError
        
        
    def count(self, owner):
        raise NotImplementedError
        
        
    def contains(self, owner, obj):
        raise NotImplementedError
        
        
    def index_key_adaptor(self, owner):
        raise NotImplementedError


class ManyProxy(object):
    
    def __init__(self, rel, owner):
        self._rel = rel
        self._owner = owner
        self.indexes = ManyIndexCollection( rel.index_key_adaptor(owner) )
        
        
    def count(self):
        return self._rel.count(self._owner)
        
        
    def __iter__(self):
        return self._rel.iter(self._owner)
        
        
    def __contains__(self, obj):
        return self._rel.contains(self._owner, obj)
        
        
    def __len__(self):
        return self.count()
        
        
    def first(self):
        return self.__iter__().next()
        
        
    def add(self, target):
        return self._rel.add(self._owner, target)
        
        
    def remove(self, target):
        return self._rel.remove(self._owner, target)
    
    
class ManyIndexCollection(object):
    
    def __init__(self, adaptor):
        self.adaptor = adaptor
        
        
    def __getitem__(self, fields):
        return ManyIndex(self.adaptor, fields)
    
    
class ManyIndex(object):
    
    def __init__(self, adaptor, fields):
        self.adaptor = adaptor
        self.index = self.adaptor.get_index(fields)
        
        
    def get(self, key):
        return self.index.get(
            self.adaptor.make_key(self.index, key)
        )
        
        
    def all(self, key):
        return self.index.all(
            self.adaptor.make_key(self.index, key)
        )
        
        
    def count(self):
        return self.index.cursor().count_range(start=self.adaptor.make_key(self.index))
        
        
    def cursor(self, reverse=False):
        return ManyCursor(self.adaptor, self.index, self.index.cursor())
        
        
class ManyIndexKeyAdaptor(object):
    
    def __init__(self, rel, owner, reference_field_name):
        self.rel = rel
        self.owner = owner
        self.reference_field_name = reference_field_name
        
        
    def make_key(self, index, key=None):
        if key is None:
            key = {}
        if isinstance(key, dict):
            key[self.reference_field_name] = self.owner.id
        else:
            key = {
                self.reference_field_name: self.owner.id,
                index.get_fields().names[1]: key
            }
        return key
        
        
    def get_index(self, fields):
        if isinstance(fields, tuple):
            complete_fields = (self.reference_field_name,) + fields
        else:
            complete_fields = (self.reference_field_name, fields)
            
        return self.rel.get_target_model().indexes[complete_fields]
        
        
class ManyCursor(object):
    
    def __init__(self, adaptor, index, cursor):
        self.adaptor = adaptor
        self.index = index
        self.cursor = cursor
        
        
    def first(self):
        try:
            self.cursor.prefix(self.adaptor.make_key(self.index)).next()
        except StopIteration:
            pass
            
            
    def last(self):
        old_reverse = self.cursor.reverse
        self.cursor.reverse = True
        result = self.first()
        self.cursor.reverse = old_reverse
        return result
        
        
    def __iter__(self):
        return self.cursor.prefix(self.adaptor.make_key(self.index))
        
        
    def range(self, start=None, end=None):
        start, end = [self.adaptor.make_key(self.index, k) if k is not None else None for k in (start, end)]
        return self.cursor(start, end)
        
        
    def prefix(self, key):
        return self.cursor.prefix(self.adaptor.make_key(self.index, key))
        
        
    def key(self, key):
        return self.cursor.key(self.adaptor.make_key(self.index, key))
        
        
    def count_prefix(self, key):
        return self.cursor.count_prefix(self.adaptor.make_key(self.index, key))
        
        
    def count_key(self, key):
        return self.cursor.count_key(self.adaptor.make_key(self.index, key))
    
    
class OneToMany(Many):
    
    def setup_indexes(self):
        index_collection = self.get_target_model().indexes
        index_collection.add(self._inverse_name)
        
        for fields in self.indexes:
            if isinstance(fields, tuple):
                complete_fields = (self._inverse_name,) + fields
            else:
                complete_fields = (self._inverse_name, fields)
            index_collection.add(*complete_fields)
        
    
    def iter(self, owner):
        return self.get_target_model().indexes[self._inverse_name].cursor().key(owner.id)
    
    
    def create_inverse(self):
        return ManyToOne(self.model)
        
        
    def add(self, owner, target):
        target.set_reference_field(self._inverse_name, owner.id)
        
        
    def remove(self, owner, target):
        target.remove_reference_field(self._inverse_name)
        
        
    def count(self, owner):
        return self.get_target_model().indexes[self._inverse_name].cursor().count_key(owner.id)
        
        
    def cascade(self, owner):
        for target in list(self.iter(owner)):
            target.remove()
            
            
    def contains(self, owner, obj):
        if isinstance(obj, self.get_target_model()):
            return obj.get_reference_field(self._inverse_name) == owner.id
        else:
            return False
            
            
    def index_key_adaptor(self, owner):
        return ManyIndexKeyAdaptor(self, owner, self._inverse_name)

    
class ManyToMany(Many):
    
    def setup_indexes(self):
        self.full_name = "%s.%s" % (self.model.__name__, self.name)
        self.full_inverse_name = "%s.%s" % (self.get_target_model_name(), self._inverse_name)
        join_table_name = ','.join(sorted([self.full_name, self.full_inverse_name]))
        self.join_table = self.model.env.db[join_table_name]
        self.model_names = tuple(sorted([self.model.__name__, self.get_target_model_name()]))
        if self.model_names not in self.join_table.indexes:
            self.join_table.indexes.add(*self.model_names)
            self.join_table.indexes.add(self.model_names[0])
            self.join_table.indexes.add(self.model_names[1])
        
        target_model = self.get_target_model()
        for fields in self.indexes:
            if not isinstance(fields, tuple):
                fields = (fields,)
            fields = ( (self.full_name, self.get_owner_ids), ) + fields
            target_model.indexes.add(*fields)
            
    
    def get_owner_ids(self, doc):
        owner_ids = []
        for pair in self.join_table.indexes[self.get_target_model_name()].all(doc['id']):
            owner_ids.append(pair[self.model.__name__])
        return owner_ids
        
        
    def iter(self, owner):
        target_model = self.get_target_model()
        for doc in self.join_table.indexes[self.model.__name__].all(owner.id):
            inst = target_model.get(doc[target_model.__name__])
            if inst:
                yield inst
            else:
                self.join_table.remove(doc['id'])
        
    
    def create_inverse(self):
        return ManyToMany(self.model)
        
        
    def validate_inverse(self, rel):
        super(ManyToMany, self).validate_inverse(rel)
        assert isinstance(rel, ManyToMany), "Expected ManyToMany got %s" % rel.__class__.__name__
        
        
    def add(self, owner, target):
        if not self.contains(owner, target):
            self.join_table.save(self.make_join_doc(owner, target))
        
        
    def remove(self, owner, target):
        key = self.make_join_doc(owner, target)
        existing = self.join_table.indexes[self.model_names].get(key)
        if existing:
            self.join_table.remove(existing['id'])
        
        
    def cascade(self, owner):
        for target in self.iter(owner):
            target.remove()
        for doc in self.join_table.indexes[self.model.__name__].all(owner.id):
            self.join_table.remove(doc['id'])
        
        
    def count(self, owner):
        return self.join_table.indexes[self.model.__name__].cursor().count_key(owner.id)
        
        
    def contains(self, owner, obj):
        if isinstance(obj, self.get_target_model()):
            key = self.make_join_doc(owner, obj)
            return self.join_table.indexes[self.model_names].cursor().count_key(key)
        else:
            return False
        
        
    def index_key_adaptor(self, owner):
        return ManyIndexKeyAdaptor(self, owner, self.full_name)
        
        
    def make_join_doc(self, owner, target):
        return {
            self.model.__name__: owner.id,
            self.get_target_model_name(): target.id
        }
        