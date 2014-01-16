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
        self._target_model = target_model
        self._inverse_name = inverse
        self._inverse = None
        self._cascade = cascade
        
        
    def setup(self, name, model):
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
        
        
    def set_inverse(self, ref):
        if not isinstance(ref, Relationship):
            raise TypeError, "Expected a Relationship instance."
        self.validate_inverse(ref)
        self._inverse = ref
        ref._inverse = self
        
        
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


class ManyProxy(object):
    
    def __init__(self, rel, owner):
        self._rel = rel
        self._owner = owner
        
        
    def count(self, spec=None):
        return self._rel.count(self._owner, spec)
        
        
    def __iter__(self):
        return self._rel.iter(self._owner)
        
        
    def add(self, target):
        return self._rel.add(self._owner, target)
        
        
    def remove(self, target):
        return self._rel.remove(self._owner, target)
        


class Many(Relationship):
    
    def setup(self, name, model):
        if not self._inverse_name:
            self._inverse_name = model.__name__.lower()
        super(Many, self).setup(name, model)
        
        
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
        
        
    
class OneToMany(Many):
    
    def iter(self, owner):
        return self.get_target_model().indexes[self._inverse_name].cursor().key(owner.id)
    
    
    def create_inverse(self):
        return ManyToOne(self.model)
        
        
    def add(self, owner, target):
        target.set_reference_field(self._inverse_name, owner.id)
        
        
    def remove(self, owner, target):
        target.remove_reference_field(self._inverse_name)
        
        
    def count(self, owner):
        self.get_target_model().indexes[self._inverse_name].cursor().count_key(owner.id)
        
        
    def cascade(self, owner):
        for target in self.iter(owner):
            target.remove()


    
class ManyToMany(Many):
    
    def __init__(self, *args, **kwargs):
        self.index_name = kwargs.pop('name', None)
        super(ManyToMany, self).__init__(*args, **kwargs)
    
    
    def setup(self, name, model):
        super(ManyToMany, self).setup(name, model)
        if not self.index_name:
            self.index_name = '-'.join(sorted(model.__name__, self.get_target_model_name()))
        self.index = model.env.db[self.index_name]
    
    
    def create_inverse(self):
        return ManyToMany(self.model)
        
        
    def validate_inverse(self, rel):
        super(ManyToMany, self).validate_inverse(rel)
        assert isinstance(rel, ManyToMany), "Expected ManyToMany got %s" % rel.__class__.__name__
        
        
    def add(self, owner, target):
        raise NotImplementedError
        
        
        
    def remove(self, owner, target):
        target_ids = self.get_target_ids(owner, target_or_spec)
        if len(target_ids) == 0:
            return
        self._storage_policy.remove(self, owner, target_ids)
        
        
    def get_target_ids(self, owner, target_or_spec):
        spec = self.get_spec_from_target_or_spec(owner, target_or_spec)
        target_collection = self.get_target_model().get_collection()
        return [r['id'] for r in target_collection.find(spec, [])]
        
        
    def cascade(self, owner):
        spec = self.spec(owner)
        self.get_target_model().remove(spec)
        
        
    def spec(self, owner):
        return self._storage_policy.spec(self, owner)
        
        
    def count(self, owner, spec=None):
        return self._storage_policy.count(self, owner, spec)
        