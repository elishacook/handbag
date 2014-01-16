import database
import registry
import model
import uniqueid


def open(path):
    return Environment(path)


class Environment(object):
    
    def __init__(self, path):
        self.db = database.open(path)
        self.backreferences = registry.BackreferenceRegistry()
        self.instances = registry.ModelInstanceRegistry()
        self.models = {}
        self.context = None
        self.Model = model.create(self)
        
        
    def register_model(self, model):
        assert model.__name__ not in self.models, "Model '%s' is already defined" % model.__name__
        self.models[model.__name__] = model
        
        
    def watch_for_changes(self, inst):
        self.instances.add(inst)
        self.current_context().watch(inst)
        
        
    def generate_id(self):
        return uniqueid.create()
        
        
    def read(self):
        self.context = EnvironmentContext(self)
        return self.context
        
        
    def write(self):
        self.context = EnvironmentContext(self, writable=True)
        return self.context
        
        
    def current_context(self):
        assert self.context is not None, "An active transaction is required"
        return self.context
        
        
        
class EnvironmentContext(object):
    
    def __init__(self, env, writable=False):
        self.env = env
        self.writable = writable
        self.watch_list = {}
        self.max_watch_list_size = 20
        
        if writable:
            self.db_context = self.env.db.write()
        else:
            self.db_context = self.env.db.read()
        
        
    def __enter__(self):
        self.db_context.__enter__()
        
        
    def __exit__(self, type, value, traceback):
        try:
            if self.writable and not value:
                self.flush()
        except Exception, e:
            self.db_context.__exit__(e.__class__, e, None)
            raise
        else:
            self.db_context.__exit__(type, value, traceback)
        
        
    def watch(self, inst):
        if inst.is_dirty():
            assert self.writable, "Transaction is read-only"
        if inst.id not in self.watch_list and \
            len(self.watch_list) > self.max_watch_list_size:
            self.flush()
            
        self.watch_list[inst.id] = inst
        
        
    def flush(self):
        if len(self.watch_list) > 0:
            assert self.writable, "Transaction is read-only"
            for inst in self.watch_list.values():
                inst.save()
                