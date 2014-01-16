import weakref
import threading
from functools import partial


class ModelInstanceRegistry(object):
    
    def __init__(self):
        self._local = threading.local()
        self._local.instances = {}
        
        
    def add(self, inst):
        id = unicode(inst.id)
        callback = partial(self._remove_model_inst_ref, id)
        ref = weakref.ref(inst, callback)
        if id not in self._local.instances:
            self._local.instances[id] = [ref]
        else:
            self._local.instances[id].append(ref)
        
        
    def __getitem__(self, id):
        id = unicode(id)
        refs = self._local.instances.get(id, [])
        insts = []
        for r in refs:
            inst = r()
            if inst:
                insts.append(inst)
        return insts
        
        
    def __delitem__(self, id, r):
        refs = self._local.instances.get(id)
        if refs:
            try:
                refs.remove(r)
            except ValueError:
                pass
            if len(refs) == 0:
                self._local.instances.pop(id)
    
    
    def clear(self):
        self._local = threading.local()
        self._local.instances = {}
        
        
    def _remove_model_inst_ref(self, id, r):
        refs = self._local.instances.get(id)
        if refs:
            try:
                refs.remove(r)
            except ValueError:
                pass
            if len(refs) == 0:
                self._local.instances.pop(id)
    
    
    
class BackreferenceRegistry(object):
    
    def __init__(self):
        self._backrefs = {}
        
        
    def get(self, model_name, rel_name):
        if model_name not in self._backrefs or rel_name not in self._backrefs[model_name]:
            return None
        return self._backrefs[model_name][rel_name]
        
        
    def get_all(self, model_name):
        return self._backrefs.get(model_name, {})
        
        
    def set(self, model_name, rel_name, rel):
        if model_name not in self._backrefs:
            self._backrefs[model_name] = {}
        self._backrefs[model_name][rel_name] = rel
        