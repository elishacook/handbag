def base(db):
    type('Model', (BaseModel,), dict(db=db))
    
    
class ModelMeta(type):
    
    def __init__(cls, bases, dict):
        cls.table = cls.db[cls.__name__]
    
    
class BaseModel(object):
    __metaclass__ = ModelMeta
    