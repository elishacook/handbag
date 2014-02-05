"""Similar to, but incompatible with BSON"""

import struct
import cStringIO
import calendar
import pytz
from datetime import datetime


class DecodeError(Exception):
    pass
    
    
class EncodeError(Exception):
    pass


def dumps(value):
    buff = cStringIO.StringIO()
    encode(value, buff)
    return buff.getvalue()
    
    
def dump(value, stream):
    encode(value, stream)
    
    
def loads(bytes):
    buff = cStringIO.StringIO(bytes)
    return load(buff)
    
    
def load(stream):
    return decode(stream)
    

_type_by_magic = {
    '\x01': "dict",
    '\x02': "list",
    '\x03': "double",
    '\x04': "unicode",
    '\x05': "binary",
    '\x06': "bool",
    '\x07': "datetime",
    '\x08': "int",
    '\x09': "none"
}


_magic_by_type = {}
for k,v in _type_by_magic.items():
    _magic_by_type[v] = k
    
    
_encode_by_type = {}
_decode_by_type = {}



def dumpone(value):
    type_name = None
    if isinstance(value, float):
        type_name = 'double'
    elif isinstance(value, unicode):
        type_name = 'unicode'
    elif isinstance(value, str):
        type_name = 'binary'
    elif isinstance(value, bool):
        type_name = 'bool'
    elif isinstance(value, datetime):
        type_name = 'datetime'
    elif isinstance(value, (int,long)):
        type_name = 'int'
    elif value is None:
        type_name = 'none'
    elif isinstance(value, dict):
        type_name = 'dict'
    elif isinstance(value, (list, tuple)):
        type_name = 'list'
    else:
        raise EncodeError, "I can't encode a single '%s'" % str(value)
        
    encode_fn = _encode_by_type[type_name]
    
    if type_name in ('dict', 'list'):
        stream = cStringIO.StringIO()
        encode_fn(value, stream)
        bytes = stream.getvalue()
    else:
        bytes = encode_fn(value)
        
    magic = _magic_by_type[type_name]
    return magic + bytes
    
    
def loadone(bytes):
    if len(bytes) == 0:
        raise DecodeError, "Can't decode the empty string"
    magic = bytes[0]
    type_name = _type_by_magic.get(magic)
    if not type_name:
        raise DecodeError, "Unknown magic number %s" % str(magic).encode('string-escape')
    decode_fn = _decode_by_type[type_name]
    return decode_fn(bytes[1:])
    


def encode(value, stream):
    type_name = None
    if isinstance(value, float):
        type_name = 'double'
    elif isinstance(value, unicode):
        type_name = 'unicode'
    elif isinstance(value, str):
        type_name = 'binary'
    elif isinstance(value, bool):
        type_name = 'bool'
    elif isinstance(value, datetime):
        type_name = 'datetime'
    elif isinstance(value, (int, long)):
        type_name = 'int'
    elif value is None:
        type_name = 'none'
        
    if type_name is not None:
        encode_fn = _encode_by_type[type_name]
        bytes = encode_fn(value)
        length = len(bytes)
        magic = _magic_by_type[type_name]
        stream.write(magic)
        stream.write(struct.pack('>I', length))
        stream.write(bytes)
    elif isinstance(value, dict):
        stream.write(_magic_by_type['dict'])
        encode_dict(value, stream)
    elif isinstance(value, (list, tuple)):
        stream.write(_magic_by_type['list'])
        encode_list(value, stream)
    else:
        raise EncodeError, "I have no idea how to encode '%s'" % str(value)


class StopDecoding(Exception):
    pass

        
def decode(stream):
    type_name = read_type(stream)
    
    if type_name == 'dict':
        return decode_dict(stream)
    elif type_name == 'list':
        return decode_list(stream)
    else:
        decode_fn = _decode_by_type[type_name]
        data = read_chunk(stream)
        return decode_fn(data)
        
        
def read_type(stream):
    magic = stream.read(1)
    if not magic:
        raise StopDecoding
    if magic == '\x00':
        raise StopDecoding
    type_name = _type_by_magic.get(magic)
    if not type_name:
        raise DecodeError, "Unknown magic number %s" % str(magic).encode('string-escape')
    return type_name
    
    
def read_chunk(stream):
    data = stream.read(struct.calcsize('>I'))
    if not data:
        raise StopDecoding
    length = struct.unpack('>I', data)[0]
    if length == 0:
        return ''
    data = stream.read(length)
    if not data:
        raise StopDecoding
    if len(data) < length:
        raise ValueError, "Unexpected end of stream"
    return data


def encode_double(value):
    return struct.pack('>d', value)
    
    
def decode_double(bytes):
    return struct.unpack('>d', bytes)[0]
    
    
def encode_unicode(value):
    return value.encode('utf8')
    
    
def decode_unicode(bytes):
    return bytes.decode('utf-8')
    
    
def encode_binary(value):
    return value
    
    
def decode_binary(bytes):
    return bytes
    
    
def encode_bool(value):
    return struct.pack('>b', 1 if value else 0)
    
    
def decode_bool(bytes):
    value = struct.unpack('>b', bytes)[0]
    return bool(value)
    
    
def encode_datetime(value):
    ms = calendar.timegm(value.utctimetuple()) * 1000 + value.microsecond / 1000.0
    return struct.pack('>d', ms)
    
    
def decode_datetime(bytes):
    ms = struct.unpack('>d', bytes)[0]
    return datetime.fromtimestamp(ms / 1000.0, pytz.utc)
    
    
def encode_int(value):
    return struct.pack('>q', value)
    
    
def decode_int(bytes):
    return struct.unpack('>q', bytes)[0]
    
    
def encode_none(value):
    return '\x00'
    
    
def decode_none(bytes):
    return None
    
    
def encode_dict(value, stream):
    for k,v in value.items():
        encode(k, stream)
        encode(v, stream)
    stream.write('\x00')
        
        
def decode_dict(stream):
    d = {}
    while True:
        try:
            k = decode(stream)
            d[k] = decode(stream)
        except StopDecoding:
            break
    return d
    
    
def encode_list(value, stream):
    for v in value:
        encode(v, stream)
    stream.write('\x00')
    
    
def decode_list(stream):
    l = []
    while True:
        try:
            l.append(decode(stream))
        except StopDecoding:
            break
    return l
    
    
_locals = locals()
for k in _magic_by_type.keys():
    _encode_by_type[k] = _locals['encode_%s' % k]
    _decode_by_type[k] = _locals['decode_%s' % k]
    