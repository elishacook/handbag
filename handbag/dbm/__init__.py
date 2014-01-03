from urlparse import urlparse
from lmdbdbm import LMDBDBM

__all__ = ['backends', 'open']

backends = {
    'lmdb': LMDBDBM
}


def open(url):
    parsed_url = urlparse(url)
    if parsed_url.scheme not in backends:
        raise Exception, "No backend found for scheme '%s'" % parsed_url.scheme
    dbm_cls = backends[parsed_url.scheme]
    return dbm_cls(parsed_url)