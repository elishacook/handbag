from setuptools import setup
execfile('handbag/version.py')

setup(
    name = 'handbag',
    version = __version__,
    packages = ['handbag'],
    description = 'An embedded database and data modeling library for python.',
    author = 'Elisha Fitch-Cook',
    author_email = 'elisha@elishacook.com',
    url = 'https://github.com/elishacook/handbag',
    test_suite = 'tests',
    install_requires=[
        'lmdb>=0.78',
        'pytz'
    ]
)
