import os
from setuptools import setup


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as fh:
    readme = fh.read()


setup(
    name='ucache',
    version=__import__('ucache').__version__,
    description='lightweight and efficient caching library',
    long_description=readme,
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    url='http://github.com/coleifer/ucache/',
    packages=[],
    py_modules=['ucache'],
    test_suite='tests')
