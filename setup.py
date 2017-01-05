from distutils.core import setup
from distutils.extension import Extension
import codecs
import os
import re


def local_file(filename):
    return codecs.open(
        os.path.join(os.path.dirname(__file__), filename), 'r', 'utf-8'
    )

version = re.search(
    "^__version__ = \((\d+), (\d+), (\d+)\)",
    local_file(os.path.join('KSIF', '__init__.py')).read(),
    re.MULTILINE
).groups()

try:
    from Cython.Build import cythonize
except ImportError:
    use_cython = False
else:
    use_cython = True

ext_modules = []

if use_cython:
    ext_modules = cythonize('KSIF/core/base.py')
else:
    ext_modules = [
        Extension('KSIF.core.base', ['KSIF/core/base.c'])
    ]

setup(
    name="KSIF",
    version='.'.join(version),
    author='Seung Hyeon Yu',
    author_email='rambor12@business.kaist.ac.kr',
    description='KSIF Library',
    keywords='python finance quant backtesting strategies',
    url='https://github.com/KAISTSIF/KSIF',
    install_requires=[
        'pyprind'
    ],
    packages=['KSIF'],
    long_description=local_file('README.rst').read(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python'
    ],
    ext_modules=ext_modules
)
