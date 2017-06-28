#!/usr/bin/env python

from distutils.core import setup, Command
import os
import os.path

setup(
    name='sqltocache',
    version='2.0',
    description='sql cache decorator',
    url='http://xiaorui.cc',
    author='ruifengyun',
    author_email='rfyiamcool@163.com',
    long_description=open('README.md').read(),
    packages=['sqltocache'],
)
