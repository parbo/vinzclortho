#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup

setup(name='VinzClortho',
      version='0.1.1.2',
      description='A distributed key/value-store, also known as a NOSQL database.',
      author='Pär Bohrarper',
      author_email='par.bohrarper@gmail.com',
      url='http://bitbucket.org/rogueops/vinzclortho',
      license="MIT",
      packages=['vinzclortho', 'tangled'],
      scripts=['scripts/vinzclortho'],
      )
