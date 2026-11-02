#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages
from setuptools import setup
from pathlib import Path
import re

ROOT = Path(__file__).parent
VERSION = re.search(r"^__version__ = '([^']+)'",
                    (ROOT / 'txlog/__init__.py').read_text(), re.M)[1]

setup(
      name='txlog',
      version=VERSION,
      long_description='Crash-resistant Python code made easy.',
      long_description_content_type='text/plain',
      python_requires='>=3.10,<4',
      description='Crash-resistant Python code made easy',
      url='https://github.com/labteral/txlog',
      author='Rodrigo Martínez',
      author_email='dev@brunneis.com',
      license='GNU General Public License v3.0',
      packages=find_packages(),
      zip_safe=False,
      classifiers=[
          "Development Status :: 4 - Beta",
          "Environment :: Console",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
          "Operating System :: POSIX :: Linux",
          "Programming Language :: Python :: 3.14",
          "Programming Language :: Python :: Implementation :: CPython",
          "Topic :: Software Development :: Libraries :: Python Modules",
      ],
      install_requires=['easyrocks>=4.2610.0,<5'],
)
