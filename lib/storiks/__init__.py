# -*- coding: utf-8 -*-
"""
@author: Adriano Lange <alange0001@gmail.com>

Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
This source code is licensed under both the GPLv2 (found in the
LICENSE file in the root directory) and Apache 2.0 License
(found in the LICENSE.Apache file in the root directory).
"""

from .version import *
__version__ = PROJECT_VERSION

__all__ = [
 	'PROJECT_NAME', 'PROJECT_VERSION', 'DOCKER_IMAGE',
	'util', 'plot', 'run'
]
__author__ = 'Adriano Lange <alange0001@gmail.com>'

from . import util
from . import plot
from . import run
