#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 16 08:54:54 2020

@author: Adriano Lange <alange0001@gmail.com>

Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
This source code is licensed under both the GPLv2 (found in the
LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
(found in the LICENSE.Apache file in the root directory).
"""

import os

def coalesce(*values):
	for v in values:
		if v is not None:
			return v
	return None

def getenv_default(envname, default):
	ev = os.getenv(envname)
	return coalesce(ev, default)

def get_recursive(value, *attributes):
	cur_v = value
	for i in attributes:
		try:
			cur_v = cur_v[i]
		except:
			return None
	return cur_v

def env_as_bool(envname, not_found=False, default=False, invalid=False):
	ev = os.getenv(envname)
	if ev is None:
		return not_found
	ev = ev.strip().lower()
	if ev == '':
		return default
	if ev in ['1', 't', 'true', 'y', 'yes']:
		return True
	if ev in ['0', 'f', 'false', 'n', 'no']:
		return False
	return invalid
