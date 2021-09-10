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
import sys
import socketserver
import threading

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


# =============================================================================
class FakeLog:
	debug_active = False
	@classmethod
	def debug(cls, msg):
		if (cls.debug_active):
			sys.stderr.write(f'DEBUG: {msg}\n')
	@classmethod
	def info(cls, msg):
		sys.stderr.write(f'INFO: {msg}\n')
	@classmethod
	def warning(cls, msg):
		sys.stderr.write(f'WARN: {msg}\n')
	@classmethod
	def error(cls, msg):
		sys.stderr.write(f'ERROR: {msg}\n')

# =============================================================================
class SocketServer:
	_log           = FakeLog
	_filename      = None
	_server        = None
	_server_thread = None

	# https://docs.python.org/3/library/socketserver.html
	class ThreadedServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
		pass

	class ThreadedRequestHandler(socketserver.BaseRequestHandler):
		_handler_method = None

		def handle(self):
			self._handler_method()

	def __init__(self, filename, handler_method, chown=None, chmod=None, log=FakeLog):
		self._log = log
		self._filename = filename
		self.ThreadedRequestHandler._handler_method = handler_method

		if os.path.exists(filename):
			os.remove(filename)

		self._server = self.ThreadedServer(filename, self.ThreadedRequestHandler)
		self._server_thread = threading.Thread(name=filename, target=self._server.serve_forever)
		self._server_thread.daemon = True
		log.info(f'Starting {self.__class__.__name__} {filename}')
		self._server_thread.start()
		log.debug(f'{self.__class__.__name__} started')

		if chown is not None:
			os.chown(filename, chown[0], chown[1])
		if chmod is not None:
			os.chmod(filename, chmod)

	def __del__(self):
		self._log.info(f'Stopping {self.__class__.__name__} {self._filename}')
		self._server.shutdown()
		os.remove(self._filename)
