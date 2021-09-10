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
import socket
import shlex

def _storiksd_send(cmd):
	comm_dir = os.getenv("STORIKS_COMMUNICATION_DIR")
	if comm_dir is not None:
		sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		sock.connect(os.path.join(comm_dir, 'storiksd.socket'))

		sock.sendall(bytes(cmd, 'utf-8'))
		return str(sock.recv(4 * 1024 ** 2), 'utf-8')

	else:
		raise Exception('undefined environment variable STORIKS_COMMUNICATION_DIR')

send = _storiksd_send

def list():
	print(_storiksd_send('list'))

def status(number=''):
	print(_storiksd_send(f'status {number}'))

def cancel(number=''):
	print(_storiksd_send(f'cancel {number}'))

def schedule(cmd, output=None, append=False, overwrite=False, compress=False):
	prefix_cmd = ['schedule']
	if output is not None: prefix_cmd += ['--output', output]
	if append is True: prefix_cmd.append('--append')
	if overwrite is True: prefix_cmd.append('--overwrite')
	if compress is True: prefix_cmd.append('--compress')
	ret = _storiksd_send(f'{shlex.join(prefix_cmd)} {cmd}')
	print(ret)
