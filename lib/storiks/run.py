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
	"""Send a command to storiks daemon (storiksd)."""
	comm_dir = os.getenv("STORIKS_COMMUNICATION_DIR")
	if comm_dir is not None:
		sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		sock.connect(os.path.join(comm_dir, 'storiksd.socket'))

		sock.sendall(bytes(cmd, 'utf-8'))
		return str(sock.recv(4 * 1024 ** 2), 'utf-8')

	else:
		raise Exception('undefined environment variable STORIKS_COMMUNICATION_DIR')

send = _storiksd_send

def _storiks_send(cmd):
	"""Send a command to the current storiks experiment (if running)."""
	comm_dir = os.getenv("STORIKS_COMMUNICATION_DIR")
	if comm_dir is None:
		raise Exception('undefined environment variable STORIKS_COMMUNICATION_DIR')

	pathfile = os.path.join(comm_dir, 'storiks_socket.path')
	if not os.path.isfile(pathfile):
		raise Exception(f'file {pathfile} not found')

	with open(pathfile, 'rt') as f:
		sockpath = f.readline().strip()
		# print(f'DEBUG: sockpath={sockpath}')
		sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		sock.connect(sockpath)
		sock.sendall(bytes(cmd, 'utf-8'))
		return str(sock.recv(4 * 1024 ** 2), 'utf-8')

def send_exp(cmd):
	print(_storiks_send(cmd))

def list():
	"""Send the command "list" to storiksd.
	Print the list of commands scheduled in storiksd.

	:return: None
	"""
	print(_storiksd_send('list'))

def status(number=''):
	"""Send the command "status" to storiksd.
	Print the status of a command/experiment scheduled in storiksd.

	:param number: Number of the scheduled command. If not informed, the last one will be selected.
	:return: None
	"""
	print(_storiksd_send(f'status {number}'))

def cancel(number=''):
	"""Send the command "cancel" to storiksd.
	Cancel a command/experiment scheduled in storiksd.

	:param number: Number of the scheduled command to cancel. If not informed, the last one will be selected.
	:return: None
	"""
	print(_storiksd_send(f'cancel {number}'))

def schedule(cmd, output=None, append=False, overwrite=False, compress=False, workdir=None):
	"""Send the command "schedule" to storiksd.
	Schedule a command or experiment to be executed by storiksd.

	:param output: Output file name (absolute or relative to /output (--output_dir) inside the storiks container)
	:param append: Append the stdout and stderr to the output file instead of overwriting it.
	:param overwrite: Overwrite the output file. If both overwrite and append parameters are False,
	                  the experiment will fail if the informed output file name exists.
	:param compress: Compress the output file using xz.
	:return: None
	"""
	prefix_cmd = ['schedule']
	if output is not None: prefix_cmd += ['--output', output]
	if append is True: prefix_cmd.append('--append')
	if overwrite is True: prefix_cmd.append('--overwrite')
	if compress is True: prefix_cmd.append('--compress')
	workdir = workdir if workdir is not None else os.path.abspath('.')
	prefix_cmd += ['--workdir', workdir]
	ret = _storiksd_send(f'{shlex.join(prefix_cmd)} {cmd}')
	print(ret)
