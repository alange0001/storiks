#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Adriano Lange <alange0001@gmail.com>

Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
This source code is licensed under both the GPLv2 (found in the
LICENSE file in the root directory) and Apache 2.0 License
(found in the LICENSE.Apache file in the root directory).
"""

import os
import sys
import signal
import traceback
import subprocess
import psutil
import argparse
import json
import collections
import re
import time

# =============================================================================
import logging
log = logging.getLogger('rocksdb_test_helper')
log.setLevel(logging.INFO)


# =============================================================================
class ExperimentList (collections.OrderedDict):
	def register(self, cls):
		self[cls.exp_name] = cls


experiment_list = ExperimentList()


# =============================================================================
class ArgsWrapper: # single global instance "args"
	def get_args(self):
		preparser = argparse.ArgumentParser("rocksdb_test helper", add_help=False)
		preparser.add_argument('-l', '--log_level', type=str,
			default='INFO', choices=[ 'debug', 'DEBUG', 'info', 'INFO' ],
			help='Log level.')
		preparser.add_argument('--load_args', type=str,
			default=None,
			help='Load arguments from file (JSON).')
		preparser.add_argument('--save_args', type=str,
			default=None,
			help='Save arguments to a file (JSON).')
		preargs, remainargv = preparser.parse_known_args()

		log_h = logging.StreamHandler()
		# log_h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
		log_h.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
		log.addHandler(log_h)
		log.setLevel(getattr(logging, preargs.log_level.upper()))

		log.debug(f'Preargs: {str(preargs)}')

		argcheck_path(preargs, 'load_args', required=False, absolute=False,  type='file')
		if preargs.load_args is not None:
			log.info(f'Loading arguments from file "{preargs.load_args}".')
			with open(preargs.load_args, 'r') as fd:
				load_args = json.load(fd)
			log.info(f'    loaded arguments: {load_args}')
		else:
			load_args = {}

		parser = argparse.ArgumentParser(
			description="rocksdb_test helper")
		parser.add_argument('-l', '--log_level', type=str,
			default='INFO', choices=[ 'debug', 'DEBUG', 'info', 'INFO' ],
			help='Log level.')
		parser.add_argument('--load_args', type=str,
			default=None,
			help='Load arguments from file (JSON).')
		parser.add_argument('--save_args', type=str,
			default=None,
			help='Save arguments to a file (JSON).')

		parser.add_argument('--output_path', type=str,
			default=coalesce(load_args.get('output_path'), '.'),
			help='Output directory of the experiments (default=./).')

		parser.add_argument('--rocksdb_test_path', type=str,
			default=load_args.get('rocksdb_test_path'),
			help='Directory of rocksdb_test (optional).')
		parser.add_argument('--rocksdb_path', type=str,
			default=load_args.get('rocksdb_path'),
			help='Directory of rocksdb (optional).')
		parser.add_argument('--ycsb_path', type=str,
			default=load_args.get('ycsb_path'),
			help='Directory of YCSB (optional).')

		parser.add_argument('--confirm_cmd', type=str, nargs='?', const=True,
			default=coalesce(load_args.get('confirm_cmd'), False),
			help='Confirm before each command execution.')

		parser.add_argument('--test', type=str,
			default='',
			help='Test routines.')

		subparsers = parser.add_subparsers(dest='experiment', title='Experiments available',
		                                   description='Use "./rocksdb_test_helper <experiment_name> -h" to list ' +
		                                               'the arguments of each experiment.')
		for eclass in experiment_list.values():
			eclass.register_subcommand(subparsers, load_args)

		args = parser.parse_args(remainargv)
		log.debug(f'Arguments: {str(args)}')

		argcheck_bool(args, 'confirm_cmd', required=True)

		argcheck_path(args, 'output_path',       required=True,  absolute=False, type='dir')
		argcheck_path(args, 'rocksdb_test_path', required=False, absolute=True,  type='dir')
		argcheck_path(args, 'rocksdb_path',      required=False, absolute=True,  type='dir')
		argcheck_path(args, 'ycsb_path',         required=False, absolute=True,  type='dir')

		if preargs.save_args is not None:
			args_d = collections.OrderedDict()
			for k in dir(args):
				if k[0] != '_' and k not in ['test', 'log_level', 'save_args', 'load_args']:
					args_d[k] = getattr(args, k)
			log.info(f'saving arguments to file: {preargs.save_args}')
			with open(preargs.save_args, 'w') as f:
				json.dump(args_d, f)
				f.write('\n')

		return args

	def __getattr__(self, name):
		global args
		args = self.get_args()
		return getattr(args, name)


def argcheck_bool(args, arg_name, required=False):
	log.debug(f'argcheck_bool: --{arg_name}, required={required}')
	value, setf = value_setf(args, arg_name)
	log.debug(f'argcheck_bool: --{arg_name}="{value}"')
	if value is None:
		if required:  raise ValueError(f'argument --{arg_name} is required')
		else:         return
	elif isinstance(value, bool):
		return
	elif isinstance(value, str) and value.lower().strip() in ['true', 't', 'yes', 'y', '1']:
		setf(True)
	elif isinstance(value, str) and value.lower().strip() in ['false', 'f', 'no', 'n', '0']:
		setf(False)
	else:
		raise ValueError(f'invalid value for boolean argument --{arg_name}="{value}"')


def argcheck_path(args, arg_name, required=False, absolute=False, type='file'):
	log.debug(f'argcheck_path: --{arg_name}, type="{type}", required={required}, absolute={absolute}')
	check_f = getattr(os.path, f'is{type}')

	value, setf = value_setf(args, arg_name)
	log.debug(f'argcheck_path: --{arg_name}="{value}"')

	if value is None:
		if required:  raise ValueError(f'argument --{arg_name} is required')
		else:         return
	elif isinstance(value, str) and check_f(value):
		if absolute:
			setf( os.path.abspath(value) )
	else:
		raise ValueError(f'argument --{arg_name}="{value}" is not a directory')


args = ArgsWrapper()


# =============================================================================
class GenericExperiment:
	exp_name = 'generic'
	parser_args = {
		'help':        'experiment help (override)',
		'description': 'experiment description (override)'
		}
	arg_groups = ['gen']
	output_filename = None

	helper_params = collections.OrderedDict([
		('data_path',           {'group': 'gen', 'type': str,  'default': None,        'register': True,  'help': 'Directory used to store both database and access_time3 files.' }),
		('output_prefix',       {'group': 'gen', 'type': str,  'default': '',          'register': True,  'help': 'Output prefix.' }),
		('output_suffix',       {'group': 'gen', 'type': str,  'default': '',          'register': True,  'help': 'Output suffix.' }),
		('compress_output',     {'group': 'gen', 'type': bool, 'default': True,        'register': True,  'help': 'Compress the output file during the experiment.' }),
		('before_run_cmd',      {'group': 'gen', 'type': str,  'default': None,        'register': True,  'help': 'Execute this command before running the rocksdb_test.' }),
		('after_run_cmd',       {'group': 'gen', 'type': str,  'default': None,        'register': True,  'help': 'Execute this command after running the rocksdb_test.' }),
		('backup_dbbench',      {'group': 'dbb', 'type': str,  'default': None,        'register': True,  'help': 'Restore the database backup used by the db_bench instances from this .tar file (don\'t use subdir).' }),
		('backup_ycsb',         {'group': 'ydb', 'type': str,  'default': None,        'register': True,  'help': 'Restore the database backup used by the YCSB instances from this .tar file (don\'t use subdirs).' }),
		('ydb_workload_list',   {'group': 'ydb', 'type': str,  'default': 'workloadb', 'register': False, 'help': 'List of YCSB workloads (space separated).' }),
		('at_block_size_list',  {'group': 'at3', 'type': str,  'default': '512',       'register': False, 'help': 'List of block sizes used in the experiments with access_time3 (space separated).' }),
		('at_iodepth_list',     {'group': 'at3', 'type': str,  'default': '1',         'register': False, 'help': 'List of iodepths used in the experiments with access_time3 (space separated).' }),
		('at_script_gen',       {'group': 'at3', 'type': int,  'default': 1,           'register': True,  'help': 'Access_time3 script generator (0 - 4).' }),
		('at_interval',         {'group': 'at3', 'type': int,  'default': 2,           'register': False, 'help': 'Interval between changes in the access pattern of the access_time3 instances.' }),
		('at_random_ratio',     {'group': 'at3', 'type': float,'default': 0.5,         'register': False, 'help': 'Random ratio (--random_ratio) used by access_time3 instances.' }),
		('at_file_size',        {'group': 'at3', 'type': int,  'default': 10000,       'register': False, 'help': 'Interval between changes in the access pattern of the access_time3 instances.' }),
		])

	exp_params = collections.OrderedDict([
	#	('docker_image',        {'group': 'gen', 'type': str,  'default': None,         'register':True,  'help': 'Docker image (optional). Use rocksdb_test\'s default if not informed.' }),
		('docker_params',       {'group': 'gen', 'type': str,  'default': None,         'register':True,  'help': 'Additional docker arguments.' }),
		('duration',            {'group': 'gen', 'type': int,  'default': None,         'register':True,  'help': 'Duration of the experiment (in minutes).' }),
		('warm_period',         {'group': 'gen', 'type': int,  'default': None,         'register':True,  'help': 'Warm period (in minutes).' }),
		('stats_interval',      {'group': 'gen', 'type': int,  'default': None,         'register':True,  'help': 'Stats report interval.' }),
		('rocksdb_config_file', {'group': 'gen', 'type': str,  'default': None,         'register':False, 'help': 'Rocksdb config file used to create the database.' }),
		('num_dbs',             {'group': 'dbb', 'type': int,  'default': 0,            'register':True,  'help': 'Number of db_bench instances.' }),
		('db_create',           {'group': 'dbb', 'type': str,  'default': 'true',       'register':False, 'help': None }),
		('db_num_keys',         {'group': 'dbb', 'type': str,  'default': '10000',      'register':True,  'help': 'Number of keys used by db_bench (default=10000). Use a greater value for realistic experiments (e.g., 500000000).' }),
		('db_path',             {'group': 'dbb', 'type': str,  'default': None,         'register':False, 'help': 'One database directory for each instance of db_bench separated by "#". This argument is configured automatically (<DATA_PATH>/rocksdb_0#<DATA_PATH>/rocksdb_1#...).' }),
		('db_benchmark',        {'group': 'dbb', 'type': str,  'default': 'readwhilewriting', 'register': True, 'help': 'db_bench workload (default=readwhilewriting).' }),
		('db_threads',          {'group': 'dbb', 'type': str,  'default': '9',          'register':True,  'help': 'Number of threads used by the db_bench workload (default=9).' }),
	#	('db_cache_size',       {'group': 'dbb', 'type': str,  'default': '536870912',  'register':True,  'help': 'Cache size used by db_bench (default=512MiB).' }),
		('num_ydbs',            {'group': 'ydb', 'type': int,  'default': 0,            'register':True,  'help': 'Number of YCSB instances.' }),
		('ydb_create',          {'group': 'ydb', 'type': str,  'default': None,         'register':False, 'help': None }),
		('ydb_num_keys',        {'group': 'ydb', 'type': str,  'default': '10000',      'register':True,  'help': 'Number of keys used by YCSB (default=10000). Use a greater value for realistic experiments (e.g., 50000000).' }),
		('ydb_path',            {'group': 'ydb', 'type': str,  'default': None,         'register':False, 'help': 'One database directory for each instance of YCSB separated by "#". This argument is configured automatically (<DATA_PATH>/rocksdb_ycsb_0#<DATA_PATH>/rocksdb_ycsb_1#...).' }),
		('ydb_threads',         {'group': 'ydb', 'type': str,  'default': '5',          'register':True,  'help': 'Number of threads used by YCSB workload (default=5).' }),
		('ydb_workload',        {'group': 'ydb', 'type': str,  'default': None,         'register':False, 'help': 'YCSB workload (default=workloadb).' }),
	#	('ydb_sleep',           {'group': 'ydb', 'type': str,  'default': '0',          'register':True,  'help': 'Sleep before executing each YCSB instance (in minutes, separated by "#").' }),
	#	('ydb_rocksdb_jni',     {'group': 'ydb', 'type': str,  'default': None,         'register':True,  'help': 'Rocksdb binding used by YCSB.' }),
	#	('ydb_socket',          {'group': 'ydb', 'type': str,  'default': '0',          'register':True,  'help': 'Activates the socket server for RocksDB''s internal statistics. Modified version of YCSB.' }),
		('num_at',              {'group': 'at3', 'type': int,  'default': 0,            'register':True,  'help': 'Number of access_time3 instances.' }),
		('at_dir',              {'group': 'at3', 'type': str,  'default': None,         'register':True,  'help': 'Directory containing the files used by the access_time3 instances. By default, this argument is configured automatically (<DATA_PATH>/tmp).' }),
		('at_file',             {'group': 'at3', 'type': str,  'default': None,         'register':False, 'help': 'Files used by the access_time3 instances (separated by #). Also configured automatically.' }),
		('at_block_size',       {'group': 'at3', 'type': str,  'default': None,         'register':False, 'help': 'Block size used by the access_time3 instances (default=512).' }),
		('at_io_engine',        {'group': 'at3', 'type': str,  'default': None,         'register':True,  'help': 'I/O engine used by the access_time3 instances.' }),
		('at_iodepth',          {'group': 'at3', 'type': str,  'default': None,         'register':False, 'help': 'I/O depth used by the access_time3 instances (default=1).' }),
		('at_o_direct',         {'group': 'at3', 'type': str,  'default': None,         'register':True,  'help': 'Use O_DIRECT in the access_time3 instances.' }),
		('at_o_dsync',          {'group': 'at3', 'type': str,  'default': None,         'register':True,  'help': 'Use O_DSYNC in the access_time3 instances.' }),
		('at_params',           {'group': 'at3', 'type': str,  'default': None,         'register':True,  'help': 'Extra access_time3 arguments, if necessary.' }),
		('at_script',           {'group': 'at3', 'type': str,  'default': None,         'register':False, 'help': 'Access_time3 script (separated by "#"). Generated automatically by experiments ycsb_at3 and dbbench_at3.' }),
	#	('perfmon',             {'group': 'gen', 'type': str,  'default': None,         'register':True,  'help': 'Connect to performancemonitor.' }),
	#	('perfmon_port',        {'group': 'gen', 'type': int,  'default': None,         'register':True,  'help': 'performancemonitor port' }),
		('commands',            {'group': 'gen', 'type': str,  'default': None,         'register':True,  'help': 'Commands used to control the experiments.' }),
		('params',              {'group': 'gen', 'type': str,  'default': None,         'register':True,  'help': 'Extra rocksdb_test arguments.' }),
		])

	@classmethod
	def register_subcommand(cls, subparsers, load_args):
		parser = subparsers.add_parser(cls.exp_name, **cls.parser_args)
		cls.register_args(parser, load_args)
		
	@classmethod
	def change_args(cls, load_args):  # override if necessary
		log.debug(f'GenericExperiment.change_args()')
		
	@classmethod
	def register_args(cls, parser, load_args):
		log.debug(f'GenericExperiment.register_args()')
		groups = cls.arg_groups

		# copy and filter helper_params:
		filtered = collections.OrderedDict()
		for k, v in cls.helper_params.items():
			if v['group'] in groups:
				filtered[k] = v.copy()
		cls.helper_params = filtered

		# copy and filter exp_params:
		filtered = collections.OrderedDict()
		for k, v in cls.exp_params.items():
			if v['group'] in groups:
				filtered[k] = v.copy()
		cls.exp_params = filtered
		
		cls.change_args(load_args)

		# register helper_params
		for k, v in cls.helper_params.items():
			if v.get('register') is True:
				parser.add_argument(f'--{k}', type=v['type'], default=coalesce(load_args.get(k), v.get('default')),
				                    help=v.get('help'))
			
		# register exp_params
		for k, v in cls.exp_params.items():
			if v.get('register') is True:
				parser.add_argument(f'--{k}', type=v['type'], default=coalesce(load_args.get(k), v.get('default')),
				                    help=v.get('help'))

	def get_args_d(self):
		log.debug(f'GenericExperiment.get_args_d()')
		return self.process_args_d( args_to_dir(args) )

	def process_args_d(self, args_d):
		log.debug(f'GenericExperiment.process_args_d()')
		num_dbs  = coalesce(args_d.get('num_dbs'), 0)
		num_ydbs = coalesce(args_d.get('num_ydbs'), 0)

		if num_dbs > 0 and num_ydbs > 0:
			if args_d.get('data_path') or not os.path.isdir(args_d.get('data_path')) is None:
				raise Exception(f'invalid data_path: {args_d.get("data_path")}')

		if num_dbs > 0:
			args_d['db_path'] = '#'.join([f'{args_d["data_path"]}/rocksdb_{x}' for x in range(0, num_dbs)])

		if num_ydbs > 0:
			args_d['ydb_path'] = '#'.join([f'{args_d["data_path"]}/rocksdb_ycsb_{x}' for x in range(0, num_ydbs)])

		if coalesce(args_d.get('num_at'), 0) > 0:
			args_d['at_dir']  = coalesce(args_d.get('at_dir'), f'{args_d["data_path"]}/tmp')
			argcheck_path(args_d, 'at_dir', required=True, absolute=True, type='dir')
			args_d['at_file'] = '#'.join([ str(x) for x in range(0, args_d['num_at']) ])

		docker_params = [coalesce(args_d.get('docker_params'), '')]
		for k, v in [('rocksdb_test_path', '/opt/rocksdb_test'),
		             ('rocksdb_path', '/opt/rocksdb'),
		             ('ycsb_path', '/opt/YCSB')]:
			if args_d.get(k) is not None:
				docker_params.append(f'-v {args_d[k]}:{v}')
		args_d['docker_params'] = ' '.join(docker_params)

		return args_d

	def run(self, args_d=None):
		log.debug(f'GenericExperiment.run()')
		args_d = coalesce(args_d, self.get_args_d())
		bin_path = get_rocksdb_bin()

		log.info('')
		log.info('==========================================')

		cmd  = f'{bin_path} \\\n'

		output_path = coalesce(args_d.get('output_path'), '')
		if self.output_filename is None:
			raise Exception('output file not defined')
		args_d['output_filename'] = f'{args_d["output_prefix"]}{self.output_filename}{args_d["output_suffix"]}.out'
		args_d['output'] = str(os.path.join(output_path, args_d['output_filename']))
		log.info(f'rocksdb_test output file              = {args_d["output"]}')

		def_p_func = lambda k, v: f'	--{k}="{args_d[k]}" \\\n'
		if self.exp_params.get('params') is not None:
			self.exp_params['params']['p_func'] = lambda k, v: f'	{args_d[k]}'

		for k, v in self.exp_params.items():
			arg_v = args_d.get(k)
			if arg_v is not None:
				log.info(f'rocksdb_test arg {k:<20} = {arg_v}')
				p_func = coalesce(v.get('p_func'), def_p_func)
				cmd += p_func(k, v)

		if args_d.get('compress_output') is True:
			cmd += f' |nice -n 10 xz -c9 --flush-timeout=30000 > "{args_d["output"]}.xz"'
		else:
			cmd += f' > "{args_d["output"]}"'

		self.before_run(args_d)

		try:
			if args_d.get('before_run_cmd') is not None:
				command(args_d.get('before_run_cmd'), cmd_args=args_d)
		except Exception as e:
			log.error(f'before_run_cmd = "{args_d.get("before_run_cmd")}" returned exception: {str(e)}')

		log.info(f'Executing rocksdb_test experiment {args_d.get("experiment")} ...')
		args_d['exit_code'] = command(cmd, raise_exception=False)
		log.info(f'Experiment {args_d.get("experiment")} finished.')

		if coalesce(args_d.get('exit_code'), 0) != 0:
			log.error(f"rocksdb_test returned error code {args_d.get('exit_code')}. " +
			          f"Check output file \"{self.output_filename}\"")

		try:
			if args_d.get('after_run_cmd') is not None:
				command(args_d.get('after_run_cmd'), cmd_args=args_d)
		except Exception as e:
			log.error(f'after_run_cmd = "{args_d.get("after_run_cmd")}" returned exception: {str(e)}')

	def before_run(self, args_d):
		self.restore_dbs(args_d)
		self.test_paths(args_d)

	def test_paths(self, args_d):
		if coalesce(args_d.get('num_ydbs'), 0) > 0:
			for db in args_d['ydb_path'].split('#'):
				test_path(f'{db}/CURRENT')
		if coalesce(args_d.get('num_dbs'), 0) > 0:
			for db in args_d['db_path'].split('#'):
				test_path(f'{db}/CURRENT')

	def remove_old_dbs(self, args_d):
		data_path = args_d["data_path"]
		log.info(f'Removing old database directores from data_path ({data_path}) ...')
		rm_entries = []
		for p in os.listdir(data_path):
			entry = os.path.join(data_path, p)
			log.debug(f'remove_old_dbs: entry = {entry}')
			if os.path.isdir(entry):
				r = re.findall(r'(rocksdb_(ycsb_){0,1}[0-9]+)', entry)
				log.debug(f'remove_old_dbs: r = {r}')
				if len(r) > 0:
					rm_entries.append(entry)
					log.info(f'\t{entry}')
		log.debug(f'remove_old_dbs: rm_entries = {rm_entries}')
		rm_entries = [f"'{x}'" for x in rm_entries]
		if len(rm_entries) > 0:
			command(f'rm -fr {" ".join(rm_entries)}')

	def restore_dbs(self, args_d):
		if coalesce(args_d.get('backup_ycsb'), '') != '' and coalesce(args_d.get('num_ydbs'), 0) > 0:
			argcheck_path(args_d, 'backup_ycsb',    required=True, absolute=False, type='file')
			log.info(f"Using database backup file: {args_d['backup_ycsb']}")
			tarfile = args_d['backup_ycsb']
			self.remove_old_dbs(args_d)
			for db in args_d['ydb_path'].split('#'):
				log.info(f'Restoring backup on directory {db}..')
				if os.path.exists(db):
					raise Exception(f'cannot restore backup on an existing path: {db}')
				command(f'mkdir -p "{db}"')
				command(f'tar -xf "{tarfile}" -C "{db}"')

		if coalesce(args_d.get('backup_dbbench'), '') != '' and coalesce(args_d.get('num_dbs'), 0) > 0:
			argcheck_path(args_d, 'backup_dbbench', required=True, absolute=False, type='file')
			log.info(f"Using database backup file: {args_d['backup_dbbench']}")
			tarfile = args_d['backup_dbbench']
			self.remove_old_dbs(args_d)
			for db in args_d['db_path'].split('#'):
				log.info(f'Restoring backup on directory {db}..')
				if os.path.exists(db):
					raise Exception(f'cannot restore backup on an existing path: {db}')
				command(f'mkdir -p "{db}"')
				command(f'tar -xf "{tarfile}" -C "{db}"')


# =============================================================================
class ExpCreateYcsb (GenericExperiment):
	exp_name = 'create_ycsb'
	parser_args = {
		'help':        'Creates the database used by the experiments with YCSB benchmark.',
		'description': 'This subcommand creates the database used by the experiments with YCSB benchmark. ' +
		               'After database creation, it executes <YDB_WORKLOAD> for <DURATION> minutes and then ' +
		               'creates a backup into the file <BACKUP_YCSB>, if informed.'
		}
	arg_groups = ['gen', 'ydb']

	@classmethod
	def change_args(cls, load_args):
		log.debug(f'create_ycsb.change_args()')
		
		cls.exp_params['duration']['default']     = 60
		cls.exp_params['warm_period']['default']  = 0
		cls.exp_params['num_ydbs']['default']     = 1
		cls.exp_params['ydb_threads']['default']  = 4
		cls.exp_params['rocksdb_config_file']['register'] = True
		cls.exp_params['rocksdb_config_file']['default']  = get_default_rocksdb_options()
		cls.exp_params['ydb_workload']['register'] = True
		cls.exp_params['ydb_workload']['default']  = 'workloada'

	def run(self):
		log.debug(f'create_ycsb.run()')
		args_d = self.get_args_d()
		args_d['num_ydbs'] = 1
		args_d['ydb_create'] = 'true'
		self.output_filename = f'ycsb_create'

		backup_file = coalesce(args_d.get('backup_ycsb'), '').strip()
		if backup_file != '' and os.path.exists(backup_file):
			raise Exception(f'YCSB database backup file already exists: {backup_file}')

		super(self.__class__, self).run(args_d)

		if coalesce(args_d.get('exit_code'), 0) != 0:
			raise Exception(f"Database creation returned error code {args_d.get('exit_code')}. " +
			                f"Check output file \"{self.output_filename}\"")

		self.test_paths(args_d)

		if coalesce(args_d.get('exit_code'), 0) == 0 and coalesce(backup_file, '') != '':
			log.info(f'Creating backup file "{backup_file}" ...')
			db = args_d['ydb_path'].split('#')[0]
			command(f'tar -C "{db}" -cf {backup_file} .')

	def before_run(self, args_d):
		db = args_d['ydb_path'].split('#')[0]

		log.info('Removing old database directory ...')
		command(f'rm -fr "{db}"')

		log.info(f'Creating database directory {db} ...')
		command(f'mkdir -p "{db}"')


experiment_list.register(ExpCreateYcsb)


# =============================================================================
class ExpYcsb (GenericExperiment):
	exp_name = 'ycsb'
	parser_args = {
		'help':        'Executes the YCSB benchmark.',
		'description': 'This experiment executes <NUM_YDBS> simultaneous instances of the YCSB benchmark ' +
		               'for <DURATION> minutes, including <WARM_PERIOD> minutes of warmup. ' +
		               'If <BACKUP_YCSB> is informed, it removes the old database directories and restores the ' +
		               'backup before the experiment.',
		}
	arg_groups = ['gen', 'ydb']

	@classmethod
	def change_args(cls, load_args):
		log.debug(f'Exp_ycsb.change_args()')
		cls.helper_params['ydb_workload_list']['register'] = True

		cls.exp_params['duration']['default']     = 90
		cls.exp_params['warm_period']['default']  = 30
		cls.exp_params['num_ydbs']['default']     = 1
		cls.exp_params['rocksdb_config_file']['register'] = True
		cls.exp_params['rocksdb_config_file']['default']  = ''

	def run(self):
		log.debug(f'Exp_ycsb.run()')
		args_d = self.get_args_d()

		for ydb_workload in args_d['ydb_workload_list'].split(' '):
			args_d['ydb_workload'] = ydb_workload
			self.output_filename = f'ycsb_{ydb_workload}'

			super(self.__class__, self).run(args_d)


experiment_list.register(ExpYcsb)


# =============================================================================
class ExpYcsbAt3 (GenericExperiment):
	exp_name = 'ycsb_at3'
	parser_args = {
		'help':        'YCSB benchmark + access_time3.',
		'description': 'This experiment executes, simultaneously, <NUM_YDBS> YCSB and <NUM_AT> access_time3 ' +
		               'instances for <DURATION> minutes, including <WARM_PERIOD> minutes of warmup. ' +
		               'If <BACKUP_YCSB> is informed, it removes the old database directories and restores the ' +
		               'backup before the experiment.',
		}
	arg_groups = ['gen', 'ydb', 'at3']

	@classmethod
	def change_args(cls, load_args):
		log.debug(f'Exp_ycsb_at3.change_args()')
		cls.helper_params['ydb_workload_list']['register']  = True
		cls.helper_params['at_block_size_list']['register'] = True
		cls.helper_params['at_iodepth_list']['register']    = True
		cls.helper_params['at_interval']['register']        = True
		cls.helper_params['at_random_ratio']['register']    = True

		cls.exp_params['duration']['default']     = -1
		cls.exp_params['warm_period']['default']  = 30
		cls.exp_params['num_ydbs']['default']     = 1
		cls.exp_params['rocksdb_config_file']['register'] = True
		cls.exp_params['rocksdb_config_file']['default']  = ''
		cls.exp_params['num_at']['default']       = 4
		cls.exp_params['at_params']['default']    = '--flush_blocks=0 --wait'

	def run(self):
		log.debug(f'Exp_ycsb_at3.run()')
		args_d = self.get_args_d()

		cur_at_script_gen = coalesce(args_d.get('at_script_gen'), 1)
		args_d['at_script'], at_duration = get_at3_script(script_gen=cur_at_script_gen,
		                                                  warm=int(args_d['warm_period']),
		                                                  instances=int(args_d['num_at']),
		                                                  interval=int(args_d['at_interval']))
		if coalesce(args_d.get('duration'), -1) < 0:
			args_d['duration'] = at_duration

		if args_d.get('at_random_ratio') is not None:
			args_d['at_params'] = coalesce(args_d.get('at_params'), '') + f" --random_ratio={args_d['at_random_ratio']}"

		for at_bs in args_d['at_block_size_list'].split(' '):
			args_d['at_block_size'] = at_bs
			for ydb_workload in args_d['ydb_workload_list'].split(' '):
				args_d['ydb_workload'] = ydb_workload
				for iodepth in args_d['at_iodepth_list'].split(' '):
					args_d['at_iodepth'] = iodepth

					cur_at_io_engine = coalesce(args_d.get("at_io_engine"), "def")
					cur_at_o_dsync = coalesce(args_d.get("at_o_dsync"), "def")
					self.output_filename = \
						f'ycsb_{ydb_workload}' + \
						f'-at3' + \
						f'_pres{cur_at_script_gen}' + \
						f'n{args_d["num_at"]}i{args_d["at_interval"]}' + \
						f'_depth{iodepth}_eng_{cur_at_io_engine}' + \
						f'_dsync_{cur_at_o_dsync}' + \
						f'_bs{at_bs}'
					super(self.__class__, self).run(args_d)


experiment_list.register(ExpYcsbAt3)


# =============================================================================
class ExpCreateDbbench (GenericExperiment):
	exp_name = 'create_dbbench'
	parser_args = {
		'help':        'Creates the database used by the experiments with db_bench.',
		'description': 'This subcommand creates the database used by the experiments with db_bench. ' +
		               'After database creation, it executes <DB_BENCHMARK> for <DURATION> minutes and then ' +
		               'creates a backup into the file <BACKUP_DBBENCH>, if informed.',
		}
	arg_groups = ['gen', 'dbb']

	@classmethod
	def change_args(cls, load_args):
		log.debug(f'Exp_create_dbbench.change_args()')
		cls.exp_params['duration']['default']      = 60
		cls.exp_params['warm_period']['default']   = 0
		cls.exp_params['num_dbs']['default']       = 1
		cls.exp_params['db_threads']['default']    = 9
		cls.exp_params['rocksdb_config_file']['default']  = get_default_rocksdb_options()
		cls.exp_params['rocksdb_config_file']['register'] = True

	def run(self):
		log.debug(f'Exp_create_dbbench.run()')
		args_d = self.get_args_d()
		args_d['num_dbs']   = 1
		args_d['db_create'] = True
		self.output_filename = f'dbbench_create'

		backup_file = coalesce(args_d.get('backup_dbbench'), '').strip()
		if backup_file != '' and os.path.exists(backup_file):
			raise Exception(f'db_bench database backup file already exists: {backup_file}')

		super(self.__class__, self).run(args_d)

		if coalesce(args_d.get('exit_code'), 0) != 0:
			raise Exception(f"Database creation returned error code {args_d.get('exit_code')}. " +
			                f"Check output file \"{self.output_filename}\"")

		self.test_paths(args_d)

		if coalesce(args_d.get('exit_code'), 0) == 0 and backup_file != '':
			log.info(f'Creating backup file "{backup_file}" ...')
			db = args_d['db_path'].split('#')[0]
			command(f'tar -C "{db}" -cf {backup_file} .')

	def before_run(self, args_d):
		log.debug(f'Exp_create_dbbench.before_run()')
		db = args_d['db_path'].split('#')[0]

		log.info('Removing old database directory ...')
		command(f'rm -fr "{db}"')

		log.info(f'Creating database directory {db} ...')
		command(f'mkdir -p "{db}"')


experiment_list.register(ExpCreateDbbench)


# =============================================================================
class ExpDbbench (GenericExperiment):
	exp_name = 'dbbench'
	parser_args = {
		'help':        'Executes the db_bench benchmark.',
		'description': 'This experiment executes <NUM_DBS> simultaneous instances of db_bench for <DURATION> ' +
		               'minutes, including <WARM_PERIOD> minutes of warmup. If <BACKUP_DBBENCH> is informed, ' +
		               'it also removes the old database directories and restores the backup before the experiment.',
		}
	arg_groups = ['gen', 'dbb']

	@classmethod
	def change_args(cls, load_args):
		log.debug(f'Exp_dbbench.change_args()')
		cls.exp_params['duration']['default']     = 90
		cls.exp_params['warm_period']['default']  = 30
		cls.exp_params['num_dbs']['default']      = 1

	def run(self):
		log.debug(f'Exp_dbbench.run()')
		args_d = self.get_args_d()

		self.output_filename = f'dbbench_{args_d.get("db_benchmark")}'
		super(self.__class__, self).run(args_d)


experiment_list.register(ExpDbbench)


# =============================================================================
class ExpDbbenchAt3 (GenericExperiment):
	exp_name = 'dbbench_at3'
	parser_args = {
		'help':        'db_bench + access_time3.',
		'description': 'This experiment executes, simultaneously, <NUM_DBS> db_bench and <NUM_AT> access_time3 ' +
		               'instances for <DURATION> minutes, including <WARM_PERIOD> minutes of warmup. ' +
		               'If <BACKUP_DBBENCH> is informed, it removes the old database directories and restores ' +
		               'the backup before the experiment.',
		}
	arg_groups = ['gen', 'dbb', 'at3']

	@classmethod
	def change_args(cls, load_args):
		log.debug(f'Exp_dbbench_at3.change_args()')
		cls.helper_params['at_block_size_list']['register'] = True
		cls.helper_params['at_iodepth_list']['register']    = True
		cls.helper_params['at_interval']['register']        = True
		cls.helper_params['at_random_ratio']['register']    = True

		cls.exp_params['duration']['default']     = -1
		cls.exp_params['warm_period']['default']  = 30
		cls.exp_params['num_dbs']['default']      = 1
		cls.exp_params['num_at']['default']       = 4
		cls.exp_params['at_params']['default']    = '--flush_blocks=0 --wait'

	def run(self):
		log.debug(f'Exp_dbbench_at3.run()')
		args_d = self.get_args_d()

		cur_at_script_gen = coalesce(args_d.get('at_script_gen'), 1)
		args_d['at_script'], at_duration = get_at3_script(script_gen=cur_at_script_gen,
		                                                  warm=int(args_d['warm_period']),
		                                                  instances=int(args_d['num_at']),
		                                                  interval=int(args_d['at_interval']))
		if coalesce(args_d.get('duration'), -1) < 0:
			args_d['duration'] = at_duration

		if args_d.get('at_random_ratio') is not None:
			args_d['at_params'] = coalesce(args_d.get('at_params'), '') + f" --random_ratio={args_d['at_random_ratio']}"

		for at_bs in args_d['at_block_size_list'].split(' '):
			args_d['at_block_size'] = at_bs
			for iodepth in args_d['at_iodepth_list'].split(' '):
				args_d['at_iodepth'] = iodepth

				cur_at_io_engine = coalesce(args_d.get("at_io_engine"), "def")
				cur_at_o_dsync = coalesce(args_d.get("at_o_dsync"), "def")
				self.output_filename = \
					f'dbbench_{args_d.get("db_benchmark")}' + \
					f'-at3' + \
					f'_pres{cur_at_script_gen}' + \
					f'n{args_d["num_at"]}i{args_d["at_interval"]}' + \
					f'_depth{iodepth}_eng_{cur_at_io_engine}' + \
					f'_dsync_{cur_at_o_dsync}' + \
					f'_bs{at_bs}'
				super(self.__class__, self).run(args_d)


experiment_list.register(ExpDbbenchAt3)


# =============================================================================
class ExpCreateAt3 (GenericExperiment):
	exp_name = 'create_at3'
	parser_args = {
		'help':        'Creates the data files used by the access_time3 instances.',
		'description': 'This subcommand creates the data files used by the <NUM_AT> access_time3 instances. ' +
		               'These files will be stored in <AT_DIR> and have <AT_FILE_SIZE> MiB each.'
		}
	arg_groups = ['gen', 'at3']

	@classmethod
	def change_args(cls, load_args):
		log.debug(f'Exp_create_at3.change_args()')
		cls.helper_params['at_file_size']['register'] = True

		cls.exp_params['duration']['register'] = False
		cls.exp_params['warm_period']['register'] = False
		cls.exp_params['num_at']['default'] = 4

	def process_args_d(self, args_d):
		log.debug(f'Exp_create_at3.process_args_d()')

		if args_d.get('data_path') is None and args_d.get('at_dir') is None:
			raise Exception('at least one of the following parameters must be defined: data_path and at_dir')

		if args_d.get('at_dir') is not None and not os.path.isdir(args_d.get('at_dir')):
			raise Exception(f'at_dir is not a valid directory: {args_d.get("at_dir")}')

		if args_d.get('at_dir') is None:
			if not os.path.isdir(args_d.get('data_path')):
				raise Exception(f'data_path is not a valid directory: {args_d.get("data_path")}')
			args_d['at_dir'] = os.path.join(args_d['data_path'], 'tmp')
			if not os.path.isdir(args_d['at_dir']):
				os.mkdir(args_d['at_dir'])

		return super(self.__class__, self).process_args_d(args_d)

	def run(self):
		log.debug(f'Exp_create_at3.run()')
		args_d = self.get_args_d()
		args_d['duration'] = 1
		args_d['warm_period'] = 0
		args_d['at_params'] = f' --create_file --filesize={args_d.get("at_file_size")} ' +\
		                      f'{coalesce(args_d.get("at_params"), "")}'
		self.output_filename = f'at3_create'

		super(self.__class__, self).run(args_d)

		if coalesce(args_d.get('exit_code'), 0) != 0:
			raise Exception(f"File creation returned error code {args_d.get('exit_code')}. " +
			                f"Check output file \"{self.output_filename}\"")

	def before_run(self, args_d):
		pass


experiment_list.register(ExpCreateAt3)


# =============================================================================
def command_output(cmd, raise_exception=True):
	log.debug(f'Executing command: {cmd}')
	err, out = subprocess.getstatusoutput(cmd)
	if err != 0:
		msg = f'erro {err} from command "{cmd}"'
		if raise_exception:
			raise Exception(msg)
		else:
			log.error(msg)
	return out


def command(cmd, raise_exception=True, cmd_args=None):
	if cmd_args is not None:
		cmd = cmd.format(**cmd_args)

	if args.confirm_cmd:
		sys.stdout.write(f'Execute command?\n\t{cmd}\n')
		while True:
			sys.stdout.write(f'y (yes) / n (no) /a (always): ')
			sys.stdout.flush()
			l = sys.stdin.readline().strip().lower()
			if l in ['a', 'always']:
				args.confirm_cmd = False
				break
			elif l in ['n', 'no']:
				return
			elif l in ['y', 'yes']:
				break
			sys.stdout.write(f'invalid option\n')

	log.debug(f'Executing command: {cmd}')
	with subprocess.Popen(cmd, shell=True) as p:
		exit_code = p.wait()
	log.debug(f'Exit code: {exit_code}')

	if exit_code != 0:
		msg = f'Exit code {exit_code} from command "{cmd}"'
		if raise_exception:
			raise Exception(msg)
		else:
			log.error(msg)
	return exit_code


def test_dir(d):
	if not os.path.isdir(d):
		raise Exception(f'directory "{d}" does not exist')
	return d


def test_path(f):
	if not os.path.exists(f):
		raise Exception(f'path "{f}" does not exist')
	return f


def coalesce(*args):
	for v in args:
		if v is not None:
			return v
	return None


def coalesce_file(*files, access=os.R_OK):
	if not isinstance(access, list): access = [access]
	def check_access(f, access):
		for a in access:
			if not os.access(f, a):
				return False
		return True

	for f in files:
		if f is None: continue
		if check_access(f, access):
			return f
	return None


def args_to_dir(args):
	args_d = collections.OrderedDict()
	for k in dir(args):
		if k[0] != '_' and k not in ['test', 'log_level', 'save_args', 'load_args']:
			args_d[k] = getattr(args, k)
	return args_d


def value_setf(args, arg_name):
	try:
		value = getattr(args, arg_name)
		setf  = lambda val: setattr(args, arg_name, val)
	except:
		try:
			value = args[arg_name]
			setf = lambda val: args.__setitem__(arg_name, val)
		except:
			raise KeyError(f'failed to get the value of attribute {arg_name}')
	return value, setf


def search_file(name: str) -> list:
	ret = []

	base_search = ['.']
	if os.environ.get('APPIMAGE') is not None:
		base_search.append(os.environ.get('APPIMAGE'))
	try:
		base_search.append(os.path.dirname(__file__))
	finally:
		pass

	last_p = None
	for p in base_search:
		while os.path.isdir(p):
			p = os.path.abspath(p)
			if p == last_p:
				break
			# print(f'p = {p}')
			for f in [os.path.join(p, name)] + \
				     [os.path.join(p, i, name) for i in ['build', 'release']]:
				log.debug(f'search_file: {f} ...')
				if f not in ret and os.path.isfile(f):
					log.debug(f'search_file: {f} FOUND')
					ret.append(f)
			last_p = p
			p = os.path.join(p, '..')

	# print(ret)
	return ret


def get_at3_script(script_gen: int = 1, warm: int = 0, w0: int = 10, instances: int = 4, interval: int = 2) -> set:
	wait = warm
	while wait < warm+w0: wait += interval

	jc = wait
	ret = []
	if script_gen == 0:
		for i in range(0, instances):
			ret.append(f"0:wait;0:write_ratio=0")
		jc += interval

	elif script_gen == 1:
		for i in range(0, instances):
			ret.append(f"0:wait;0:write_ratio=0;{jc}m:wait=false")
			jc += interval
		for wr in [0.1, 0.2, 0.3, 0.5, 0.7, 1]:
			for i in range(0, instances):
				ret[i] += f";{jc}m:write_ratio={wr}"
				jc += interval

	elif script_gen == 2:
		for i in range(0, instances):
			ret.append(f"0:wait;0:write_ratio=0;{jc}m:wait=false")
			jc += interval
		for i in range(0, instances):
			ret[i] += f";{jc}m:write_ratio=0.1"
			jc += interval
		for i in range(0, instances):
			ret[i] += f";{jc}m:write_ratio={0.9 if i + 1 == instances else 1}"
		jc += interval
		for i in range(0, instances):
			ret[i] += f";{jc}m:write_ratio=1"
		jc += interval

	elif script_gen == 3:
		for i in range(0, instances):
			ret.append(f"0:wait;0:write_ratio=0;{jc}m:wait=false")
			jc += interval
		for i in range(0, instances):
			ret[i] += f";{jc}m:write_ratio=0.1"
			jc += interval
		for i in range(0, instances):
			ret[i] += f";{jc}m:write_ratio=0.9"
			jc += interval

	elif script_gen == 4:
		for i in range(0, instances):
			ret.append(f"0:wait;0:write_ratio=0;{jc}m:wait=false")
		jc += interval
		for i in range(0, instances):
			ret[i] += f";{jc}m:write_ratio=0.1"
		jc += interval
		for i in range(0, instances):
			ret[i] += f";{jc}m:write_ratio=0.9"
		jc += interval

	else:
		raise Exception(f'Invalid at_script_gen = "{script_gen}". Must be [0-3].')
	return '#'.join(ret), jc


def get_default_rocksdb_options():
	o = os.environ.get('ROCKSDB_OPTIONS_FILE')
	f = coalesce_file(o, *search_file('rocksdb.options'), access=os.R_OK)
	if f is None:
		f = ''
	log.debug(f'get_default_rocksdb_options: {f}')
	return f


def get_rocksdb_bin():
	f = coalesce_file(os.environ.get('ROCKSDB_TEST_PATH'), *search_file('rocksdb_test'), access=os.X_OK)
	if f is None:
		f = 'rocksdb_test'
	log.debug(f'get_rocksdb_bin: {f}')
	return f


# =============================================================================
class Test:
	def __init__(self, name):
		f = getattr(self, name)
		if f is None:
			raise Exception(f'test named "{name}" does not exist')
		f()

	def args(self):
		args_d = args_to_dir(args)
		for k, v in args_d.items():
			v2 = f'"{v}"' if v is not None else ''
			log.info(f'Arg {k:<20} = {v2}')

	def env(self):
		e = os.environ
		for k, v in e.items():
			log.info(f'Env {k:<20} = {v}')


# =============================================================================
def signal_handler(signame, signumber, stack):
	def kill_process(p_):
		try:
			log.warning(f'Child process {p_.pid} is still running. Sending signal {signame}.')
			p_.send_signal(signumber)
		except Exception as e:
			sys.stderr.write(f'signal_handler exception1: {str(e)}\n')

	try:
		log.warning("signal {} received".format(signame))
		for p in psutil.Process().children(recursive=False):
			kill_process(p)
		for i in range(0, 10):
			time.sleep(0.2)
			if len(psutil.Process().children(recursive=True)) == 0:
				break
		for p in psutil.Process().children(recursive=True):
			kill_process(p)

	except Exception as e:
		sys.stderr.write(f'signal_handler exception2: {str(e)}\n')
	exit(1)


# =============================================================================
def main() -> int:
	for i in ('SIGINT', 'SIGTERM'):
		signal.signal(getattr(signal, i),
		              lambda signumber, stack, signame=i: signal_handler(signame,  signumber, stack))

	try:
		if args.test == '':
			exp_class = experiment_list.get(args.experiment)
			if exp_class is not None:
				exp_class().run()
			else:
				raise Exception(f'experiment {args.experiment} not found')
		else:
			Test(args.test)

	except Exception as e:
		if log.level == logging.DEBUG:
			exc_type, exc_value, exc_traceback = sys.exc_info()
			sys.stderr.write('main exception:\n' +
			                 ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)) + '\n')
		else:
			sys.stderr.write(str(e) + '\n')
		return 1
	return 0


# =============================================================================
if __name__ == '__main__':
	exit(main())
