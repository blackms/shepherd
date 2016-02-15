__author__ = 'alessio.rocchi'

import Queue
from threading import Thread
import logging
from pyVim import connect
from pyVmomi import vim
import ConfigParser

import requests
requests.packages.urllib3.disable_warnings()

rootLogger = logging.getLogger('shepherd.reactioneer')

Config = ConfigParser.ConfigParser()
Config.read('config.ini')

def config_section_map(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
        except KeyError:
            dict1[option] = None
    return dict1


class VmFinder(object):
    def __init__(self, vc, username, password):
        self.vc = vc
        self.username = username
        self.password = password
        self.si = None
        self.content = None
        self.logger = logging.getLogger('shepherd.reactioneer.VmFinder')
        self.logger.propagate = True

    def connect(self):
        self.si = connect.SmartConnect(host=self.vc,
                                       user=self.username,
                                       pwd=self.password)
        self.content = self.si.RetrieveContent()
        self.logger.debug('Connected to vCenter: {}'.format(self.vc))

    def find_vm_by_moref(self, mo_ref):
        self.logger.debug('Searching for object with mo_ref: {}'.format(mo_ref))
        vm = vim.VirtualMachine(mo_ref)
        vm._stub = self.si._stub
        self.logger.debug('Found Object: {}'.format(vm.name))
        return vm

    def __exit__(self):
        connect.Disconnect(self.si)


class Reactioneer(Thread):
    def __init__(self, reactioneer_queue, executor_queue, dispatch_any=False):
        super(Reactioneer, self).__init__()
        self.reactioneer_queue = reactioneer_queue
        self.executor_queue = executor_queue
        self.stop = False
        self.logger = logging.getLogger('shepherd.reactioneer.Reactioneer')
        self.name = 'Reactioneer'
        self.dispatch_any = dispatch_any
        self.logger.info("Reactioneer Initialized. Waiting for events...")
        if self.dispatch_any == '1':
            self.logger.info('Reactioneer will dispatch any VM. Testing purpose only. Disable in production!')

    def run(self):
        while not self.stop:
            try:
                vm_mo_ref = self.reactioneer_queue.get(timeout=10)
            except Queue.Empty:
                continue
            self.logger.info('Received vm_mo_ref from Resolver: {}'.format(vm_mo_ref))
            vcenter_config = config_section_map("vcenter")
            finder = VmFinder(vc=vcenter_config['vcenter'],
                              username=vcenter_config['username'],
                              password=vcenter_config['password'])
            finder.connect()
            vm = finder.find_vm_by_moref(mo_ref=vm_mo_ref)
            if 'windows' in vm.config.guestFullName.lower():
                self.logger.info('Windows vm found. Dispatching to Executor.')
                self.executor_queue.put(vm)
            elif self.dispatch_any == '1':
                self.logger.info('Testing purpose: vm received. Dispatching to Executor.')
                self.executor_queue.put(vm)
            else:
                self.logger.info('VM not recognized as Windows. Skipping it.')
                self.logger.debug('VM guest: {}'.format(vm.config.guestFullName.lower()))
            self.reactioneer_queue.task_done()
