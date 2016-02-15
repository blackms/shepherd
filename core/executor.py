__author__ = 'alessio.rocchi'

import Queue
from threading import Thread
import logging
from pyVim import connect
from pyVmomi import vim
from vspherelib.clustermanager import ClusterManager
from vspherelib.helper.Types import HostGroupNotExists, VmGroupNotExists

import requests

requests.packages.urllib3.disable_warnings()

rootLogger = logging.getLogger('shepherd.executor')


class Executor(Thread):
    def __init__(self, host, username, password, executor_queue, host_group_name='WindowsVM', vm_group_name='Windows',
                 windows_affinity_rule_name="WindowsAffinityRule", create_affinity_rule=True):
        super(Executor, self).__init__()
        self.host = host
        self.username = username
        self.password = password
        self.si = None
        self.content = None
        self.logger = logging.getLogger('shepherd.executor.Executor')
        self.executor_queue = executor_queue
        self.stop = False
        self.name = "Executor"
        self.host_group_name = host_group_name
        self.vm_group_name = vm_group_name
        self.windows_affinity_rule_name = windows_affinity_rule_name
        self.create_affinity_rule = create_affinity_rule
        self.logger.info("Executor Initialized. Waiting for events...")
        if create_affinity_rule == '0':
            self.logger.info('Executor has disabled VM Affinity Rule Check!')

    def connect(self):
        self.si = connect.SmartConnect(host=self.host,
                                       user=self.username,
                                       pwd=self.password)
        self.content = self.si.RetrieveContent()
        self.logger.debug('Connected to vCenter: {}'.format(self.host))

    def run(self):
        while not self.stop:
            try:
                vm = self.executor_queue.get(timeout=10)
            except Queue.Empty:
                continue
            self.logger.info("Received VM from Reactioneer: {}".format(vm.name))
            self.logger.debug('Instancing Cluster Manager.')
            self.connect()
            cm = ClusterManager(self.si, self.content)
            cluster = vm.resourcePool.owner

            self.logger.debug('Identified cluster: {}'.format(cluster.name))

            try:
                # check if the HostGroup exists.
                self.logger.debug("Checking if HostGroup: {} exists.".format(self.host_group_name))
                host_group = cm.get_host_group_by_name(name=self.host_group_name, cluster=cluster)
                if len(host_group) <= 0:
                    # the HostGroup doesnt exists, create it.
                    self.logger.debug("Creating HostGroup: {}.".format(self.host_group_name))
                    cm.create_host_group(cluster)
                else:
                    if not isinstance(host_group[0], vim.cluster.HostGroup):
                        cm.create_host_group(cluster)

                # check if the HostGroup has not enough resources to contain the new VM, in that case add another
                # host to the group.
                try:
                    self.logger.debug('Checking if there is enough power with the current number of hosts...')
                    if not cm.check_avail_res(vm, cluster):
                        # the HostGroup doesn't have enough resources. Add another host to it.
                        self.logger.debug("Adding a new Host to the HostGroup: {}.".format(self.host_group_name))
                        cm.add_host_to_host_group(cluster)
                except HostGroupNotExists:
                    raise

                # check if the VmGroup exists.
                self.logger.debug("Checking if VmGroup: {} exists".format(self.vm_group_name))
                vm_group = cm.get_vm_group_by_name(self.vm_group_name, cluster)
                if len(vm_group) <= 0:
                    # the VmGroup doesn't exists, create it.
                    self.logger.debug("VmGroup: {} doesn't exists. Creating it.".format(self.vm_group_name))
                    cm.create_vm_group(cluster)
                else:
                    if not isinstance(vm_group[0], vim.cluster.VmGroup):
                        self.logger.debug("VmGroup: {} doesn't exists. Creating it.".format(self.vm_group_name))
                        cm.create_vm_group(cluster)

                # finally add the VM to the VmGroup
                self.logger.debug("Adding VM: {} to VmGroup: {}.".format(vm.name, self.vm_group_name))
                if not cm.add_vm_to_vm_group(vm, cluster):
                    self.logger.critical("Failed to add VM: {} to VmGroup: {}".format(vm.name, self.vm_group_name))
                    # TODO: raise a Nagios alarm.

                if self.create_affinity_rule == '1':
                    # check if the affinity rule exists or not
                    if not cm.get_affinity_rule_by_name(self.windows_affinity_rule_name, cluster):
                        self.logger.info("The affinity rule: {} doesn't exists.".format(
                            self.windows_affinity_rule_name
                        ))
                        self.logger.info("Creating affinity rule: {}.".format(self.windows_affinity_rule_name))
                        cm.add_affinity_rule(cluster)
                else:
                    self.logger.info("Affinity rule not checked according to config.")

            except HostGroupNotExists:
                # TODO: raise a Nagios alarm
                self.logger.critical("Failed to handle HostGroup: {}. Aborting.".format(self.host_group_name))
                self.executor_queue.task_done()
            except VmGroupNotExists:
                # TODO: raise a Nagios alarm
                self.logger.critical("Failed to handle VmGroup: {}. Aborting".format(self.vm_group_name))
                self.executor_queue.task_done()

            self.executor_queue.task_done()
            self.logger.info('VM: {} finally processed correctly.\n\n'.format(vm.name))


def __exit__(self):
    connect.Disconnect(self.si)
