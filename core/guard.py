__author__ = 'alessio.rocchi'

from threading import Thread, Event
from vspherelib.clustermanager import ClusterManager
from core.base import VcInterface
from pyVmomi import vim
import logging
import time

import requests
requests.packages.urllib3.disable_warnings()


def timeit(f):
    def timed(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        print 'func:%r args:[%r, %r] took: %2.4f sec' % (f.__name__, args, kw, te - ts)
        return result

    return timed

rootLogger = logging.getLogger("shepherd.guard")


# noinspection PyTypeChecker
class Guard(Thread, VcInterface):
    def __init__(self, host, username, password, event, host_group_name='WindowsVM', vm_group_name='Windows',
                 windows_affinity_rule_name="WindowsAffinityRule", pattern='windows', wait_time=3600,
                 post_start=30):
        Thread.__init__(self)
        VcInterface.__init__(self, host=host, username=username, password=password)
        self.stopped = event
        self.host_group_name = host_group_name
        self.vm_group_name = vm_group_name
        self.windows_affinity_rule_name = windows_affinity_rule_name
        self.name = "Guard"
        self.logger = logging.getLogger('shepherd.guard.Guard')
        self.pattern = pattern
        self.wait_time = wait_time
        self.post_start = post_start
        self.logger.info("Guardian Initialized. Cycles will start in {} seconds.".format(post_start))

    # noinspection PyUnresolvedReferences
    def run(self):
        time.sleep(self.post_start)
        vm_properties = ["name", "config.guestFullName"]
        while True:
            try:
                self.logger.debug("Checking Windows VM coherency group.")
                if self.connect():
                    self.logger.debug("Connected to vCenter: {}.".format(self.host))
                    cm = ClusterManager(si=self.si, content=self.content)
                    all_vc_vms = self.get_all_vms()
                    all_clusters = self.get_all_clusters()
                    # now it becomes fun...
                    vm_data = self.collect_properties(view_ref=all_vc_vms,
                                                      obj_type=vim.VirtualMachine,
                                                      path_set=vm_properties,
                                                      include_mors=True)

                    # map cluster data into a dictionary
                    clusters = {}
                    for cluster in all_clusters.view:
                        clusters[cluster.name] = {}
                        clusters[cluster.name]['obj'] = cluster
                        windows_vm_group = filter(
                            lambda x: isinstance(x, vim.cluster.VmGroup) and x.name == self.vm_group_name,
                            cluster.configurationEx.group
                        )
                        vm_list = []
                        if len(windows_vm_group) > 0 and isinstance(windows_vm_group[0], vim.cluster.VmGroup):
                            # check if there are VM inside the vm group
                            if len(windows_vm_group[0].vm) > 0:
                                vm_list = map(lambda x: x.name, windows_vm_group[0].vm)
                        clusters[cluster.name]['windows.vm.group'] = vm_list

                    self.logger.debug("Start iterating VMs...")
                    try:
                        filtered_vm_list = filter(lambda x: self.pattern in x['config.guestFullName'].lower(), vm_data)
                    except KeyError:
                        # probably running during VM creation. continue the cycle with empty vm list.
                        self.logger.error("KeyError: 'config.guestFullName', continue.")
                        filtered_vm_list = []
                    finally:
                        for vm in filtered_vm_list:
                            cluster_name = vm['obj'].resourcePool.owner.name
                            if vm['name'] not in clusters[cluster_name]['windows.vm.group']:
                                self.logger.info("VM: {} in cluster: {} is not in the VmGroup. Adding it.".format(
                                    vm['name'], cluster_name
                                ))
                                # more defensive here... sorry bro.
                                _vmgroup = cm.get_vm_group_by_name(name=self.vm_group_name,
                                                                   cluster=clusters[cluster_name]['obj'])
                                if len(_vmgroup) <= 0:
                                    cm.create_vm_group(clusters[cluster_name]['obj'])
                                _hostgroup = cm.get_host_group_by_name(name=self.host_group_name,
                                                                       cluster=clusters[cluster_name]['obj'])
                                if len(_hostgroup) <= 0:
                                    cm.create_host_group(clusters[cluster_name]['obj'])

                                if not cm.check_avail_res(vm['obj'], clusters[cluster_name]['obj']):
                                    if not cm.add_host_to_host_group(cluster=clusters[cluster_name]['obj']):
                                        self.logger.critical("Failed to expand HostGroup. Skipping VM add.")
                                        continue
                                cm.add_vm_to_vm_group(vm['obj'], clusters[cluster_name]['obj'])
                else:
                    self.logger.critical("Cannot connect to vcenter: {}.".format(self.host))
                self.logger.debug("Windows VM coherency check complete.")
                self.disconnect()
            except Exception as exc:
                self.logger.error(exc, exc_info=True)
            if self.stopped.wait(self.wait_time):
                self.logger.debug("Break reached.")
                break
