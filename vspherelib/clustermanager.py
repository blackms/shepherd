__author__ = 'alessio.rocchi'

from pyVmomi import vim
from helper.tasks import wait_for_task
from helper.Types import Operation
from helper.Types import HostGroupNotExists
from helper.Types import VmGroupNotExists
import logging

rootLogger = logging.getLogger('shepherd.clustermanager')


class ClusterManager(object):
    def __init__(self,
                 si,
                 content,
                 host_group_name='WindowsVM',
                 vm_group_name='Windows',
                 windows_affinity_rule_name="WindowsAffinityRule",
                 test_mode=False):
        """
        :param si: ServiceInstance Managed object referred to a vCenter connection
        :param content: vim.ServiceInstanceContent data object define properties for ServiceInstance MO
        :param host_group_name: string representing the name of the vim.cluster.HostGroup to manage.
        Default: "WindowsVM"
        :param vm_group_name: string representing the name of the vim.cluster.VmGroup to manage.
        Default: "Windows"
        """
        self.logger = logging.getLogger('shepherd.clustermanager.ClusterManager')
        self.si = si
        self.content = content
        self.host_group_name = host_group_name
        self.vm_group_name = vm_group_name
        self.windows_affinity_rule_name = windows_affinity_rule_name
        self.test_mode = test_mode

    @staticmethod
    def get_host_group_by_name(name, cluster):
        """Return a list of objects containing a single object representing the HostGroup to find.
        :param name: string representing the HostGroup name to find.
        :param cluster: vim.ClusterComputeResource data object representing the cluster containing the HostGroup.
        :return: [vim.cluster.HostGroup]
        """
        assert name is not None, Exception('Bad arguments.')
        assert cluster is not None, Exception('Bad arguments.')
        result = filter(lambda x: isinstance(x, vim.cluster.HostGroup) and x.name == name,
                        cluster.configurationEx.group)
        return result

    @staticmethod
    def get_vm_group_by_name(name, cluster):
        """Return a list containing a single object representing the VmGroup to find,
        :param name: string representing the VmGroup name to find.
        :param cluster: vim.ClusterComputeResource data object representing the cluster containing the VmGroup.
        :return: [vim.cluster.VmGroup]
        """
        result = filter(lambda x: isinstance(x, vim.cluster.VmGroup) and x.name == name,
                        cluster.configurationEx.group)
        return result

    @staticmethod
    def get_affinity_rule_by_name(name, cluster):
        """"Return a single object representing the Affinity Rule to find.
        :param name: string representing the name of the Affinity Rule
        :param cluster: vim.clusterComputeResource data object representing the cluster containing the Rule.
        return: vim.cluster.VmHostRuleInfo if the rule is found else None
        """
        result = filter(lambda x: x.name == name, cluster.configurationEx.rule)
        if len(result) <= 0:
            return None
        else:
            if isinstance(result[0], vim.cluster.VmHostRuleInfo):
                return result[0]
        return None

    def get_cluster(self, name=None):
        """Return all clusters in the ServiceInstanceContent data object if a name is not specified, otherwise
        it return a list of cluster having the name to find.
        :param name: string representing the cluster to find. Default is None.
        :return: [vim.ClusterComputeResource]
        """
        clusters = self.content.rootFolder.childEntity[0].hostFolder.childEntity
        return clusters if name is None else filter(lambda x: x.name == name, clusters)[0]

    def add_affinity_rule(self, cluster):
        """Add the new Affinity Rule to associate VmGroup with the HostGroup.
        :param cluster: vim.clusterComputeResource representing the cluster to reconfigure.
        :return True if success, else False.
        """
        operation = Operation()
        spec = vim.cluster.ConfigSpecEx()
        rule = vim.cluster.RuleSpec()
        rule.operation = operation.add
        rule.info = vim.cluster.VmHostRuleInfo()
        rule.info.enabled = True
        rule.info.name = self.windows_affinity_rule_name
        rule.info.vmGroupName = self.vm_group_name
        rule.info.affineHostGroupName = self.host_group_name
        spec.rulesSpec.append(rule)
        if self.test_mode:
            from pprint import pprint
            pprint(spec)
        else:
            task = cluster.ReconfigureComputeResource_Task(spec, True)
            # wait_for_task(task)
        return True

    def add_vm_to_vm_group(self, vm, cluster):
        """Add a VM entity vo a VmGroup spec.
        :param vm: vim.VirtualMachine representing the VM to add.
        :param cluster: vim.ClusterComputeResource representing the cluster where the VM is contained.
        :return: True if the operation is successful else False.
        """
        # check if VmGroup Exists
        vm_name = vm.name
        windows_group = self.get_vm_group_by_name(self.vm_group_name, cluster)
        spec = vim.cluster.ConfigSpecEx()
        operation = Operation()
        if windows_group and len(windows_group) > 0:
            self.logger.info('VmGroup: {} exists. Adding VM {} to it.'.format(self.vm_group_name, vm_name))
            operation.operation = operation.edit
            group_vm = vim.cluster.GroupSpec()
            group_vm.info = vim.cluster.VmGroup()
            # if there are some vm already configured I add it now to the spec.
            if len(windows_group[0].vm) > 0:
                for _vm in windows_group[0].vm:
                    # going defensive here to avoid bad spec.
                    if isinstance(_vm, vim.VirtualMachine):
                        group_vm.info.vm.append(_vm)
        else:
            raise VmGroupNotExists(name=self.vm_group_name)
        group_vm.operation = operation.operation
        group_vm.info.name = self.vm_group_name
        group_vm.info.vm.append(vm)
        spec.groupSpec.append(group_vm)
        if self.test_mode:
            from pprint import pprint

            pprint(spec)
        else:
            task = cluster.ReconfigureComputeResource_Task(spec, True)
            # wait_for_task(task)
        self.logger.info('VM: {} finally plugged into VmGroup: {}'.format(vm_name, self.vm_group_name))
        return True

    def add_host_to_host_group(self, cluster):
        """Add a new host entity to an HostGroup spec.
        :param cluster: vim.ClusterComputeResource representing the cluster containing the HostGroup.
        :return True in case of success, else False
        """
        active_hosts = []
        # I will take the first result returned by the function.
        windows_host_group = self.get_host_group_by_name(self.host_group_name, cluster)
        # Assuring that the group exists.
        operation = Operation()
        if windows_host_group and len(windows_host_group) > 0:
            # Extrapolate the first element of the list (we should have only one record here if everything is fine.
            try:
                windows_host_group = windows_host_group[0]
            except IndexError:
                # Something really strange is happening... Let's raise an error.
                raise HostGroupNotExists(name=self.host_group_name)
            # I take the first host available in the cluster
            operation.operation = operation.edit
            # If the HostGroup is empty, I will add the first host of the cluster (sorted by name)
            if len(windows_host_group.host) == 0:
                sorted_list = sorted(
                    map(lambda x: x, cluster.host),
                    key=lambda x: x.name, reverse=False
                )
                first_usable_host = sorted_list[0]
            else:
                sorted_list = sorted(
                    filter(lambda x: x not in windows_host_group.host, cluster.host),
                    key=lambda x: x.name, reverse=False
                )
                first_usable_host = sorted_list[0]
            active_hosts = windows_host_group.host
            active_hosts.append(first_usable_host)
            self.logger.debug('Picked host: {}'.format(first_usable_host.name))
        else:
            self.logger.critical("HostGroup doesn't exists. We shouldn't be here...")
            raise HostGroupNotExists(name=self.host_group_name)
        spec = vim.cluster.ConfigSpecEx()
        group = vim.cluster.GroupSpec()
        group.operation = operation.operation
        group.info = vim.cluster.HostGroup()
        group.info.name = self.host_group_name
        # if the group exists and the object is really an HostGroup I will add the current list of available
        # hosts to it.
        if windows_host_group and isinstance(windows_host_group, vim.cluster.HostGroup):
            group.info.host = active_hosts
        spec.groupSpec.append(group)
        if self.test_mode:
            from pprint import pprint

            pprint(spec)
        else:
            task = cluster.ReconfigureComputeResource_Task(spec, True)
            # wait_for_task(task)
        return True

    def create_vm_group(self, cluster):
        """Create the VmGroup in the DRS.
        :param cluster: vim.ClusterComputeResource representing the cluster where the VM is contained.
        :return: True in case of success, else False.
        """
        # check if VmGroup Exists
        windows_group = self.get_vm_group_by_name(self.vm_group_name, cluster)
        if len(windows_group) > 0:
            if isinstance(windows_group[0], vim.cluster.VmGroup):
                self.logger.warning("VmGroup: {} exists. Skipping creation.".format(self.vm_group_name))
                return True
        spec = vim.cluster.ConfigSpecEx()
        operation = Operation()
        self.logger.info("Creating VmGroup: {}.".format(self.vm_group_name))
        operation.operation = operation.add
        group_vm = vim.cluster.GroupSpec()
        group_vm.info = vim.cluster.VmGroup()
        group_vm.operation = operation.operation
        group_vm.info.name = self.vm_group_name
        spec.groupSpec.append(group_vm)
        if self.test_mode:
            from pprint import pprint

            pprint(spec)
        else:
            task = cluster.ReconfigureComputeResource_Task(spec, True)
            # wait_for_task(task)
        self.logger.info("VmGroup: {} Created.".format(self.vm_group_name))
        return True

    def create_host_group(self, cluster):
        """Create the HostGroup in the DRS Configuration.
        :param cluster: vim.ClusterComputeResource representing the cluster containing the HostGroup.
        :return True in case of success, else False
        """
        # I will take the first result returned by the function.
        windows_host_group = self.get_host_group_by_name(self.host_group_name, cluster)
        operation = Operation()
        # Assuring that the group doesn't exists
        if len(windows_host_group) > 0:
            if isinstance(windows_host_group[0], vim.cluster.HostGroup):
                self.logger.warning("HostGroup: {} exists. Skipping creation.".format(self.host_group_name))
                return True
        self.logger.warning('Creating HostGroup: {}.'.format(self.host_group_name))
        operation.operation = operation.add
        spec = vim.cluster.ConfigSpecEx()
        group = vim.cluster.GroupSpec()
        group.operation = operation.operation
        group.info = vim.cluster.HostGroup()
        group.info.name = self.host_group_name
        spec.groupSpec.append(group)
        if self.test_mode:
            from pprint import pprint

            pprint(spec)
        else:
            task = cluster.ReconfigureComputeResource_Task(spec, True)
            wait_for_task(task)
        return True

    def get_rp_view_by_vm(self, vm):
        view_container = self.si.content.viewManager.CreateContainerView(
            self.si.content.rootFolder, [vim.ResourcePool], True
        )
        if view_container:
            rp_name = vm.parent.parent
            for view in view_container.view:
                if view.name == rp_name:
                    return view
            return None
        else:
            return None

    def check_avail_res(self, vm, cluster, pattern="windows", only_powered_on=False):
        """Calculate if there is enough resources available on the Hosts members of the Windows HostGroup to contain
        also the new Virtual Machine.
        :param vm: vim.VirtualMachine managed object representing the VM to add the DRS Group.
        :param cluster: vim.ClusterComputeResource representing the cluster containing the HostGroup.
        :return True if there are enough resource else False.
        """
        # check hosts in cluster host group
        try:
            windows_host_group = self.get_host_group_by_name(self.host_group_name, cluster)[0]
        except IndexError:
            raise HostGroupNotExists(host_group_name=self.host_group_name)

        hosts = windows_host_group.host
        if len(hosts) <= 0:
            return False

        worst_case_mhz_allocation = 0

        # get view of resourcePools inside the cluster
        rp_view = self.si.content.viewManager.CreateContainerView(
            cluster, [vim.ResourcePool], True
        )

        _host = cluster.host[0]  # let's take the first host in cluster to get information
        single_host_max_mhz_capacity = _host.summary.hardware.cpuMhz * int(_host.summary.hardware.numCpuCores)
        single_core_max_mhz_capacity = int(_host.hardware.cpuInfo.hz) / 1000000
        available_mhz_on_windows_host_group = len(windows_host_group.host) * single_host_max_mhz_capacity

        # defensive here
        assert rp_view and len(rp_view.view) > 0, Exception()

        for rp in rp_view.view:
            max_rp_usage = rp.config.cpuAllocation.limit
            if rp.name == 'Resources' or 'System vDC' in rp.name:
                continue
            _windows_vm = []
            for vm in rp.vm:
                vm_config = vm.config
                if hasattr(vm_config, 'guestFullName'):
                    if pattern in vm.config.guestFullName.lower():
                        _windows_vm.append(vm)
            rp_total_cpu = sum(
                map(lambda x: x.config.hardware.numCPU, _windows_vm)
            )
            rp_total_mhz = rp_total_cpu * single_core_max_mhz_capacity
            if rp_total_mhz > max_rp_usage:
                rp_total_mhz = max_rp_usage
            self.logger.debug('RP: {}, VM Count: {}, Windows cpuCount: {}, RP totalMhz: {}'.format(rp.name,
                                                                                                   len(_windows_vm),
                                                                                                   rp_total_cpu,
                                                                                                   rp_total_mhz))
            worst_case_mhz_allocation = worst_case_mhz_allocation + rp_total_mhz
            # self.logger.debug('-> Current Worst Case Mhz Allocation: {}'.format(worst_case_mhz_allocation))
            # self.logger.debug('-> Total Mhz Available on HostGroup: {}'.format(available_mhz_on_windows_host_group))
        self.logger.info('Processed: {} Resource Pools'.format(len(rp_view.view)))

        worst_case_mhz_allocation += int(vm.config.hardware.numCPU * single_core_max_mhz_capacity)

        total_mhz = single_host_max_mhz_capacity * len(cluster.host)
        if worst_case_mhz_allocation > available_mhz_on_windows_host_group:
            self.logger.debug('Not enough resources available with the current number of hosts. Returning False')
            return False
        else:
            # HostGroup must have the equal amount of a single host in terms of mhz available in order to have
            # a spare host in the group.
            spare_capacity = available_mhz_on_windows_host_group - worst_case_mhz_allocation
            # self.logger.debug('Total Mhz Available on Windows HostGroup: {}'.format(
            #    available_mhz_on_windows_host_group
            # ))
            self.logger.debug("Total Mhz: {} - Worst Case: {} - Spare Capacity: {} - Single Host Capacity: {}".format(
                available_mhz_on_windows_host_group,
                worst_case_mhz_allocation,
                spare_capacity,
                single_host_max_mhz_capacity
            ))
            if spare_capacity >= single_host_max_mhz_capacity:
                self.logger.debug("Spare host available. Returning True")
                return True
            else:
                self.logger.debug("No spare host resource available. Returning False")
                return False
