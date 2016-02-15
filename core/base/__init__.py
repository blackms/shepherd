__author__ = 'alessio.rocchi'

from pyVim import connect
from pyVmomi import vim
import pyVmomi


class VcInterface(object):
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.si = None
        self.content = None

    def _get_container_view(self, obj_type, container=None):
        """
        Get a vSphere Container View reference to all objects of type 'obj_type'
        It is up to the caller to take care of destroying the View when no longer
        needed.
        Args:
            obj_type (list): A list of managed object types
        Returns:
            A container view ref to the discovered managed objects
        """
        if self.si:
            if not container:
                container = self.si.content.rootFolder
            view_ref = self.si.content.viewManager.CreateContainerView(
                container=container,
                type=obj_type,
                recursive=True
            )
            return view_ref

    def _get_obj(self, vimtype, name, root_path=None):
        """
        Get the vSphere object associated with a given text name
        """
        obj = None
        root_path = root_path if not None else self.si.content.rootFolder
        container = self.si.content.viewManager.CreateContainerView(root_path, vimtype, True)
        for c in container.view:
            if c.name == name:
                obj = c
                break
        return obj

    def connect(self):
        """Connect to a vCenter server with credential passed in constructor.
        :return: True if connection is successful else False.
        """

        si = connect.SmartConnect(host=self.host,
                                  user=self.username,
                                  pwd=self.password)
        if si:
            self.si = si
            self.content = si.RetrieveContent()
            return True
        return False

    def collect_properties(self, view_ref, obj_type, path_set=None, include_mors=False):
        """
        Collect properties for managed objects from a view ref
        Check the vSphere API documentation for example on retrieving
        object properties:
            - http://goo.gl/erbFDz
        Args:
            si          (ServiceInstance): ServiceInstance connection
            view_ref (pyVmomi.vim.view.*): Starting point of inventory navigation
            obj_type      (pyVmomi.vim.*): Type of managed object
            path_set               (list): List of properties to retrieve
            include_mors           (bool): If True include the managed objects
                                           refs in the result
        Returns:
            A list of properties for the managed objects
        """
        collector = self.si.content.propertyCollector

        # Create object specification to define the starting point of
        # inventory navigation
        obj_spec = pyVmomi.vmodl.query.PropertyCollector.ObjectSpec()
        obj_spec.obj = view_ref
        obj_spec.skip = True

        # Create a traversal specification to identify the path for collection
        traversal_spec = pyVmomi.vmodl.query.PropertyCollector.TraversalSpec()
        traversal_spec.name = 'traverseEntities'
        traversal_spec.path = 'view'
        traversal_spec.skip = False
        traversal_spec.type = view_ref.__class__
        obj_spec.selectSet = [traversal_spec]

        # Identify the properties to the retrieved
        property_spec = pyVmomi.vmodl.query.PropertyCollector.PropertySpec()
        property_spec.type = obj_type

        if not path_set:
            property_spec.all = True

        property_spec.pathSet = path_set

        # Add the object and property specification to the
        # property filter specification
        filter_spec = pyVmomi.vmodl.query.PropertyCollector.FilterSpec()
        filter_spec.objectSet = [obj_spec]
        filter_spec.propSet = [property_spec]

        # Retrieve properties
        props = collector.RetrieveContents([filter_spec])

        data = []
        for obj in props:
            properties = {}
            for prop in obj.propSet:
                properties[prop.name] = prop.val
            if include_mors:
                properties['obj'] = obj.obj
            data.append(properties)
        return data

    def disconnect(self):
        connect.Disconnect(self.si)

    def get_all_vms(self):
        """Return a view of all vm in the vCenter.
        :return: list of vim.VirtualMachine
        """
        # create a view of every VM in the VC
        vm_view = self._get_container_view([vim.VirtualMachine])
        return vm_view

    def get_all_clusters(self):
        """Return a view of all clusters in the vCenter.
        :return: list of vim.ClusterComputeResource
        """
        cluster_view = self._get_container_view([vim.ClusterComputeResource])
        return cluster_view
