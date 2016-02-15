__author__ = 'alessio.rocchi'


class Operation(object):
    add = "add"
    remove = "remove"
    edit = "edit"

    def __init__(self):
        self._operation = None

    @property
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, value):
        self._operation = value


class HostGroupNotExists(Exception):
    name = ''

    def __init__(self, *args, **kwargs):
        try:
            self.name = kwargs.pop('name')
        except KeyError:
            # No name specified. Ignoring it
            pass
        super(HostGroupNotExists, self).__init__()


class HostListEmpty(Exception):
    def __init__(self, *args, **kwargs):
        super(HostListEmpty, self).__init__(*args, **kwargs)


class VmGroupNotExists(Exception):
    name = ''

    def __init__(self, *args, **kwargs):
        try:
            self.name = kwargs.pop('name')
        except KeyError:
            # No name specified. Ignoring it
            pass
        super(VmGroupNotExists, self).__init__(*args, **kwargs)
