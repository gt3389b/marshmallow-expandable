import warnings
from collections import OrderedDict, defaultdict


class Registry(object):
    """
    Registry class for Restorm Resources.
    Adapted from django.apps.registry.Apps.
    """
    def __init__(self):
        # Mapping of app labels => resource names => model classes. Every time
        # a resource is imported, ResourceBase.__new__ calls register() which
        # creates an entry in all_resources. All imported resources are
        # registered, regardless of whether they're defined in an installed
        # application and whether the apps registry has been populated.
        # Since it isn't possible to reimport a module safely (it could
        # reexecute initialization code) all_resources is never overridden
        # or reset.
        self.all_resources = defaultdict(OrderedDict)

    def register(self, app_label, resource):
        """
        Registers a resource for a given app.
        Taken from django.apps.registry.Apps#register_model
        """
        resource_name = getattr(resource._meta, 'resource_name', None)

        if not resource_name:
            raise RuntimeError(
                "Resource '%s.%s' does not define a resource_name and is not "
                "marked abstract." % (app_label, resource.__name__))

        app_resources = self.all_resources[app_label]

        if resource_name in app_resources:
            existing = app_resources[resource_name]
            if (resource.__name__ == existing.__name__ and
                    resource.__module__ == existing.__module__):
                info = (app_label, resource_name)
                warnings.warn(
                    "Resource '%s.%s' was already registered." % info,
                    RuntimeWarning, stacklevel=2)
            else:
                info = (resource_name, app_label, existing, resource)
                raise RuntimeError(
                    "Conflicting '%s' resources in application '%s':"
                    "%s and %s." % info)

        app_resources[resource_name] = resource


registry = Registry()
