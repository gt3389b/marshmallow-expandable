# -*- coding: utf-8 -*-
from marshmallow_expandable.exceptions import RestServerException, RestValidationException
from marshmallow_expandable.patterns import ResourcePattern
from marshmallow_expandable.query import RestQuerySet

class ResourceManagerDescriptor(object):
    """
    This class ensures managers aren't accessible via model instances. For
    example, Book.objects works, but book_obj.objects raises AttributeError.
    """
    def __init__(self, manager):
        self.manager = manager

    def __get__(self, instance, type=None):
        if instance is not None:
            raise AttributeError(
                'Manager is not accessible via %s instances' % type.__name__)
        return self.manager


class ResourceManager(object):

    def __init__(self, queryset_class=None):
        if queryset_class is None:
            queryset_class = RestQuerySet
        self.queryset_class = queryset_class
        self.object_class = None

    @property
    def options(self):
        try:
            return getattr(self, '_options')
        except AttributeError:
            self._options = self.object_class._meta
            return self._options

    def get_queryset(self):
        queryset = self.queryset_class(
            model=self.object_class, 
            client=self.options.client)
        return queryset

    def filter(self, **kwargs):
        queryset = self.get_queryset().filter(**kwargs)
        return queryset

    def all(self):
        queryset = self.get_queryset()
        return queryset

    def get(self, **kwargs):
        obj = self.get_queryset().get(**kwargs)
        return obj

    def using(self, client):
        return self.get_queryset().using(client)

    def order_by(self, *args):
        return self.get_queryset().order_by(*args)

    def create(self, **kwargs):
        """Send POST request to resource and return Resource instance."""
        instance = self.object_class(kwargs)
        instance.save()
        return instance
