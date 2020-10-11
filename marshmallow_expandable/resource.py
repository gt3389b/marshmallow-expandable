from collections import OrderedDict
import sys, json

from .exceptions import RestServerException, RestValidationException
#from .fields import Field, ToOneField, ToManyField
from .managers import ResourceManager, ResourceManagerDescriptor
from .patterns import ResourcePattern
from .registry import registry

from pprint import pprint

"""
class ResourceList(list):
    A list of ``Resource`` instances which are most likely incomplete compared
    to when they are retrieved as an individual.
    def __init__(self, data, **kwargs):
        self.client = kwargs.pop('client', None)
        self.absolute_url = kwargs.pop('absolute_url', None)

        super(ResourceList, self).__init__([Resource(item, self.client) for item in data])
"""


class Resource(object):
    """
    Class that holds information about a resource.

    It has a manager to retrieve and/or manipulate the state of a resource.
    """
    objects = None

    def __init__(self, data={}, client=None, absolute_url=None, delete_url=None):
        if '_meta' in data:
            self._meta = data['_meta']
        
        if '_fields' in data:
            self._fields = data['_fields']

        self.client = client or self._meta.client
        self.absolute_url = absolute_url
        self.delete_url = delete_url
        assert type(data) == dict, (type(data), data)
        if 'data' in data:
            self.data = data['data'].copy()
        else:
            self.data = {}
        #for key, value in self._meta.get_fields().items():
        for key, value in self._fields.items():
            #if key in self.data and not value.is_relation:
            if key in self.data:
                setattr(self, key, self.data.get(key))

        if not hasattr(self, '_state'):
            State = type("State", (object,), dict())
            self._state = State()

        #if self.absolute_url is None and self._meta.pk.attname not in self.data:
        if self.absolute_url is None and self._meta.primary_key not in self.data:
            self._state.adding = True
        
        self._item_pattern = ResourcePattern.parse(self._meta.item)
        self._list_pattern = ResourcePattern.parse(self._meta.list)

        # default 
        self._create_pattern = ResourcePattern.parse(self._meta.list) if self._meta.create == '' else ResourcePattern.parse(self._meta.create)
        self._delete_pattern = ResourcePattern.parse(self._meta.item) if self._meta.delete == '' else ResourcePattern.parse(self._meta.delete)

    def __unicode__(self):
        if self.absolute_url:
            if not isinstance(self.absolute_url, str):
                return self.absolute_url.decode()
        return self.absolute_url

    # need to fix code when it assumes repr for str
    def __str__(self):
        #if self.absolute_url:
        #    return self.__unicode__()
        #else:
        #    return '' 
        #return self.__unicode__()
        #pprint(self.data)
        return json.dumps(self.data)

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.__unicode__())

    def _get_pk_val(self):
        try:
            return self.data[self._meta.pk.attname]
        except:
            return getattr(self.data, self._meta.pk.attname)

    def _get_unique_checks(self, exclude=None):
        return [], []

    @property
    def pk(self):
        if getattr(self._meta, '_pk_attr', 'id'):
            return getattr(self, self._meta._pk_attr, None)

    def serializable_value(self, name):
        return getattr(self, name, None)

    def full_clean(self, *args, **kwargs):
        pass

    def validate_unique(self, *args, **kwargs):
        pass

    def _clean_request_data(self):
        obj_data = self.data.copy()
        for key, value in self.data.items():
            try:
                field = self._meta.get_field(key)
            except FieldDoesNotExist:
                del obj_data[key]
                continue
            if value and isinstance(field, ToOneField):
                value = value.pk
            if value and isinstance(field, ToManyField):
                if field.rel.through is None:
                    value = [o.pk for o in value]
                else:
                    del obj_data[key]
                    continue
            obj_data[key] = value
        return obj_data

    def save(self, commit=True):
        """
        Performs a PUT request to update the object.

        No guarantees are given to what this method actually returns due to the
        freedom of API implementations. If there is a body in the response, the
        contents of this body is returned, otherwise ``None``.
        """
        obj_data = self._clean_request_data()
        if not commit:
            return
        if not self.absolute_url:
            created = True
            absolute_url = self._create_pattern.get_absolute_url(root=self._meta.root)
            response = self.client.post(absolute_url, obj_data)
        else:
            created = False
            #absolute_url = self.absolute_url
            #absolute_url = self._create_pattern.get_absolute_url(root=self._meta.root)
            response = self.client.put(self.absolute_url, obj_data)

        # Although 204 is the best HTTP status code for a valid PUT response.
        if response.status_code in [200, 201, 204]:
            if response.content and isinstance(response.content, dict):
                self.data = response.content
                pk_attr = self._meta.pk.attname
                if not self.absolute_url:
                    self.absolute_url = self._item_pattern.get_absolute_url(
                        root=self._meta.root, **{pk_attr: self.data[pk_attr]})

                # create a delete url for this resource
                if not self.delete_url:
                    self.delete_url = self._delete_pattern.get_absolute_url(
                        root=self._meta.root, **{pk_attr: self.data[pk_attr]})
            return created
        elif response.status_code in [400]:
            raise RestValidationException('Cannot save "%s" (%d): %s' % (
                response.request.uri, response.status_code, response.content),
                response)
        else:
            raise RestServerException('Cannot save "%s" (%d): %s' % (
                response.request.uri, response.status_code, response.content))

    def delete(self):
        """
        Performs a DELETE request to delete the object.

        No guarantees are given to what this method actually returns due to the
        freedom of API implementations. If there is a body in the response, the
        contents of this body is returned, otherwise ``None``.
        """
        response = self.client.delete(self.delete_url)

        # Although 204 is the best HTTP status code for a valid PUT response.
        if response.status_code in [200, 201, 204]:
            self.absolute_url = None
            self.delete_url = None
            if response.content:
                return response.content
            else:
                return None
        elif response.status_code in [400]:
            raise RestValidationException('Cannot delete "%s" (%d): %s' % (
                response.request.uri, response.status_code, response.content),
                response)
        else:
            raise RestServerException('Cannot delete "%s" (%d): %s' % (
                response.request.uri, response.status_code, response.content))


class SimpleResource(object):
    """
    Class that holds information about a resource.

    It has a manager to retrieve and/or manipulate the state of a resource.
    """
    objects = None

    def __init__(self, data=None, client=None, absolute_url=None, delete_url=None):
        if '_meta' in data:
            self._meta = data['_meta']

        self.client = client or self._meta.client
        self.absolute_url = absolute_url
        self.delete_url = delete_url

        self.data = data

    def __unicode__(self):
        return self.absolute_url

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.__unicode__())

    def save(self):
        self.client.put(self.absolute_url, self.data)
