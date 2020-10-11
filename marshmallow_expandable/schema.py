import logging

import marshmallow
from marshmallow import fields

from .argument_builder import ArgumentBuilder
from .patterns import ResourcePattern
from .query import RestQuerySet
from .managers import ResourceManagerDescriptor, ResourceManager
from .resource import Resource

from pprint import pprint

logger = logging.getLogger(__name__)

class ExpandableSchemaMixin(object):
    def __init__(self, extra=None, only=None, exclude=(), prefix='', strict=None,
                 many=False, context=None, load_only=(), dump_only=(),
                 partial=False, expand=()):
        super().__init__()

        #print(self, pprint(vars(self)), pprint(str(self)))
        #print(self.__class__, self.fields.keys())
        #print(self.fields.values())
        #print("****")
        #pprint(self.fields)
        #print("****")
        meta = vars(self.Meta)

        # set up fields
        #self._fields = self.fields.keys()

        _meta = self._init_metadata(meta.items())
        #self._init_resource()
        self.model = Resource(data={'_meta':_meta, '_fields':self.fields})

        manager = ResourceManager()
        manager.object_class = self.model
        #self._objects = ResourceManagerDescriptor(manager)
        self._objects = manager

        logger.info("****************test****************")
        self._expand = self._normalize_expand(expand)

    def _init_resource(self):
        Patterns = type("Patterns", (object,), dict())
        self._patterns = Patterns()

        setattr(self._patterns, "item", ResourcePattern.parse(self._meta.item))
        setattr(self._patterns, "list", ResourcePattern.parse(self._meta.list))

        # default 
        setattr(self._patterns, "create", ResourcePattern.parse(self._meta.list) if self._meta.create == '' else ResourcePattern.parse(self._meta.create))
        setattr(self._patterns, "delete", ResourcePattern.parse(self._meta.item) if self._meta.delete == '' else ResourcePattern.parse(self._meta.delete))

        #print(vars(self._patterns))
        #print(self._patterns.create.get_absolute_url('test/'))


    def _init_metadata(self, meta):
        #Options = type("Options", (object,), dict())
        class Options(dict):
            def __repr__(self):
                #return json.dumps(self.__dict__)
                return ""
        _meta = Options()

        # init to nothing
        for key in ['item', 'list', 'create', 'delete', 'root', 'primary_key']:
            setattr(_meta, key, '')

        # init to nothing
        for key in ['page_size', 'page_size_params']:
        #for key in ['page_size', 'page_size_params', 'pk_name', 'pk']:
            setattr(_meta, key, None)

        # init metas
        for key, value in meta:
            if key[0] != '_':
                setattr(_meta, key, value)

        #print(vars(self._meta))
        return _meta
        
    def _normalize_expand(self, expand):
        """
        This function takes the list of fields to expand and assigns this attribute
        recursively, while assigning to self.expand the fields he is immediately interested
        """
        if expand is not None:
            self._Schema__apply_nested_option('expand', expand, 'intersection')

            expand = self.set_class([field.split('.', 1)[0] for field in expand])

        return expand

    @property
    def expand(self):
        return self._expand

    @expand.setter
    def expand(self, value):
        """Every time we assign a new expand terms we need to normalize them"""
        self._expand = self._normalize_expand(value)


class ExpandableNested(fields.Nested):
    def __init__(self, nested, **kwargs):
        super().__init__(nested, **kwargs)
        #super().__init__(lambda: test,**kwargs)
        self.expand = kwargs.get('expand', ())

    @property
    def schema(self):
        if self._schema:
            return self._schema

        schema = super().schema
        if isinstance(schema, ExpandableSchemaMixin):
            setattr(schema, 'expand', self.expand)

        return schema

    def _serialize(self, nested_obj, attr, obj):
        should_expand = hasattr(self.root, 'expand') and attr in self.root.expand
        resource = self._expand_resource(nested_obj) if should_expand else nested_obj
        return super()._serialize(resource, attr, obj)

    def _expand_resource(self, resource):
        return ResourceExpander().expand_resource(self.schema, self.many, resource)


class ResourceExpander:
    """
    The resource is only used for generating the arguments that will serve as input for the
    function we are going to call
    """
    def expand_resource(self, schema, many, resource):
        arg_builder = ArgumentBuilder()

        #print(schema, resource)
        if many:
            batch_func, batch_arguments_map = self._get_query_function_and_arguments(schema, 'batch')
            if batch_func:
                argument_set = arg_builder.build_arguments(resource, batch_arguments_map, aggregate=True, many=True)
                result = self._execute_query(batch_func, argument_set)
            else:
                retrieve_func, retrieve_argument_map = self._get_query_function_and_arguments(schema, 'retrieve')
                argument_set = arg_builder.build_arguments(resource, retrieve_argument_map, many=True)
                result = [self._execute_query(retrieve_func, arguments) for arguments in argument_set]
        else:
            retrieve_func, retrieve_argument_map = self._get_query_function_and_arguments(schema, 'retrieve')
            arguments = arg_builder.build_arguments(resource, retrieve_argument_map)
            result = self._execute_query(retrieve_func, arguments)

        return result

    def _get_query_function_and_arguments(self, schema, function_name):
        if not hasattr(schema.Meta, function_name):
            return None, None

        try:
            function, arguments = getattr(schema.Meta, function_name)
        except Exception as e:
            raise Exception('The interactor Meta attribute should be a tuple composed '
                            'by the function to build the interactor and the list of parameters') from e

        argument_map = dict(self._split_argument_map(arg) for arg in arguments)
        """Maps a queryparam from the schema to the interactor"""

        return function, argument_map

    def _split_argument_map(self, argument):
        return argument if isinstance(argument, tuple) else (argument, argument)

    def _execute_query(self, retrieve_func, arguments):
        resource_or_interactor = retrieve_func(**arguments)
        is_interactor = hasattr(resource_or_interactor, 'execute')
        return resource_or_interactor.execute() if is_interactor else resource_or_interactor
