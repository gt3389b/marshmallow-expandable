import re
import urllib

from marshmallow_expandable.utils import reverse

class ResourcePattern(object):
    """
    # TODO: This class needs cleaning up and refactoring.
    """

    def __init__(self, pattern, obj_path=None):
        self.pattern = pattern
        self.obj_path = obj_path

    @classmethod
    def parse(cls, obj):
        if isinstance(obj, tuple):
            return cls(*obj)
        return cls(obj)

    def params_from_uri(self, uri):
        return re.search(self.pattern.strip('^$'), uri).groupdict()

    def clean(self, response):
        if self.obj_path:
            return response.content[self.obj_path]
        return response.content

    def encode_obj(self, in_obj):
        def encode_list(in_list):
            out_list = []
            for el in in_list:
                out_list.append(self.encode_obj(el))
            return out_list

        def encode_dict(in_dict):
            out_dict = {}
            for k, v in in_dict.iteritems():
                out_dict[k] = self.encode_obj(v)
            return out_dict

        if isinstance(in_obj, unicode):
            return in_obj.encode('utf-8')
        elif isinstance(in_obj, list):
            return encode_list(in_obj)
        elif isinstance(in_obj, tuple):
            return tuple(encode_list(in_obj))
        elif isinstance(in_obj, dict):
            return encode_dict(in_obj)

        return in_obj

    def get_url(self, query=None, **kwargs):
        if query:
            query = '?%s' % urllib.urlencode(self.encode_obj(query))
        else:
            query = ''
        return '%s%s' % (reverse(self.pattern, **kwargs), query)

    def get_absolute_url(self, root=None, query=None, **kwargs):
        if root is None:
            root = ''
        return '%s%s' % (root, self.get_url(query, **kwargs))
