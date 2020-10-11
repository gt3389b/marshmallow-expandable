import re
import sys

def reverse(pattern, **kwargs):
    template = pattern.strip('^$')

    start = re.compile(r'\(\?P\<')
    end = re.compile(r'\>[^\)]*\)')

    template = start.sub('%(', template)
    template = end.sub(')s', template)

    try:
        result = template % kwargs
    except KeyError as e:
        raise ValueError('The URL pattern requires %s as named argument.' % e)

    return result


class patch(object):
    def __init__(self, dotted_path, new):
        self._dotted_path = dotted_path
        self._new = new
        try:
            module_path, class_name = self._dotted_path.rsplit('.', 1)
        except ValueError:
            msg = "%s doesn't look like a module path" % dotted_path
            raise Exception(msg, sys.exc_info()[2])
        self._module_path = module_path
        self._class_name = class_name
        self._module = import_module(self._module_path)
        try:
            self._old = getattr(self._module, self._class_name)
        except AttributeError:
            msg = 'Module "%s" does not define a "%s" attribute/class' % (self._module_path, self._class_name)
            raise Exception(msg, sys.exc_info()[2])

    def __enter__(self):
        setattr(self._module, self._class_name, self._new)
        return self._new

    def __exit__(self, type, value, traceback):
        setattr(self._module, self._class_name, self._old)
