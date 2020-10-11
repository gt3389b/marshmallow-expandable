class RestException(Exception):
    pass


class ResourceException(RestException):
    pass


class RestServerException(RestException):
    pass


class RestValidationException(RestServerException):
    def __init__(self, msg, response=None):
        self.response = response
        super(RestValidationException, self).__init__(msg)

    def add_errors_to_form(self, form):
        for key, errors in self.response.content.items():
            if key == 'non_field_errors':
                field = None
            else:
                field = key
            for error in errors:
                form.add_error(field, error)
        return form


class DoesNotExist(RestException):
    pass
