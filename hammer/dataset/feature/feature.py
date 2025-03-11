import wrapt


class Feature(object):
    def __init__(self, source, entity, mode, ttl, timestamp_field, schema, description):
        self.metadata = {
            "source": source,
            "entity": entity,
            "mode": mode,
            "ttl": ttl,
            "timestamp_field": timestamp_field,
            "schema": schema,
            "description": description,
        }

    @wrapt.decorator
    def __call__(self, wrapped, instance, args, kwargs):
        result = wrapped(*args, **kwargs)
        return result

    def get_metadata(self):
        return self.metadata
