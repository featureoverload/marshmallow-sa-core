import types
from typing import Any, List
from marshmallow import (
    EXCLUDE,
    Schema,
    SchemaOpts,
    post_dump,
    post_load,
    pre_load
)

import marshmallow_sa_core

VERSION = marshmallow_sa_core.__version__


class ObjectSchemaOptions(SchemaOpts):
    def __init__(self, meta: Any, **kwargs: Any) -> None:
        super().__init__(meta, **kwargs)
        self.object_class = getattr(meta, "object_class", None)
        self.exclude_fields = getattr(meta, "exclude_fields", None) or []
        self.unknown = getattr(meta, "unknown", EXCLUDE)


class ObjectSchema(Schema):
    """
    This Marshmallow Schema automatically instantiates an object whose type is indicated by the
    `object_class` attribute of the class `Meta`. All deserialized fields are passed to the
    constructor's `__init__()` unless the name of the field appears in `Meta.exclude_fields`.
    """

    OPTIONS_CLASS = ObjectSchemaOptions

    class Meta:
        object_class = None  # type: type
        exclude_fields = []  # type: List[str]
        unknown = EXCLUDE

    @pre_load
    def _remove_version(self, data: dict, **kwargs: Any) -> dict:
        """
        Removes a __version__ field from the data, if present.

        Args:
            - data (dict): the serialized data

        Returns:
            - dict: the data dict, without its __version__ field
        """
        # don't mutate data
        data = data.copy()
        data.pop("__version__", None)
        return data

    @post_dump
    def _add_version(self, data: dict, **kwargs: Any) -> dict:
        """
        Adds a __version__ field to the data, if not already provided.

        Args:
            - data (dict): the serialized data

        Returns:
            - dict: the data dict, with an additional __version__ field
        """
        # don't mutate data
        data = data.copy()
        data.setdefault("__version__", VERSION)
        return data

    def load(self, data: dict, create_object: bool = True, **kwargs: Any) -> Any:
        """
        Loads an object by first retrieving the appropate schema version (based on the data's
        __version__ key).

        Args:
            - data (dict): the serialized data
            - create_object (bool): if True, an instantiated object will be returned. Otherwise,
                the deserialized data dict will be returned.
            - **kwargs (Any): additional keyword arguments for the load() method

        Returns:
            - Any: the deserialized object or data
        """
        self.context.setdefault("create_object", create_object)
        return super().load(data, **kwargs)

    @post_load
    def create_object(self, data: dict, **kwargs: Any) -> Any:
        """
        By default, returns an instantiated object using the ObjectSchema's `object_class`.
        Otherwise, returns a data dict.

        Args:
            - data (dict): the deserialized data

        Returns:
            - Any: an instantiated object, if `create_object` is found in context; otherwise,
                the data dict
        """
        if self.context.get("create_object", True):
            object_class = self.opts.object_class
            if object_class is not None:
                if isinstance(object_class, types.FunctionType):
                    object_class = object_class()
                init_data = {
                    k: v for k, v in data.items() if k not in self.opts.exclude_fields
                }
                return object_class(**init_data)
        return data
