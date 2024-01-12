from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from typing import Dict

from swisspollentools.utils.constants import KEY_SEP
from swisspollentools.utils.utils import flatten_dictionary

NoneType = type(None)

class Schema(ABC):
    dtypes = tuple()
    defaults = []

    @abstractmethod
    def __init__(self, schema, validate, allow_defaults):
        pass

    def __post_init__(self):
        pass

    @abstractmethod
    def __getitem__(self, key):
        pass

    @abstractmethod
    def __setitem__(self, key, value):
        pass

    @property
    @abstractmethod
    def schema(self):
        pass

    def __len__(self):
        return len(self.__attr_keys__())

    def __str__(self):
        return str(self.schema)

    def __repr__(self):
        return str(self.schema)

    #def values(self):
    #    return [self[key] for key in self.__attr_keys__()]
    
    #def keys(self):
    #    return [key for key in self.__attr_keys__()]

    @classmethod
    @abstractmethod
    def __attr_keys__(cls):
        pass

    @classmethod
    @abstractmethod
    def __attr_items__(cls):
        pass

    @classmethod
    @abstractmethod
    def fit(cls, schema, allow_defaults):
        pass

    @classmethod
    @abstractmethod
    def empty(cls):
        pass

    @classmethod
    def get_caster(cls, other, translation):
        translation = flatten_dictionary(translation, separator=KEY_SEP)

        def caster(schema):
            if not isinstance(schema, cls):
                raise ValueError()
            
            other_schema = other.empty()
            for other_key, key in translation.items():
                other_schema[other_key] = schema[key]
            return other_schema
        
        return caster

class SchemaDict(Schema):
    keys = tuple()
    defaults = [{}, None]
    allow_undefined_keys=False,
    allow_missing_keys=False,

    def __init__(
        self,
        schema: dict,
        validate=True,
        allow_defaults=True,
    ):
        if not validate:
            self.__schema__ = schema
            return
        
        if allow_defaults and schema in self.defaults:
            self.__schema__ = schema
            return
        
        undefined_keys = list(set(schema.keys()).difference(set(self.__attr_keys__())))
        if undefined_keys and self.allow_undefined_keys:
            raise ValueError(f"Undefined keys were found in the schema: {undefined_keys}")

        missing_keys = list(set(self.__attr_keys__()).difference(set(schema.keys())))
        if missing_keys and not self.allow_missing_keys:
            raise ValueError(f"Missing keys were not found in the schema: {missing_keys}")

        self.__schema__ = {}
        for key, value in self.__attr_items__():
            if self.allow_missing_keys and key not in schema.keys():
                if not isinstance(value, tuple) and issubclass(value, Schema):
                    self[key] = value.empty()
                self[key] = None
                continue

            if not isinstance(value, tuple) and issubclass(value, Schema):
                self[key] = value(
                    schema=schema[key],
                    validate=validate,
                    allow_defaults=allow_defaults
                )
                continue

            if not isinstance(schema[key], value):
                raise ValueError(f"ValueError: key {key} expected {value}, got type {type(schema[key])}")

            self[key] = schema[key]

        self.__post_init__()

    def __getitem__(self, key):
        key = key.split(KEY_SEP, maxsplit=1)

        if len(key) == 2:
            return self[key[0]][key[1]]

        return self.__schema__[key[0]]

    def __setitem__(self, key, value):
        key = key.split(KEY_SEP, maxsplit=1)

        if len(key) == 2:
            self[key[0]][key[1]] = value
            return

        self.__schema__[key[0]] = value

    @property
    def schema(self):
        return {
            key: self[key] if not isinstance(self[key], Schema) \
                else self[key].schema \
                for key in self.__attr_keys__()
        }

    @classmethod
    def __attr_keys__(cls):
        return list(cls.keys)
    
    @classmethod
    def __attr_items__(cls):
        return list(zip(cls.keys, cls.dtypes))

    @classmethod
    def fit(cls, schema: dict, allow_defaults=True):
        if schema in cls.defaults and allow_defaults:
            return True

        undefined_keys = list(set(schema.keys()).difference(set(cls.__attr_keys__())))
        if undefined_keys and cls.allow_undefined_keys:
            return False

        missing_keys = list(set(cls.__attr_keys__()).difference(set(schema.keys())))
        if missing_keys and not cls.allow_missing_keys:
            return False
        
        dtypes_dict = dict(cls.__attr_items__())
        for key, value in schema.items():
            if key not in dtypes_dict and cls.allow_undefined_keys:
                continue

            dtype = dtypes_dict[key]

            if not isinstance(dtype, tuple) and issubclass(dtype, Schema):
                if not dtype.fit(value, allow_defaults=allow_defaults):
                    return False

            elif not isinstance(value, dtype):
                return False

    
        return True

    @classmethod
    def empty(cls):
        empty = {}
        for key, dtype in cls.__attr_items__():
            if not isinstance(dtype, tuple) and issubclass(dtype, Schema):
                empty[key] = dtype.empty()
                continue

            empty[key] = None
        
        empty = cls(schema=empty, validate=False)
        return empty

class SchemaTuple(Schema):
    def __init__(self, schema: list, validate=True, allow_defaults=False):
        if not validate:
            self.__schema__ = list(schema)
            return

        if allow_defaults and schema in self.defaults:
            self.__schema__ = schema
            return

        if not len(self.dtypes) == len(schema):
            raise ValueError()

        self.__schema__ = list(None for _ in self.__attr_keys__())
        for key, value in self.__attr_items__():
            if not isinstance(value, tuple) and issubclass(value, Schema):
                self[key] = value(schema=schema[key], validate=validate, allow_defaults=allow_defaults)
                continue

            if not isinstance(schema[key], value):
                raise ValueError()
            
            self[key] = schema[key]

        self.__post_init__()

    def __getitem__(self, key):
        if isinstance(key, str):
            key = key.split(KEY_SEP, maxsplit=1)

            if len(key) == 2:
                return self[int(key[0])][key[1]]
            
            return self[int(key[0])]

        if not isinstance(key, int):
            raise ValueError()

        return self.__schema__[key]
    
    def __setitem__(self, key, value):
        if isinstance(key, str):
            key = key.split(KEY_SEP, maxsplit=1)

            if len(key) == 2:
                self[int(key[0])][key[1]] = value
                return
            
            self[int(key[0])] = value
            return

        if not isinstance(key, int):
            raise ValueError()

        self.__schema__[key] = value

    @property
    def schema(self):
        return [
            self[key] if not isinstance(self[key], Schema) \
                else self[key].schema \
                for key in self.__attr_keys__()
        ]

    @classmethod
    def __attr_keys__(cls):
        return list(range(len(cls.dtypes)))
    
    @classmethod
    def __attr_items__(cls):
        return {k: cls.dtypes[k] for k in cls.__attr_keys__()}.items()
    
    @classmethod
    def fit(cls, schema: list, allow_defaults=True):
        if schema in cls.defaults and allow_defaults:
            return True

        if not len(cls.dtypes) == len(schema):
            return False
        
        for value, dtype in zip(schema, cls.dtypes):
            if not isinstance(value, tuple) and issubclass(dtype, Schema):
                if not dtype.fit(value, allow_defaults=allow_defaults):
                    return False
            elif not isinstance(value, dtype):
                return False
            
        return True
    
    @classmethod
    def empty(cls):
        empty = []
        for _, dtype in cls.__attr_items__():
            if not isinstance(dtype, tuple) and issubclass(dtype, Schema):
                empty.append(dtype.empty())
                continue

            empty.append(None)
        
        empty = cls(schema=empty, validate=False)
        return empty

def get_auto_caster(cls, others, others_translation):
    
    def auto_caster(schema):
        if cls.fit(schema):
            return cls(schema)
        
        for other, other_translation in zip(others, others_translation):
            if other.fit(schema):
                schema = other(schema)
                caster = other.get_caster(cls, other_translation)
                return caster(schema)

        raise ValueError("`auto_caster` could not fit any of the provided schema definition.")

    return auto_caster
