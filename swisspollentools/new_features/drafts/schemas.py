from abc import abstractmethod
from typing import Any, Callable, Union

__all__ = ["Schema", "SchemaDict", "SchemaList", "SchemaTuple"]

class SchemaMetaclass(type):
    """
    SchemaMetaclass defines the procedure to instantiate Schemas

    Methods:
    --------
    - __call__ : instantiation procedure for Schemas.

    Example:
    --------
    ```
    class MySchema(metaclass=SchemaMetaclass):
        def __init__(self, data, validate):
            pass
            
        def __pot_init__(self):
            pass
        
        @classmethod
        def build(cls, data):
            pass

        @classmethod
        def fit(cls, data):
            pass
    ```

    Note:
    -----
    - Classes build with the SchemaMetaclass requires to define a __post_init__
    function and the build and fit class functions. By default, this methods
    are implemented in the SchemaBase class.
    """
    def __call__(
        cls: type,
        data: Union[type, dict, tuple],
        validate: bool=True
    ):
        """
        The procedure to instantiate an object

        Arguments:
        ----------
        - data: the data to create a schema from
        - validate: the validation option, if set to true the procedure assert
        that the data follows the class schema

        Returns:
        --------
        - An instance of `cls`
        """
        if not isinstance(cls, SchemaMetaclass):
            raise RuntimeError()

        # Validation if required
        if validate and not cls.fit(data):
            raise ValueError()

        # Object instantiation
        object = cls.__new__(cls, data, validate)
        if isinstance(object, cls):
            data = cls.build(data, validate)
            cls.__init__(object, data, validate)
            cls.__post_init__(object)

        return object

class SchemaBase(metaclass=SchemaMetaclass):
    """
    Schema's base class to define the methods for the subclasses using the
    SchemaMetaclass type

    Class Variables:
    ----------------
    - dtypes: properties of the schema

    Methods:
    --------
    - __init_subclass__: initialize the subclasses allowing for parameters. 
    Takes as argument the subclass dtypes which allows to define the properties
    of the schema
    - __init__: set the instance schema as the data is passed in. Note that the
    validation occurs previously following the SchemaMetaclass __call__
    implementation.
    - __post_init__: default post_init empty method
    - fit: validate that data fit the dtypes definition, the function should
    be implemented in the SchemaBase's subclasses
    - build: instantiate the class subschemas, the function should be
    implemented in the SchemaBase's subclasses
    - __get_item__: abstract getitem method
    - __set_item__: abstract setitem method
    """
    def __init_subclass__(
        cls: SchemaMetaclass,
        dtypes: Union[type, dict, tuple]=None,
        **kwargs
    ):
        """
        The procedure to define SchemaBase's subclasses, setting the class
        dtypes and initializing as None the fit and build functions

        Arguments:
        ----------
        - cls: a class of type SchemaMetaclass
        - dtypes: a dtypes schema definition
        """
        cls.dtypes: Union[type, dict, tuple] = dtypes
        cls.fit: Callable[[Any], bool] = None
        cls.build: Callable[[Any], bool] = None

    def __init__(
        self,
        data: Union[list, dict, tuple],
        validate: bool
    ):
        """
        Set the object schema

        Arguments:
        ----------
        - data: data to set to the schema
        - validate: if True a validation step is conducted to assert that the
        data follows the dtypes.

        Note:
        -----
        - The validation step is conducted prior to the object instantiation
        according to the SchemaMetaclass definition.
        """
        self.schema = data

    def __post_init__(self):
        """
        Default post_init empty method
        """
        pass

    def __repr__(self) -> str:
        """
        Representation method

        Returns:
        --------
        - str: the string representation of a schema
        """
        return str(self.schema)

    @abstractmethod
    def __getitem__(self, key: Any) -> Any:
        """
        Abstract getitem method that has to be implemented in the SchemaBase's
        subclasses
        """
        pass

    @abstractmethod
    def __setitem__(self, key: Any, value: Any):
        """
        Abstract setitem method that has to be implemented in the SchemaBase's
        subclasses
        """
        pass

    __str__ = __repr__

class SchemaDict(SchemaBase):
    """
    SchemaDict base class to define the default fit, build, getitem and setitem
    functions.

    Class Variables:
    ----------------
    - dtypes: properties of the schema as a dictionnary
    - allow_missing_key: boolean to define the fit method policy with missing
    variables in the data
    - allow_undefined_keys: boolean to define the fit method policy with key
    present in the data but not defined in the schemas definition

    Methods:
    --------
    - __init_subclass__: initialize the subclasses, taking as
    - fit: validate that data fit the dtypes definition
    - build: instantiate the class subschemas
    - __get_item__: access an item from the schema
    - __set_item__: not implemented yet
    """
    def __init_subclass__(
        cls: SchemaMetaclass,
        allow_missing_keys: bool=False,
        allow_undefined_keys: bool=False,
        **kwargs
    ):
        """
        The procedure to define SchemaDict's subclasses, setting the class
        dtyps with the SchemaBase's implementation and the policy on missing 
        and undefined keys.

        Arguments:
        ----------
        - cls: a class of type SchemaMetaclass
        - allow_missing_keys: policy on missing keys
        - allow_undefined_keys: policy on undefined keys
        """
        super().__init_subclass__(**kwargs)

        cls.allow_missing_keys = allow_missing_keys
        cls.allow_undefined_keys = allow_undefined_keys

        def fit(
            cls: SchemaMetaclass,
            data: dict
        ):
            """
            SchemaDict fit method

            Arguments:
            ----------
            - cls: a class extending SchemaDict class
            - data: the data to validate and fill the schema with

            Returns:
            --------
            - bool

            Raise:
            ------
            - ValueError according to missing/undefined keys policy
            """
            if isinstance(data, cls):
                return True

            dtypes_keys = set(cls.dtypes.keys())
            data_keys = set(data.keys())

            # Check for missing keys
            missing_keys = list(dtypes_keys.difference(data_keys))
            if missing_keys and not cls.allow_missing_keys:
                raise ValueError(
                    f"Missing keys found in data: {missing_keys}"
                )
            
            # Check for undefined keys
            undefined_keys = list(data_keys.difference(dtypes_keys))
            if undefined_keys and not cls.allow_undefined_keys:
                raise ValueError(
                    f"Undefined keys found in data: {undefined_keys}"
                )

            for key, value in data.items():
                if key not in cls.dtypes:
                    if not cls.allow_undefined_keys:
                        return False
                    continue

                dtype = cls.dtypes[key]

                if isinstance(dtype, type) and issubclass(dtype, SchemaBase):
                    if not dtype.fit(value):
                        return False
                    continue

                if not isinstance(value, dtype):
                    return False

            return True

        def build(
            cls: SchemaMetaclass,
            data: dict,
            validate: bool=True
        ):
            """
            Instantiate the subschemas in the data

            Arguments:
            ----------
            - data: a dictionnary with key and value fitting the schema
            - validate: (ignored)

            Returns:
            --------
            - dict
            """
            if isinstance(data, cls):
                return data

            out = {}

            dtypes_keys = set(cls.dtypes.keys())
            for key in dtypes_keys:
                dtype = cls.dtypes[key]

                if isinstance(dtype, type) and issubclass(dtype, SchemaBase):
                    value = None
                    if key in data:
                        value = dtype(data[key], validate)
                    out[key] = value
                    continue

                value = None
                if key in data:
                    value = data[key]
                out[key] = value

            return out

        cls.fit = classmethod(fit)
        cls.build = classmethod(build)

    def __getitem__(
        self,
        key: str
    ) -> Any:
        """
        Get item function to simplify the access to the schema. Intead of using
        `obj.schema[key]`, we can use `obj[key]`.

        Arguments:
        ----------
        - key: key that we want to access

        Returns:
        --------
        - Any

        Raises:
        -------
        - KeyError if the key is not specified in the schema
        """
        if key not in self.dtypes:
            raise KeyError()

        return self.schema[key]

class SchemaList(SchemaBase):
    def __init_subclass__(
        cls: SchemaMetaclass,
        **kwargs
    ):
        """
        The procedure to define SchemaList's subclasses, setting the class
        dtyps with the SchemaBase's implementation

        Arguments:
        ----------
        - cls: a class of type SchemaMetaclass
        """
        super().__init_subclass__(**kwargs)

        def fit(
            cls: SchemaMetaclass,
            data: list
        ):
            """
            SchemaList fit method

            Arguments:
            ----------
            - cls: a class extending SchemaDict class
            - data: the data to validate and fill the schema with

            Returns:
            --------
            - bool
            """
            if isinstance(data, cls):
                return True

            dtype = cls.dtypes

            if isinstance(dtype, type) and issubclass(dtype, SchemaBase):
                return all(dtype.fit(value) for value in data)
            
            return all(isinstance(value, dtype) for value in data)
        
        def build(
            cls: SchemaMetaclass,
            data: list,
            validate: bool=True
        ):
            """
            Instantiate the subschemas in the data

            Arguments:
            ----------
            - data: a dictionnary with key and value fitting the schema
            - validate: validate propagated to the sub-schemas

            Returns:
            --------
            - list
            """
            if isinstance(data, cls):
                return data

            dtype = cls.dtypes
            if isinstance(dtype, type) and issubclass(dtype, SchemaBase):
                return [dtype(value, validate) for value in data]

            return data

        cls.fit = classmethod(fit)
        cls.build = classmethod(build)

    def __getitem__(
        self,
        key: Union[int, str]
    ) -> Any:
        """
        Get item function to simplify the access to the schema. Intead of using
        `obj.schema[key]`, we can use `obj[key]`.

        Arguments:
        ----------
        - key: key that we want to access either as int or string

        Returns:
        --------
        - Any

        Raises:
        -------
        - KeyError if the key is not a number
        """
        if isinstance(key, str) and not key.isnumeric():
            raise KeyError()

        key = int(key)
        return self.schema[key]

    def append(self, value, validate=True):
        """
        Append function adapted to SchemaList

        Argumnets:
        ----------
        - value: value to append to the list
        - validate: validation policy on the value to append if the value is of
        type SchemaBase.
        """
        dtype = self.dtypes
        if isinstance(dtype, type) and issubclass(dtype, SchemaBase):
            self.schema.append(dtype(value, validate))
            return
        
        if isinstance(value, dtype):
            self.schema.append(value)
            return
        
        raise ValueError()

class SchemaTuple(SchemaBase):

    def __init_subclass__(
        cls: SchemaMetaclass,
        **kwargs
    ):
        """
        The procedure to define SchemaTuple's subclasses, setting the class
        dtyps with the SchemaBase's implementation

        Arguments:
        ----------
        - cls: a class of type SchemaMetaclass
        """
        super().__init_subclass__(**kwargs)

        def fit(
            cls: SchemaMetaclass,
            data: Union[list, tuple, SchemaTuple]
        ):
            """
            SchemaTuple fit method

            Arguments:
            ----------
            - cls: a class extending SchemaTuple class
            - data: the data to validate and fill the schema with

            Returns:
            --------
            - bool
            """
            if isinstance(data, cls):
                return True

            dtypes = cls.dtypes

            if not len(dtypes) == len(data):
                raise ValueError(
                    f"Expected {len(dtypes)} values got {len(data)}"
                )

            for dtype, value in zip(dtypes, data):
                if isinstance(dtype, type) and issubclass(dtype, SchemaBase):
                    if not dtype.fit(value):
                        return False
                    continue

                if not isinstance(value, dtype):
                    return False

            return True

        def build(
            cls: SchemaTuple,
            data: tuple,
            validate: bool=True
        ):
            """
            Instantiate the subschemas in the data

            Arguments:
            ----------
            - data: a dictionnary with key and value fitting the schema
            - validate: validate propagated to the sub-schemas

            Returns:
            --------
            - tuple
            """
            if isinstance(data, cls):
                return data

            out = []

            dtypes = cls.dtypes
            for dtype, value in zip(dtypes, data):
                if isinstance(dtype, type) and issubclass(dtype, SchemaBase):
                    value = dtype(value, validate)

                out.append(value)

            return tuple(out)

        cls.fit = classmethod(fit)
        cls.build = classmethod(build)

    def __getitem__(
        self,
        key: Union[int, str]
    ) -> Any:
        """
        Get item function to simplify the access to the schema. Intead of using
        `obj.schema[key]`, we can use `obj[key]`.

        Arguments:
        ----------
        - key: key that we want to access either as int or string

        Returns:
        --------
        - Any

        Raises:
        -------
        - KeyError if the key is not a number
        """
        if isinstance(key, str) and not key.isnumeric():
            raise KeyError()

        key = int(key)
        return self.schema[key]

class Schema:
    """
    User interface for new schema defintion, allowing for nested schemas

    Methods:
    --------
    - new: create a new schema definition based on the dtypes argument

    Example:
    --------
    ```
    MySchema = Schema.new(
        Schema.new({
            "nested": Schema.new({
                "test": int
        })}),
    )

    data = [{"nested": {"test": 1}}]
    schema = MySchema([{"nested": {"test": 1}}])
    schema.append({"nested": {"test": 2}})
    print(schema)

    # Outputs a SchemaList with nested SchemaDict
    # [{'nested': {'test': 1}}, {'nested': {'test': 2}}]
    ```

    Note:
    -----
    - The `Schema.new` method does not allow to define a Schema post_init
    function. If required, the user has to extend the `SchemaDict`,
    `SchemaList` or `SchemaTuple` classes.
    """
    @classmethod
    def new(cls, dtypes, **kwargs):
        
        if isinstance(dtypes, dict):
            class schema(SchemaDict, dtypes=dtypes, **kwargs):
                pass
        elif isinstance(dtypes, type):
            class schema(SchemaList, dtypes=dtypes, **kwargs):
                pass
        elif isinstance(dtypes, tuple):
            class schema(SchemaTuple, dtypes=dtypes, **kwargs):
                pass

        return schema
