import yaml
import warnings
import json


class ConfigBase:

    @classmethod
    def load_from_file(cls, file_path):
        with open(file_path, 'r') as file:
            env_config = yaml.safe_load(file)
        return cls(**env_config)

    def __getitem__(self, key):
        """
            Called to implement evaluation of self[key]
            https://docs.python.org/3/reference/datamodel.html#object.__getitem__
        """
        warnings.warn("Getting property using dictionary access, please update to use class property",
                      category=FutureWarning)
        if not isinstance(key, str):
            raise TypeError('key must be strings')
        return getattr(self, key)

    def __setitem__(self, key, value):
        """
            Called to implement assignment to self[key]
            https://docs.python.org/3/reference/datamodel.html#object.__setitem__
        """
        warnings.warn("Setting property using dictionary setter, please update to use class property",
                      category=FutureWarning)
        if not isinstance(key, str):
            raise TypeError('key must be strings')
        setattr(self, key, value)

    def __delitem__(self, key):
        raise NotImplementedError()

    def __repr__(self):
        return self.to_json()

    def to_json(self):
        return json.dumps(self.__dict__, default=repr)

    def save(self, path):
        with open(path, 'w') as file:
            file.write(self.to_json())
