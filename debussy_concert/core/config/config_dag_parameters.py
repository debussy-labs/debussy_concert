import datetime as dt


class ConfigDagParameters:
    dag_id: str
    start_date: dt.datetime

    def __init__(
            self,
            **kwargs
    ):
        self.__dict__ = kwargs

    @classmethod
    def create_from_dict(cls, dag_parameters_dict):
        start_date = dag_parameters_dict['start_date']
        start_date = dt.datetime(**start_date)
        dag_parameters_dict['start_date'] = start_date
        end_date = dag_parameters_dict['end_date']
        end_date = dt.datetime(**end_date)
        dag_parameters_dict['end_date'] = end_date
        return cls(**dag_parameters_dict)

    def keys(self):
        return self.__dict__.keys()

    def __getitem__(self, key):
        return getattr(self, key)
