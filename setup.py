# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="debussy_concert",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "Inject",
        "yaml_env_var_parser",
        "dbt-bigquery"
    ]
)
