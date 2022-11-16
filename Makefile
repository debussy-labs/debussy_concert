.PHONY: clean-pyc clean-build docs clean install mypy

clean: clean-build clean-pyc clean-test clean-mypy

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

clean-mypy:
	find . -name '.mypy_cache' -exec rm -fr {} +

mypy:
	mypy --namespace-packages --explicit-package-bases debussy_concert

install:
	pip install -e .
