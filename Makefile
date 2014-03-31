# Makefile to simplify some common development tasks.
# Run 'make help' for a list of commands.

PYTHON=`which python`

default: help

help:
	@echo "Available commands:"
	@sed -n '/^[a-zA-Z0-9_.]*:/s/:.*//p' <Makefile | sort

test:
	tox

coverage:
	py.test --cov=sqlparse --cov-report=html --cov-report=term

clean:
	@$(PYTHON) setup.py clean
	@find . -name '*.pyc' -delete
	@find . -name '*~' -delete
	@rm -rf *.egg-info
	@rm -rf dist
	@rm -rf htmlcov
	@find . -name '__pycache__' -delete
