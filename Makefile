
# Customize creation by defining the environment variables PYTHON and/or VENV


# Virtualenv to install in. In this order:
#   1. the one given by the VENV environment variable
#   2. an active one (as given by the VIRTUAL_ENV environment variable)
#   3. a default
VENV ?= $(shell echo $${VIRTUAL_ENV:-/opt/venv/dps})

# Python executable to create the virtualenv with
PYTHON ?= python3

# --------------------------------------------------------------------------


# Package version: taken from the __init__.py file
VERSION_FILE := dps/spark_df/__init__.py
VERSION	     := $(shell grep "VERSION =" $(VERSION_FILE) | sed -r "s/^VERSION\s*=\s*\"(.*)\"/\1/")

# Package name
NAME := dps

PKGFILE := dist/$(NAME)-$(VERSION).tar.gz

VENV_PYTHON ?= $(VENV)/bin/python3


# --------------------------------------------------------------------------

all:
	@echo "VERSION = $(VERSION)"
	@echo "VIRTUALENV = $(VENV)"
	@echo "use 'make pkg' to build the package"
	@echo "use 'make install' to install the package into $(VENV)"


build pkg: $(PKGFILE)

clean:
	rm -f "$(PKGFILE)"

rebuild: clean build


version:
	@echo "$(VERSION)"


install: $(VENV) $(PKGFILE)
	$(VENV)/bin/pip install $(PKGFILE)


reinstall: $(VENV) rebuild install


# --------------------------------------------------------------------------


$(PKGFILE): $(VENV) $(VERSION_FILE) setup.py requirements-df.txt
	$(VENV_PYTHON) setup.py sdist


$(VENV):
	mkdir -p $@
	$(PYTHON) -m venv $@
	$@/bin/pip install --upgrade pip wheel
