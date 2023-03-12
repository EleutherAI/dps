
# Package name
NAME := dps


# Virtualenv to install in. In this order:
#   1. the one given by the VENV environment variable
#   2. an active one (as given by the VIRTUAL_ENV environment variable)
#   3. a default
VENV ?= $(shell echo $${VIRTUAL_ENV:-/opt/venv/dps})

PYTHON ?= python3
VENV_PYTHON ?= $(VENV)/bin/python3


# Package version: taken from the __init__.py file
VERSION_FILE := setup.py
VERSION	     := $(shell grep "^PKGVERSION =" $(VERSION_FILE) | sed -r "s/^PKGVERSION\s*=\s*\"(.*)\"/\1/")

PKGFILE := dist/$(NAME)-$(VERSION).tar.gz


# --------------------------------------------------------------------------

all:
	@echo "VERSION = $(VERSION)"
	@echo "use 'make pkg' to build the package"

build pkg: $(PKGFILE)

clean:
	rm -f "$(PKGFILE)"

rebuild: clean build

version:
	@echo "$(VERSION)"

# --------------------------------------------------------------------------

$(PKGFILE): $(VERSION_FILE) setup.py
	$(VENV_PYTHON) setup.py sdist

