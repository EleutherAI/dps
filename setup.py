import sys
from setuptools import setup, find_packages

PKGNAME = "dps"
PKGVERSION = "0.1.0"

PYTHON_VERSION = (3, 8)

if sys.version_info < PYTHON_VERSION:
    sys.exit(
        "**** Sorry, {} {} needs at least Python {}".format(
            PKGNAME, PKGVERSION, ".".join(map(str, PYTHON_VERSION))
        )
    )


def requirements(filename="requirements-df.txt"):
    """Read the requirements file"""
    with open(filename, "r") as f:
        return [line.strip() for line in f if line and line[0] != "#"]


setup(
    name=PKGNAME,
    version=PKGVERSION,
    packages=find_packages("."),
    license="Apache",
    entry_points={
        "console_scripts": [
            "dps-run-spark = dps.spark_df.app.sparkapp:main"
        ]
    },
    python_requires=">=3.8",
    install_requires=requirements()
)
