import sys
from pathlib import Path
from shutil import copytree

from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop

from dps.spark_df import VERSION

PKGNAME = "dps"
PYTHON_MIN_VERSION = (3, 8)

if sys.version_info < PYTHON_MIN_VERSION:
    sys.exit(
        "**** Sorry, {} {} needs at least Python {}".format(
            PKGNAME, VERSION, ".".join(map(str, PYTHON_MIN_VERSION))
        )
    )


def post_install_hook():
    '''
    Execute post-install tasks
    '''
    src_dir = Path(__file__).parent / "configs"
    dst_dir = Path(sys.prefix) / "etc" / "dps"
    try:
        copytree(src_dir, dst_dir, dirs_exist_ok=True)
    except Exception as e:
        print(repr(e), file=sys.stderr, flush=True)
        raise


class PostInstall(install):
    def run(self):
        super().run()
        post_install_hook()


class PostDevelop(develop):
    def run(self):
        super().run()
        raise Exception()
        post_install_hook()


def requirements(filename="requirements-df.txt"):
    """Read the requirements file"""
    with open(filename, "r") as f:
        return [line.strip() for line in f if line and line[0] != "#"]


setup(
    name=PKGNAME,
    version=VERSION,
    packages=find_packages("."),
    license="Apache",
    entry_points={
        "console_scripts": [
            "dps-run-spark = dps.spark_df.app.sparkapp:main"
        ]
    },
    python_requires=">=3.8",
    install_requires=requirements(),
    cmdclass={'install': PostInstall,
              'develop': PostDevelop}
)
