from setuptools import setup, find_packages

setup(
    name="dps",
    version="0.0.1",
    packages=find_packages("."),
    install_requires=[
        'pyspark==3.2.1',
        'fire==0.3.1',
        'pyyaml'
    ]
)