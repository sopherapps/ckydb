import pathlib

from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="ckydb",
    version="0.0.4",
    description="A simple fast memory-first thread-safe (or goroutine-safe for Go) key-value embedded database that persist data on disk.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/sopherapps/ckydb/tree/master/implementations/py_ckydb",
    author="Martin Ahindura",
    author_email="team.sopherapps@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(exclude=("tests",)),
    include_package_data=True,
    entry_points={
    },
)
