from os.path import dirname, join
from setuptools import find_packages, setup

package_name = "txkazoo"

def read(path):
    with open(join(dirname(__file__), path)) as f:
        return f.read()

import re
version_line = read("{0}/_version.py".format(package_name))
match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_line, re.M)
version_string = match.group(1)

dependencies = map(str.split, read("requirements.txt").split())

setup(
    name=package_name,
    version=version_string,
    description='Twisted binding for Kazoo',
    long_description=open("README.md").read(),
    url="https://github.com/rackerlabs/txkazoo",

    author='Manish Tomar',
    author_email='manish.tomar@rackspace.com',
    maintainer='Manish Tomar',
    maintainer_email='manish.tomar@rackspace.com',

    license='Apache 2.0',
    keywords="crypto urwid twisted",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: Twisted",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2 :: Only",
        "Programming Language :: Python :: 2.7",
        "Topic :: System :: Distributed Computing"
    ],

    packages=find_packages(),
    install_requires=dependencies,
    zip_safe=True
)
