from setuptools import find_packages, setup

package_name = "txkazoo"

import re
version_line = open("{0}/_version.py".format(package_name), "rt").read()
match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_line, re.M)
version_string = match.group(1)

with open("requirements.txt") as requirements_file:
    dependencies = map(str.split, requirements_file.read().split())

setup(
    name=package_name,
    version=version_string,

    description='Twisted binding for Kazoo',
    long_description=open("README.md").read(),
    url="https://github.com/rackerlabs/txkazoo",

    maintainer='Manish Tomar',
    maintainer_email='manish.tomar@rackspace.com',
    license='Apache 2.0',

    packages=find_packages(),
    install_requires=dependencies
)
