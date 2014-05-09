from setuptools import find_packages, setup

with open("requirements.txt") as requirements_file:
    dependencies = map(str.split, requirements_file.read().split())

setup(
    name='txkazoo',
    version='0.0.4',

    description='Twisted binding for Kazoo',
    long_description=open("README.md").read(),
    url="https://github.com/rackerlabs/txkazoo",

    maintainer='Manish Tomar',
    maintainer_email='manish.tomar@rackspace.com',
    license='Apache 2.0',

    packages=find_packages(),
    install_requires=dependencies
)
