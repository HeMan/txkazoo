from os.path import dirname, join
from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand
from sys import exit

package_name = "txkazoo"

def read(path):
    with open(join(dirname(__file__), path)) as f:
        return f.read()

import re
version_line = read("{0}/_version.py".format(package_name))
match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]$", version_line, re.M)
version_string = match.group(1)

dependencies = map(str.split, read("requirements.txt").split())

class Tox(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import tox
        exit(tox.cmdline([]))

setup(name=package_name,
      version=version_string,
      description='Twisted binding for Kazoo',
      long_description=read("README.md"),
      url="https://github.com/rackerlabs/txkazoo",

      author='Manish Tomar',
      author_email='manish.tomar@rackspace.com',
      maintainer='Manish Tomar',
      maintainer_email='manish.tomar@rackspace.com',

      license='Apache 2.0',
      keywords="twisted kazoo zookeeper distributed",
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
      test_suite="{0}.test".format(package_name),
      cmdclass={'test': Tox},

      zip_safe=True)
