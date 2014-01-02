from setuptools import setup

setup(
    name='txkazoo',
    version='0.0.3',
    description='Twisted binding for Kazoo',
    maintainer='Manish Tomar',
    maintainer_email='manish.tomar@rackspace.com',
    license='Apache 2.0',
    py_modules=['txkazoo'],
    install_requires=['twisted==13.1.0', 'kazoo==1.3.1']
)
