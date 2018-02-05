from setuptools import setup
from setuptools import find_packages

setup(
    name="pyfusegridfs",
    description='Mount MongoDB GridFS as FUSE filesystem using Python',
    author='Serge Matveenko',
    version=open('VERSION').read().strip('\n') or '0.0.1',
    install_requires=open('requirements.txt').readlines(),
    include_package_data=True,
    package_dir={'pyfusegridfs': 'fusegridfs'},
    packages=find_packages('.'),
    entry_points={
        'console_scripts': [
            'pyfusegridfs = fusegridfs.__main__:main',
        ]
    }
)
