from setuptools import setup, find_packages

VERSION = '0.2.0'

with open('requirements.txt') as f:
    DEPENDENCIES = f.read().split('\n')

setup(
    name = 'bqorm',
    version = VERSION,
    description = 'Python Micro-ORM for BigQuery',
    author = 'Jonathan Rahn',
    author_email = 'jonathan.rahn@42digital.de',
    url = 'https://github.com/42DIGITAL/bqorm',
    packages = find_packages(exclude=['tests']),
    install_requires=DEPENDENCIES,
    extras_require={'test': ['pytest']},
)
