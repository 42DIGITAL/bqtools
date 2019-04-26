from setuptools import setup, find_packages

VERSION = '0.3.2'

with open('README.md', 'r') as f:
    LONG_DESCRIPTION = f.read()

with open('requirements.txt') as f:
    DEPENDENCIES = f.read().split('\n')

setup(
    name = 'bqtools',
    version = VERSION,
    description = 'Python Tools for BigQuery',
    long_description = LONG_DESCRIPTION,
    author = 'Jonathan Rahn',
    author_email = 'jonathan.rahn@42digital.de',
    url = 'https://github.com/42DIGITAL/bqtools',
    packages = find_packages(exclude=['tests']),
    install_requires=DEPENDENCIES,
    extras_require={'test': ['pytest']},
    classifiers=[
        'Development Status :: 4 - Beta',
        'Topic :: Database',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
