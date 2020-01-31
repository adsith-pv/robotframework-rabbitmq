"""Setup module for Robot Framework RabbitMq Http Library package."""

# To use a consistent encoding
from codecs import open
from os import path

from setuptools import setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst')) as f:
    long_description = f.read()

# Get install requires from requirements.txt
with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

setup(
    name='robotframework-http-rabbitmq',
    version='1.0.3',
    description='A Robot Framework RabbitMq Library',
    long_description=long_description,
    url='https://github.com/adsith-pv/robotframework-http-rabbitmq',
    author='Carlos Alvarez',
    author_email='c.alvarez@payvision.com',
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Testing',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Framework :: Robot Framework :: Library',
    ],
    keywords='testing testautomation robotframework rabbitmq http',
    package_dir={'': 'src'},
    py_modules=['RabbitMqHttp'],
    install_requires=requirements,
)
