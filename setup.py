import sys, os
import scrapy_rabbitmq

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


from codecs import open

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()


packages = [
    'scrapy_rabbitmq'
]

requires = [
    'pika',
    'scrapy>=1.0'
]

setup(
    name='scrapy-rabbitmq',
    author='Royce Haynes',
    description='RabbitMQ Plug-in for Scrapy',
    version='0.1.2',
    author_email='royce.haynes@gmail.com',
    license='MIT',
    url='https://github.com/roycehaynes/scrapy-rabbitmq',
    install_requires=requires,
    packages=packages
)