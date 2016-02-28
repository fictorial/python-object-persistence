from setuptools import setup, find_packages

with open('README.rst') as f:
  readme = f.read()

with open('LICENSE') as f:
  license = f.read()

setup(
  name='python-object-persistence',
  version='0.9.5',
  description='Easily save, load, query your Python objects with SQLite3.',
  long_description=readme,
  author='Brian Hammond',
  author_email='brian@fictorial.com',
  url='https://github.com/fictorial/python-object-persistence',
  license=license,
  packages=find_packages(exclude=('tests', 'docs'))
)
