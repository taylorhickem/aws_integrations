from setuptools import setup, find_packages

setup(
    name='aws_integrations',
    version='1.0',
    packages=find_packages(),
    url='https://github.com/taylorhickem/aws_integrations',
    description='a library of useful functions and classes for integrating AWS SDK with other python packages',
    author='@taylorhickem',
    long_description=open('README.md').read(),
    install_requires=open("requirements.txt", "r").read().splitlines(),
    include_package_data=True,
)