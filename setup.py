from setuptools import setup, find_packages
with open("README.txt", "r") as fh:
    long_description = fh.read()

setup(
    name='lobsteronaws',
    version='0.1.2',
    author="Ruihong Huang",
    author_email="huang@lobsterdata.com",
    packages=find_packages(),
    license='MIT License',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vonde-consulting/lobsteronaws",
)
