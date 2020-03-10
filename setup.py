"""
Setup and installation for the package.
"""

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

def requirements():
    """Build the requirements list for this project"""
    requirements_list = []

    with open('requirements.txt') as requirements:
        for install in requirements:
            requirements_list.append(install.strip())

    return requirements_list

packages = find_packages(exclude=['tests*', 'temp*'])

setup(
    name="airbnb-scraper",
    version="0.1.0",
    url="https://github.com/diatche/airbnb-scraper",
    author="Pavel Diatchenko",
    author_email="diatche@gmail.com",
    description="Spider built with scrapy and ScrapySplash to crawl Airbnb listings",
    keywords=["Airbnb", "scraping"],
    packages=packages,
    install_requires=requirements(),
    include_package_data=True,
    license="MIT",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
    ],
)
