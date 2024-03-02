# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name="common_crawler",
    version="1.0.1",
    packages=find_packages(),
    entry_points={"scrapy": ["settings = common_crawler.settings"]},
)
