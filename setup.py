#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="dataframe_processor_framework",
    version="0.1.0",
    author="Author Name",
    author_email="author@example.com",
    description="Interactive UI framework for processing pandas DataFrames in Google Colab",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/username/dataframe_processor_framework",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Scientific/Engineering :: Visualization",
        "Topic :: Software Development :: User Interfaces",
    ],
    python_requires=">=3.6",
    install_requires=[
        "pandas>=1.0.0",
        "ipywidgets>=7.0.0",
        "notebook>=5.0.0",
    ],
    keywords="pandas, dataframe, processing, google colab, ui, widgets",
)
