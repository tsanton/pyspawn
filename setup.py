import setuptools
import os

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setuptools.setup(
    name="pyspawn",
    version= os.getenv("PYSPAWN_VERSION_NUM"),
    author="Tobias Antonsen",
    author_email="tobias@tsant.no",
    description="Intelligent database cleaner for integration tests ported from Jbogard/Respawn.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Tsanton/pyspawn",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "pyodbc>=4.0.32",
        "psycopg2>=2.8.5"
    ],
    packages=setuptools.find_packages(where="pyspawn", exclude=["tests"]),
    python_requires=">=3.6",
)