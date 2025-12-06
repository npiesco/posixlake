from setuptools import setup, Distribution
import platform
from pathlib import Path

# Determine the library file name based on platform
if platform.system() == "Darwin":
    lib_name = "libposixlake.dylib"
elif platform.system() == "Windows":
    lib_name = "posixlake.dll"
else:
    lib_name = "libposixlake.so"

# Read the README file for PyPI description
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

# Force platform-specific wheel since we have native code
class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True

setup(
    name="posixlake",
    version="0.1.5",
    distclass=BinaryDistribution,
    description="High-performance Delta Lake database with POSIX interface and Python bindings",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="posixlake Contributors",
    author_email="",
    license="MIT",
    url="https://github.com/npiesco/posixlake",
    project_urls={
        "Bug Tracker": "https://github.com/npiesco/posixlake/issues",
        "Documentation": "https://github.com/npiesco/posixlake#readme",
        "Source Code": "https://github.com/npiesco/posixlake",
    },
    packages=["posixlake"],
    package_data={"posixlake": [lib_name]},
    include_package_data=True,
    python_requires=">=3.8",
    keywords=[
        "database",
        "delta-lake",
        "sql",
        "parquet",
        "rust",
        "datafusion",
        "time-travel",
        "acid",
        "analytics",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Rust",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
        "Operating System :: OS Independent",
    ],
)

