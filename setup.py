from setuptools import find_packages, setup


# Note:
#   The Hitchiker's guide to python provides an excellent, standard, method for creating python packages:
#       http://docs.python-guide.org/en/latest/writing/structure/
#
#   To deploy on PYPI follow the instructions at the bottom of:
#       https://packaging.python.org/tutorials/distributing-packages/#uploading-your-project-to-pypi

with open("README.md") as f:
    readme_text = f.read()

with open("LICENSE") as f:
    license_text = f.read()

setup(
    name="data_gateway",
    version="0.5.1",
    install_requires=[
        "click>=7.1.2",
        "pyserial==3.5",
        "python-slugify==5.0.2",
        "octue==0.4.10",
    ],
    url="https://gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway",
    license="MIT",
    author="OST Aerosense",
    description="A data_gateway that runs on-nacelle for relaying data streams from aerosense nodes to cloud.",
    long_description=readme_text,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=("tests", "docs")),
    include_package_data=True,
    entry_points="""
    [console_scripts]
    gateway=data_gateway.cli:gateway_cli
    """,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    keywords=["aerosense", "wind", "energy", "blades"],
)
