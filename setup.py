import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dbxscd",
    version="0.0.5",
    author="Sean Ma",
    author_email="maye-msft@outlook.com",
    description="Slowly Changing Dimensions implemenation with Databricks Delta Lake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/maye-msft/SlowlyChangingDimensionsInDeltaLake",
    project_urls={
        "Bug Tracker": "https://github.com/maye-msft/SlowlyChangingDimensionsInDeltaLake/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)