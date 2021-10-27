from setuptools import setup, find_packages

setup(
    name="data-profiler",
    version="0.4",
    description="CLI to submit profiling jobs which run on Databricks and save results to Snowflake",
    url="TBD",
    author="Peter Katz",
    author_email="pkatz2690@gmail.com",
    license="MIT",
    packages=find_packages(),
    install_requires=["click==8.0.1", "requests==2.25.1"],
    entry_points={
        "console_scripts": [
            "data-profiler = cli.profiler:cli",
        ],
    },
)
