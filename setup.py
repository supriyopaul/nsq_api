from setuptools import setup
from setuptools import find_packages
setup(
    name="nsq_api",
    version="0.0.1",
    description="API to read logs from NSQ for logagg",
    keywords="nsq-api",
    author="Deep Compute, LLC",
    author_email="contact@deepcompute.com",
    url="https://github.com/deep-compute/nsq-api",
    license='MIT',
    dependency_links=[
        "https://github.com/deep-compute/nsq-api",
    ],
    install_requires=[
        "tornado",
        "basescript==0.2.6",
        "nsq-py==0.1.10",
        "logagg-utils==0.5.0"
    ],
    packages=find_packages('.'),
    include_package_data=True,
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 2.7",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
    ],
    test_suite='test.suite_maker',
    entry_points={
        "console_scripts": [
            "nsq-api = nsq_api:main",
            ]
        }
)
