from codecs import open
from os import path

from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='drive4data',
    version='0.0.1',
    description='Drive4Data Processing Toolchain',
    long_description=long_description,
    url='https://github.com/N-Coder/drive4data-toolchain',
    author='Simon Dominik `Niko` Fink',
    author_email='sfink@uwaterloo.ca',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],
    packages=find_packages(),
    install_requires=[
        'tabulate>=0.7.5',
        'more-itertools>=2.2',
        'python-geohash>=0.8.5',
        'influxdb>=3.0.0',
        'more-itertools>=2.2',
        'webike'
    ],
    include_package_data=True,
    package_data={
        # 'webike': [
        #     'ui/glade/*'
        # ],
    },
    entry_points={
        "console_scripts": [
            # "webike-timeline = webike.ui.UI:main",
            # "webike-histogram = webike.Histogram:main",
            # "webike-prepocess = webike.Preprocess:main",
        ]
    },
)
