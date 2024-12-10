#!/usr/bin/env python3
# -*- coding: utf-8 -*

import os
import re
from setuptools import setup

setup_root = os.path.dirname(os.path.abspath(__file__))
requirements = os.path.join(setup_root, 'requirements.txt')
delay_package = os.path.join(setup_root, 'quickdist', '__init__.py')
readme = os.path.join(setup_root, 'README.md')


def find_packages(include=None):
    def impl(root, dir, packages):
        root_dir = os.path.join(root, dir)
        filenames = os.listdir(root_dir)
        for filename in filenames:
            fullpath = os.path.join(root_dir, filename)
            if os.path.isdir(fullpath):
                subdir = os.path.join(dir, filename)
                impl(root, subdir, packages)
            else:
                if filename == '__init__.py':
                    packages.append(dir)
    packages = []
    for top in include:
        impl(setup_root, top, packages)
    return packages


with open(requirements, 'rb') as f:
    install_requires = f.read().decode('utf-8').splitlines(keepends=False)

with open(delay_package, 'rb') as f:
    content = f.read().decode('utf-8')
    match = re.findall(r'__version__\s*=\s*["\'](.*)["\']', content)
    if match:
        version = match[0]
    else:
        version = '0.0.1'

with open(readme, 'rb') as f:
    readme_content = f.read().decode('utf-8')

print(find_packages(include=['quickdist']))

setup(
    name="quickdist",
    version=version,
    keywords=["distribute"],
    description="Quick run python script on multi-pc in multi-process.",
    long_description=readme_content,
    license="Do what ever you what.",

    author="Leval.up",
    author_email="likier@sina.cn",

    packages=find_packages(include=['quickdist']),

    include_package_data=True,
    package_data={},

    platforms="any",
    install_requires=install_requires,

    scripts=[],
    entry_points={
        'console_scripts': [
            'quickdist = quickdist:main',
        ]
    },
    zip_safe=False,

    classifiers=[
        'Programming Language :: Python', 'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)
