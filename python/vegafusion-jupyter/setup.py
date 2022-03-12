#!/usr/bin/env python
# coding: utf-8

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from glob import glob
import os
from os.path import join as pjoin
from setuptools import setup, find_packages


from jupyter_packaging import (
    create_cmdclass,
    install_npm,
    ensure_targets,
    combine_commands,
    get_version,
    skip_if_exists
)

HERE = os.path.dirname(os.path.abspath(__file__))

# The name of the project
name = 'vegafusion-jupyter'
module = name.replace('-', '_')

# Get the version
version = get_version(pjoin(module, '_version.py'))

# Representative files that should exist after a successful build
jstargets = [
    pjoin(HERE, module, 'nbextension', 'index.js'),
    pjoin(HERE, module, 'labextension', 'package.json'),
]

package_data_spec = {
    name: [
        'nbextension/**js*',
        'labextension/**'
    ]
}


data_files_spec = [
    ('share/jupyter/nbextensions/vegafusion-jupyter', 'vegafusion_jupyter/nbextension', '**'),
    ('share/jupyter/labextensions/vegafusion-jupyter', 'vegafusion_jupyter/labextension', '**'),
    ('share/jupyter/labextensions/vegafusion-jupyter', '.', 'install.json'),
    ('etc/jupyter/nbconfig/notebook.d', '.', 'vegafusion-jupyter.json'),
]


cmdclass = create_cmdclass('jsdeps', package_data_spec=package_data_spec,
    data_files_spec=data_files_spec)

npm_install = combine_commands(
    install_npm(HERE, build_cmd='build:prod'),
    ensure_targets(jstargets),
)

cmdclass['jsdeps'] = skip_if_exists(jstargets, npm_install)


setup_args = dict(
    name            = name,
    description     = 'Altair Jupyter Widget library that relies on VegaFusion for serverside calculations',
    version         = version,
    scripts         = glob(pjoin('scripts', '*')),
    cmdclass        = cmdclass,
    packages        = find_packages(".", exclude=["tests"]),
    author          = 'VegaFusion Technologies LLC',
    author_email    = 'jon@vegafusion.io',
    url             = 'https://github.com/jonmmease/vegafusion',
    license         = 'AGPL-3.0-or-later',
    platforms       = "Linux, Mac OS X, Windows",
    keywords        = ['Jupyter', 'Widgets', 'IPython'],
    classifiers     = [
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Framework :: Jupyter',
    ],
    include_package_data = True,
    python_requires=">=3.7",
    install_requires = [
        'ipywidgets>=7.0.0',
        'altair>=4.2.0',
        'vegafusion',
    ],
    extras_require={
        'embed': ["vegafusion-python-embed"],
    },
    entry_points = {
    },
)

if __name__ == '__main__':
    setup(**setup_args)
