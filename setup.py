from setuptools import setup

# Todo: Parse this from a proper readme file in the future
description='T2K Data Manager'
long_description = """t2kdm - T2K Data Manager

Provides functions for a smoother handling of grid data within the T2K
experiment.

"""

setup(name='t2kdm',
    version='1.4.0',
    description=description,
    long_description=long_description,
    url='https://github.com/t2k-software/t2kdm',
    author='Lukas Koch',
    author_email='lukas.koch@mailbox.org',
    license='MIT',
    packages=['t2kdm'],
    install_requires=[
        'sh>=1.12.14',
        'six>=1.10.0',
        'appdirs>=1.4.3',
    ],
    extras_require = {
    },
    python_requires='>=2.6',
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',

        # Pick your license as you wish (should match "license" above)
         'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    entry_points = {
        'console_scripts': [
            't2kdm-ls=t2kdm.commands:ls.run_from_console',
            't2kdm-replicas=t2kdm.commands:replicas.run_from_console',
            't2kdm-replicate=t2kdm.commands:replicate.run_from_console',
            't2kdm-remove=t2kdm.commands:remove.run_from_console',
            't2kdm-SEs=t2kdm.commands:SEs.run_from_console',
            't2kdm-get=t2kdm.commands:get.run_from_console',
            't2kdm-put=t2kdm.commands:put.run_from_console',
            't2kdm-check=t2kdm.commands:check.run_from_console',
            't2kdm-fix=t2kdm.commands:fix.run_from_console',
            't2kdm-cli=t2kdm.cli:run_cli',
            't2kdm-tests=t2kdm.tests:run_tests',
            't2kdm-config=t2kdm.configuration:run_configuration_wizard',
            't2kdm-maid=t2kdm.maid:run_maid',
        ],
    },
    zip_safe=True)
