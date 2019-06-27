from setuptools import setup

# Todo: Parse this from a proper readme file in the future
description='HyperK Data Manager'
long_description = """hkdm - HyperK Data Manager

Provides functions for a smoother handling of grid data within the HyperK
experiment. A fork of the T2K Data Manager t2kdm.

"""

def get_version():
    """Get the version number by parsing the package's __init__.py."""
    with open("hkdm/__init__.py", 'rt') as f:
        for line in f:
            if line.startswith("__version__ = "):
                return eval(line[14:])
        else:
            raise RuntimeError("Could not determine package version!")

setup(name='hkdm',
    version=get_version(),
    description=description,
    long_description=long_description,
    url='https://github.com/t2k-software/t2kdm',
    author='Lukas Koch',
    author_email='lukas.koch@mailbox.org',
    license='MIT',
    packages=['hkdm'],
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
            'hkdm-ls=hkdm.commands:ls.run_from_console',
            'hkdm-replicas=hkdm.commands:replicas.run_from_console',
            'hkdm-replicate=hkdm.commands:replicate.run_from_console',
            'hkdm-remove=hkdm.commands:remove.run_from_console',
            'hkdm-rmdir=hkdm.commands:rmdir.run_from_console',
            'hkdm-SEs=hkdm.commands:SEs.run_from_console',
            'hkdm-get=hkdm.commands:get.run_from_console',
            'hkdm-put=hkdm.commands:put.run_from_console',
            'hkdm-check=hkdm.commands:check.run_from_console',
            'hkdm-fix=hkdm.commands:fix.run_from_console',
            'hkdm-html-index=hkdm.commands:html_index.run_from_console',
            'hkdm-move=hkdm.commands:move.run_from_console',
            'hkdm-rename=hkdm.commands:rename.run_from_console',
            'hkdm-cli=hkdm.cli:run_cli',
            'hkdm-tests=hkdm.tests:run_tests',
            'hkdm-config=hkdm.configuration:run_configuration_wizard',
            'hkdm-maid=hkdm.maid:run_maid',
        ],
    },
    zip_safe=True)
