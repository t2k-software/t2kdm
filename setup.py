from setuptools import setup

# Todo: Parse this from a proper readme file in the future
description='T2K Data Manager'
long_description = """t2kdm - T2K Data Manager

Provides functions for a smoother handling of grid data within the T2K
experiment.

"""

setup(name='t2kdm',
    version='0.0.1',
    description=description,
    long_description=long_description,
    url='http://github.com/ast0815/t2kdm',
    author='Lukas Koch',
    author_email='lukas.koch@mailbox.org',
    license='MIT',
    packages=['t2kdm'],
    install_requires=['sh>=1.12.14', 'six>=1.10.0'],
    extras_require = {
    },
    python_requires='>=2.6',
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

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
    scripts=[
        'bin/t2kdm-ls',
    ],
    zip_safe=True)