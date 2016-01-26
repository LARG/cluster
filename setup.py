import os
import setuptools

setuptools.setup(
    name='cluster',
    version='0.1.5',
    packages=setuptools.find_packages(),
    install_requires=['psutil'],
    scripts=['scripts/cluster', 'scripts/cluster-ls', 'scripts/cluster-rm'],
    author='Matthew Hausknecht',
    author_email='matthew.hausknecht@gmail.com',
    description='Transparent cluster execution.',
    license='MIT',
    keywords=('condor '
              'slurm '
              ),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        ],
    )
