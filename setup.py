from setuptools import setup, find_packages

setup(
    name="swisspollentools",
    version="1.0",
    author="Axel Giottonini",
    author_email="giottonini.axel@gmail.com",
    packages=find_packages(include=['swisspollentools', 'swisspollentools.*']),
    install_requires=[
        'numpy',
        'pandas',
        'h5py',
        'tensorflow',
        'pyzmq',
        'sklearn'
    ]
)