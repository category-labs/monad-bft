from setuptools import find_packages, setup

setup(
    name='monad_flexnet',
    packages=find_packages(),
    version='0.0.0',
    description='Library for configuring Monad Flexnet instances',
    install_requires=[
        'eth_account',
        'gitpython',
        'natsort',
        'py-evm',
        'python-on-whales',
        'requests',
        'rlp',
        'urllib3',
        'web3'
    ]
)
