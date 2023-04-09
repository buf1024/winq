from setuptools import find_packages, setup
import os
import shutil

dest_file = os.sep.join([os.path.expanduser('~'), '.config', 'winq'])

if not os.path.exists(dest_file):
    os.makedirs(dest_file)

dest_file = os.sep.join(['config.yml'])
conf_file = os.sep.join([os.getcwd(), 'winq', 'config', 'config.yml'])

shutil.copy(conf_file, dest_file)

setup(
    name='winq',
    version='0.0.1',
    packages=find_packages(include=['winq']),
    include_package_data=True,
    zip_safe=False,
    platform="any",
    install_requires=[
        'numpy',
        'pandas',
        'akshare==1.6.75',
        'hiq_pyfetch',
        'aiohttp',
        'motor',
        'pymongo',
        'xlrd',
        'aiosmtplib',
        'Click',
        'PyYAML',
        'pyecharts',
        'scipy',
        'sklearn',
        'TA-Lib',
        'nest-asyncio',
        'tqdm',
        'hisql',
        'requests',
        'protobuf',
        'ipython',
        'polars'
    ],
    entry_points={
        'console_scripts': [
            'winqwatch=winq.cmd.stock_watch:main',
            'winqselect=winq.cmd.stock_select:main',
            'winqtrader=winq.cmd.trader:main'
        ]
    },
)
