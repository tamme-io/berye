from setuptools import setup

setup(
    name='berye',
    version='0.1',
    description='A library for making the management of our MySQL schema easier for our lambda services',
    url='https://github.com/tamme-io/berye',
    author='tamme',
    author_email='opensource@tamme.io',
    license='MIT',
    packages=[
        'berye'
    ],
    install_requires=[
    ],
    zip_safe=False,
    setup_requires=[
        'pytest-runner',
    ],
    tests_require=[
        'pytest',
    ]
)
