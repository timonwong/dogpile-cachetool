import re
import setuptools


def get_version():
    content = open('dogpile_cachetool/__version__.py').read()
    if isinstance(content, bytes):
        content = content.decode('utf-8')
    return re.search(r"""^__version__ = (['"])([^'"]+)\1""", content).group(2)


def get_long_description():
    with open('README.rst') as f:
        return f.read()


setuptools.setup(
    name='dogpile-cachetool',
    version=get_version(),
    author='Timon Wong',
    author_email='timon86.wang@gmail.com',
    description='Additions to dogpile.cache',
    long_description=get_long_description(),
    license='Apache License 2.0',
    url='https://github.com/timonwong/dogpile-cachetool',
    packages=setuptools.find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    install_requires=[
        'six',
        'dogpile.cache==0.6.1',
    ],
    extras_require={
        'rc': ['rc>=0.3.1'],
    },
    tests_require=[
        'pytest',
    ]
)
