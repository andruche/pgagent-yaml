import sys

from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand

from pgagent_yaml import __version__

PACKAGE_TYPE = 'pg-tools'
PACKAGE_NAME = 'pgagent-yaml'
PACKAGE_DESC = 'Yaml interface for pgagent'
PACKAGE_VERSION = __version__


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to pytest")]

    def initialize_options(self):
        super().initialize_options()
        # default list of options for testing
        # https://docs.pytest.org/en/latest/logging.html
        self.pytest_args = (
            '--flake8 {0} tests examples '
            '--junitxml=.reports/{0}_junit.xml '
            '--cov={0} --cov=tests '
            '-p no:logging'.format(PACKAGE_NAME.replace('-', '_'))
        )

    def run_tests(self):
        import shlex
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(shlex.split(self.pytest_args))
        sys.exit(errno)


setup_requires = []

install_requires = [
    'pydantic',
    'pyyaml',
    'asyncpg>=0.27.0,<0.31.0',
]

tests_require = [
    'flake8>=5,<6',
    'pytest',
    'pytest-cov',
    'pytest-flake8',
    'pytest-asyncio',
    'pytest-sugar',
    'asynctest',
]

console_scripts = [
    'pgagent_yaml=pgagent_yaml.main:main'
]


def readme():
    with open('README.md', 'r') as f:
        return f.read()


setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description=PACKAGE_DESC,
    long_description=readme(),
    long_description_content_type='text/markdown',
    url='https://github.com/andruche/pgagent-yaml',
    project_urls={
        'Documentation': 'https://github.com/andruche/pgagent-yaml/blob/master/README.md',
        'Bug Tracker': 'https://github.com/andruche/pgagent-yaml/issues',
    },
    author="Andrey Chernyakov",
    license="BSD",
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ],
    zip_safe=False,
    packages=find_packages(exclude=['tests', 'examples', '.reports']),
    entry_points={'console_scripts': console_scripts},
    python_requires='>=3.9',
    setup_requires=setup_requires,
    install_requires=install_requires,
    tests_require=tests_require,
    cmdclass={'test': PyTest},
    keywords='pgagent,git,yaml'
)
