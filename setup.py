import os
import re
from setuptools import setup, find_packages

pwd = os.path.dirname(__file__)

with open(os.path.join(pwd, 'src', 'marshmallow_sa_core', '__init__.py')) as f:
  VERSION = (
      re.compile(r""".*__version__ = ["'](.*?)['"]""", re.S)
      .match(f.read())
      .group(1)
  )


def parse_requirements_file(filename):
    with open(filename) as fid:
        requires = [l.strip() for l in fid.readlines() if not l.startswith("#")]

    return requires


# base requirements
install_requires = parse_requirements_file('requirements.txt')
test_requires = parse_requirements_file('test-requirements.txt')

extras = {
    "test": test_requires,
}

extras["all"] = sum(extras.values(), [])


setup(
    name='marshmallow_sa_core',
    version=VERSION,
    install_requires=install_requires,
    extras_require=extras,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    description='SQLAlchemy-core integration with marshmallow',
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    include_package_data=True,
    zip_safe=False,
    python_requires='>=3.8',
    url='https://github.com/featureoverload/marshmallow-sa-core',
    license="MIT License",
    author='Feature Overload',
    author_email="featureoverload@gmail.com",
)
