
from setuptools import setup, find_packages

DEPENDENCIES = [
    "anthill-common"
]

setup(
    name='anthill-social',
    package_data={
      "anthill.social": ["anthill/social/sql", "anthill/social/static"]
    },
    setup_requires=["pypigit-version"],
    git_version="0.1.0",
    description='Social service for Anthill platform',
    author='desertkun',
    license='MIT',
    author_email='desertkun@gmail.com',
    url='https://github.com/anthill-platform/anthill-social',
    namespace_packages=["anthill"],
    include_package_data=True,
    packages=find_packages(),
    zip_safe=False,
    install_requires=DEPENDENCIES
)
