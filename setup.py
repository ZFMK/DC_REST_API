import os

from setuptools import setup, find_packages


requires = [
    'pyramid',
    'pyramid_tm',
    'pyramid_chameleon',
    'pyramid_debugtoolbar',
    'pyramid_beaker',
    'pyramid-mako',
    'waitress',
    'pymysql',
    'pudb',
    'pyodbc',
    'configparser',
    'cryptography',
    'bcrypt'
    ]

setup(name='dc_rest_api',
      version='0.1',
      description='import data into DWB',
      classifiers=[
        "Programming Language :: Python",
        ],
      author='Björn Quast',
      author_email='b.quast@leibniz-lib.de',
      url='',
      keywords='DiversityWorkbench DiversityCollection CRUD API',
      packages=find_packages(),
      #packages=['DBConnectors'],
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      entry_points="""\
      [paste.app_factory]
      main = dc_rest_api:main
      """,
      )
