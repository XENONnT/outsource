from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='outsource',
      version='0.1',
      description='XENONnT Outsource module',
      long_description=readme(),
      url='https://github.com/XENONnT/outsource',
      packages=['outsource'],
      install_requires=['markdown',
                        'utilix'
                        ],
      include_package_data=True,
      zip_safe=False)
