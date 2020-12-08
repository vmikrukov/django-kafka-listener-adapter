from setuptools import setup, find_packages

setup(
    name='django-kafka-listener-adapter',
    version='0.3.1',
    description='A Django app to listen kafka topics.',
    author='Vladimir Mikryukov',
    author_email='motobiker2008@gmail.com',
    license='Apache v.2',
    classifiers=[
        'Framework :: Django'
        'Intended Audience :: Developers'
        'License :: OSI Approved :: Apache License'
        'Operating System :: OS Independent'
        'Programming Language :: Python :: 3'
        'Programming Language :: Python :: 3.6'
    ],
    packages=find_packages(),
    include_package_data=True,
    url='https://github.com/vmikrukov/django-kafka-listener-adapter',
    install_requires=['kafka-python']

)
