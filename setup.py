from setuptools import setup, find_packages

with open("README.md", "r",encoding='utf-8') as fh:
    long_description = fh.read()
    
setup(
    name='SwingedgeCore',
    version='0.1',
    author='Anurupa Karmakar',
    author_email='anurupakarmakar.dgp18@gmail.com',
    maintainer='Anurupa Karmakar',
    maintainer_email='anurupakarmakar.dgp18@gmail.com',
    packages=find_packages(), 
    package_data={'': ['Images_list/*']},
    include_package_data=True,
    description='Swignedge Core Package',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/dhirs/swingedge_core',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent"
    ],
)