from setuptools import setup, find_packages

setup(
    name="towstat",
    version="0.3",
    author="Brian Seel",
    author_email="brian.seel@baltimorecity.gov",
    description="Data processor for the IVIC database",
    packages=find_packages('src'),
    package_data={'towstat': ['py.typed'], },
    python_requires='>=3.0',
    package_dir={'': 'src'},
    install_requires=[
        'tqdm~=4.60.0',
        'pyodbc~=4.0.30',
        'loguru~=0.5.3',
    ]
)
