from setuptools import setup, find_packages

setup(
    name='process-gambling',
    version='1.0',
    description='Stat project to generate a process for gambling',
    author='Scott P. White',
    author_email='spwhite1337@gmail.com',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'scikit-learn',
        'requests',
        'plotly',
        'tqdm',
        'boto3'
    ],
)
