from setuptools import setup, find_packages

with open('LICENSE') as f:
    license = f.read()

setup(
    name='bgp_extrapolator',
    packages=find_packages(),
    version='0.1.0',
    author='Michael Pappas',
    author_email='thatmikepappas@gmail.com',
    url='https://github.com/mike-a-p/bgp_extrapolator',
    license=license,
    install_requires=[
        'pybgpstream',
        'setuptools',
        'psycopg2',
        'lib-bgp-data',
    ],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3'],
    entry_points={
        'console_scripts': [
            'bgp_extrapolate = bgp_extrapolator.__main__:main'
        ]},
)
