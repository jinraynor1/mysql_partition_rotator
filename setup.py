from distutils.core import setup

setup(
    name='mysql_partition_rotater',
    packages=['mysql_partition_rotater'],
    version='0.1',
    license='MIT',
    description='Rotates mysql tables by using partition method',
    author='Jimmy Atauje',
    author_email='jimmy.atauje@gmail.com',
    url='https://github.com/jinraynor1/mysql_partition_rotator',
    download_url='https://github.com/jinraynor1/mysql_partition_rotator/releases/tag/v0.1-alpha',
    keywords=['mysql', 'partition', 'rotate', 'python'],
    install_requires=[
        'pymysql',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
