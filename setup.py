from Cython.Distutils import build_ext
from Cython.Build import cythonize
from setuptools import setup
from setuptools.extension import Extension
import pkgconfig


def chfs_extension():
    libs = ['chfs']
    for lib in libs:
        if not pkgconfig.exists(lib):
            raise FileNotFoundError('pkgconfig did not find ' + lib)
    ext = Extension('pychfs.chfs', sources=['pychfs/*.pyx'])
    pkgconfig.configure_extension(ext, 'chfs')
    return ext


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='pychfs',
    version='0.0.1',
    author="Kazuki Obata, Sohei Koyama",
    author_email="obata@hpcs.cs.tsukuba.ac.jp, skoyama@hpcs.cs.tsukuba.ac.jp",
    description="CHFS Python bindings",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/donkomura/PyCHFS',
    packages=["pychfs"],
    entry_points={
        'fsspec.specs': [
            'chfs=chfs.CHFSClient',
            'chfs_stub=chfs.CHFSStubClient'
        ]
    },
    cmdclass={'build_ext': build_ext},
    ext_modules=cythonize([chfs_extension()])
)
