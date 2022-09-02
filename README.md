# PyCHFS

PyCHFS is the Python bindings for CHFS (consistent hashing file system).

# Requirements

* [CHFS](https://github.com/otatebe/chfs)

# Getting Started

## Installation

```
$ pip install pychfs
```

## Create file system

You can create CHFS by `chfsctl` and set `CHFS_SERVER` environmental variable.

```
$ eval `chfsctl start`
$ chlist # show started servers
```

# Developing

## Building locally

```
$ python setup.py build_ext --inplace
```

## VSCode devcontainer

You can use VSCode devcontainer to develop fsspec-chfs.
The setup steps are follows:

1. Install Docker and Remote-Container extension.
2. Press Ctrl+Shift+P in VSCode.
3. select `Remote-Container: Open the folder in the Container`

## Testing

```
$ eval `chfsctl start` # start the server and set CHFS_SERVER
$ tox
```
