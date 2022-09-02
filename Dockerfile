FROM koyamaso/chfs:master

USER root
RUN apt update -y
RUN apt upgrade -y
RUN apt install -y python3-pip
USER chfs

RUN echo 'spack env activate -d /home/chfs/.spack-extension/envs/chfs' >> ~/.bashrc
RUN echo 'alias python="python3"' >> ~/.bashrc
RUN echo 'alias pip="pip3"' >> ~/.bashrc
RUN pip3 install tox pytest fsspec cython pkgconfig numpy pytest-dependency dask[complete] click==8.0.2
