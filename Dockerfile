FROM continuumio/miniconda

MAINTAINER ben.koziol@gmail.com

RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get clean

RUN conda update -y --all

RUN conda install -y -c nesii/channel/dev-ocgis -c conda-forge ocgis esmpy nose rtree icclim cf_units mpi4py
RUN conda remove -y ocgis
RUN git clone -b v-2.0.0.dev1 --depth=1 https://github.com/NCPP/ocgis.git
RUN cd ocgis && python setup.py install

ENV GDAL_DATA /opt/conda/share/gdal
RUN cd && python -c "from ocgis.test import run_simple; run_simple(verbose=False)"

RUN rm -r /opt/conda/pkgs/*
RUN rm -r /ocgis