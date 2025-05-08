FROM quay.io/ucsc_cgl/nrp:12.4.1cudnn-runtime-ubuntu22.04

RUN pip install neuroconv[maxwell,dandi] pynwb

COPY run.py /tmp/run.py
