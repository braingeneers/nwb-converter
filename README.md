# NWB Conversion Workflow

Intended to be run on the [National Research Platform (NRP)](https://nrp.ai/documentation/).  Given a [Maxwell](https://www.mxwbio.com/products/maxone-mea-system-microelectrode-array/) hdf5 filepath in s3, this can launch a Job on a Pod, download the Maxwell hdf5 file, convert it to [NWB](https://www.nwb.org/nwb-neurophysiology/) format (and update any metadata that [DANDI](https://dandiarchive.org/) may require), and upload the data back to s3.

### Running on the NRP

To launch this container as a basic Job, modify the kubernetes yaml file with your Maxwell hdf5 filepath in s3, and run the example kubernetes yaml file with:

    kubectl apply -f run.yaml

For example, given a Maxwell hdf5 s3 input path at:

	s3://braingeneers/ephys/{uuid}/original/data/{filename}.raw.hdf5
 
 this will deposit a converted NWB file with updated metadata at:
 
 	s3://braingeneers/ephys/{uuid}/derived/nwb/{filename}.raw.hdf5.nwb

### Building and Publishing the Container

To build the nwb-converter container, login to quay.io to host the docker image in quay:

	docker login quay.io

To build the image:

	docker build . -t quay.io/ucsc_cgl/nwb:12.4.1cudnn-runtime-ubuntu22.04

To push the image:

	docker push quay.io/ucsc_cgl/nwb:12.4.1cudnn-runtime-ubuntu22.04
