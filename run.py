#!/usr/bin/env python
import os
import sys
import zipfile
import boto3

from datetime import datetime

from dateutil import tz
from neuroconv.datainterfaces import MaxOneRecordingInterface
from pynwb import NWBHDF5IO
from pynwb.file import Subject


this_dir = os.path.dirname(__file__)
output_dir = os.path.join(this_dir, 'output')

s3_client = boto3.client('s3', endpoint_url='https://s3-west.nrp-nautilus.io')
braingeneers_bucket = 'braingeneers'


def upload_local_to_s3(src: str, dst: str) -> None:
    """Given a local filepath, this will upload it to an s3 URI."""
    if not dst.startswith(f's3://{braingeneers_bucket}/ephys/'):
        print(f'URI is unexpected and non-canonical ({dst})!  Skipping upload to s3.', file=sys.stderr)

    print(f'Uploading {src} to {dst} with boto3 version: {boto3.__version__}')
    s3_client.upload_file(src, braingeneers_bucket, dst[len(f's3://{braingeneers_bucket}/'):])


def download_s3_to_local(src: str, dst: str) -> None:
    """Given an s3 URI, this will download it locally."""
    if not src.startswith('s3://'):
        raise RuntimeError(f'Input filepath must start with s3:// !  {src}')

    print(f'Downloading {src} to {dst} with boto3 version: {boto3.__version__}')
    bucket, key = src[len('s3://'):].split('/', maxsplit=1)
    s3_client.download_file(bucket, key, dst)


def s3_destination(src: str, filename: str):
    """From an input s3 URI, determine the s3 destination URI where kilosort outputs should go."""
    if not src.startswith(f's3://{braingeneers_bucket}/ephys/'):
        print(f'URI is unexpected and non-canonical ({src})!  Skipping upload to s3.', file=sys.stderr)

    uuid = src[len(f's3://{braingeneers_bucket}/ephys/'):].split('/', maxsplit=1)[0]
    return f's3://{braingeneers_bucket}/ephys/{uuid}/derived/nwb/{filename}'


metadata = {'electrode_group_description': 'V1 Maxwell Electrode Group',
            'electrodes_channel_name_description': 'Name (number) of the electrode channel',
            'electrode_name_description': 'Name (number) of the electrode',
            'subject': Subject(
                subject_id="001",
                age="P90D",
                description="mouse",
                species="Mus musculus",
                sex="U"
            ),
            'institution': 'UCSC',
            'experimenter': 'Tal Sharf',
            'keywords': ['ephys', 'mouse', 'organoid'],
            'experiment_description': 'Hmmmm'}


def update_electrode_group_desc(nwbfile, desc):
    #  check_description - 'ElectrodeGroup' object at location '/general/extracellular_ephys/0'
    #       Message: Description ('no description') is a placeholder.
    if nwbfile.electrode_groups['0'].description.strip() == 'no description':
        del nwbfile.electrode_groups['0'].fields['description']
        setattr(nwbfile.electrode_groups['0'], 'description', desc)


def update_electrodes_channel_name(nwbfile, desc):
    #  check_description - 'VectorData' object with name 'channel_name'
    #        Message: Description ('no description') is a placeholder.
    if nwbfile.electrodes['channel_name'].fields['description'].strip() == 'no description':
        del nwbfile.electrodes['channel_name'].fields['description']
        setattr(nwbfile.electrodes['channel_name'], 'description', desc)


def update_electrode_name(nwbfile, desc):
    #  check_description - 'VectorData' object with name 'electrode'
    #        Message: Description ('no description') is a placeholder.
    if nwbfile.electrodes['electrode'].fields['description'].strip() == 'no description':
        del nwbfile.electrodes['electrode'].fields['description']
        setattr(nwbfile.electrodes['electrode'], 'description', desc)


def update_subject(nwbfile, subject):
    # check_subject_exists - 'NWBFile' object at location '/'
    #        Message: Subject is missing.
    if 'subject' in nwbfile.fields:
        del nwbfile.fields['subject']
    nwbfile.subject = subject


def update_institution(nwbfile, institute):
    # check_institution - 'NWBFile' object at location '/'
    #        Message: Metadata /general/institution is missing.
    if 'institution' in nwbfile.fields:
        del nwbfile.fields['institution']
    nwbfile.institution = institute


def update_experimenter(nwbfile, experimenter):
    # check_experimenter_exists - 'NWBFile' object at location '/'
    #        Message: Experimenter is missing.
    if 'experimenter' in nwbfile.fields:
        del nwbfile.fields['experimenter']
    nwbfile.experimenter = experimenter


def update_keywords(nwbfile, keywords):
    # check_keywords - 'NWBFile' object at location '/'
    #        Message: Metadata /general/keywords is missing.
    if 'keywords' in nwbfile.fields:
        del nwbfile.fields['keywords']
    nwbfile.keywords = keywords


def update_experiment_description(nwbfile, experiment_description):
    # check_experiment_description - 'NWBFile' object at location '/'
    #        Message: Experiment description is missing.
    if 'experiment_description' in nwbfile.fields:
        del nwbfile.fields['experiment_description']
    nwbfile.experiment_description = experiment_description


def update_metadata(input_filename: str, output_filename: str):
    with NWBHDF5IO(input_filename, mode='r') as read_io:
        nwbfile = read_io.read()
        update_electrode_group_desc(nwbfile, desc=metadata['electrode_group_description'])
        update_electrodes_channel_name(nwbfile, desc=metadata['electrodes_channel_name_description'])
        update_electrode_name(nwbfile, desc=metadata['electrode_name_description'])
        update_subject(nwbfile, subject=metadata['subject'])
        update_institution(nwbfile, institute=metadata['institution'])
        update_experimenter(nwbfile, experimenter=metadata['experimenter'])
        update_keywords(nwbfile, keywords=metadata['keywords'])
        update_experiment_description(nwbfile, experiment_description=metadata['experiment_description'])
        nwbfile.set_modified()
        nwbfile.generate_new_id()

        with NWBHDF5IO(output_filename, mode='w') as export_io:
            export_io.export(src_io=read_io, nwbfile=nwbfile, write_args={'link_data': False})


def convert_maxwell_to_nwb(src_local_path: str, dst_local_path: str, dry_run: bool = False) -> str:
    """Neuroconv only works locally, so this only accepts local file paths."""
    if not os.path.exists(src_local_path):
        raise FileNotFoundError(f'Local path does not exist: {src_local_path}')

    interface = MaxOneRecordingInterface(file_path=src_local_path, verbose=True)
    metadata = interface.get_metadata()
    metadata["NWBFile"].update(session_start_time=datetime.now(tz=tz.gettz("US/Pacific")))
    interface.run_conversion(nwbfile_path=dst_local_path, metadata=metadata)

    print(f'Successfully converted {src_local_path} to {dst_local_path}')
    return dst_local_path

def main():
    input_uri: str = sys.argv[1]
    local_filepath: str = f'/tmp/{os.path.basename(input_uri)}'
    output = f'{local_filepath}.nwb'

    download_s3_to_local(src=input_uri, dst=local_filepath)
    convert_maxwell_to_nwb(local_filepath, f'/tmp/temp.nwb')
    try:
        update_metadata(f'/tmp/temp.nwb', output)
        upload_local_to_s3(src=output, dst=s3_destination(input_uri, os.path.basename(output)))
    except:
        upload_local_to_s3(src=f'/tmp/temp.nwb', dst=s3_destination(input_uri, os.path.basename(output)))
    print(f'Completed: {input_uri}')


if __name__ == '__main__':
    main()
