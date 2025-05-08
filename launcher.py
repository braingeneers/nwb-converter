import subprocess
import json
import os
import sys
import boto3

from datetime import datetime
from uuid import uuid4
from textwrap import dedent

this_dir = os.path.dirname(__file__)
output_dir = os.path.join(this_dir, 'output')

s3_client = boto3.client('s3', endpoint_url='https://s3-west.nrp-nautilus.io')
braingeneers_bucket = 'braingeneers'


yaml_format = '''
apiVersion: batch/v1
kind: Job
metadata:
  name: {jobname}
spec:
  template:
    spec:
      containers:
        - name: {jobname}
          image: quay.io/ucsc_cgl/nwb:12.4.1-cudnn-runtime-ubuntu22.04
          imagePullPolicy: Always
          command: ["sh", "-c"]
          args:
            - >
              python3 /tmp/run.py {s3_uri}
          volumeMounts:
            - name: prp-s3-credentials
              mountPath: "/root/.aws/credentials"
              subPath: "credentials"
            - name: prp-s3-credentials
              mountPath: "/root/.aws/.s3cfg"
              subPath: ".s3cfg"
            - name: kube-config
              mountPath: "/root/.kube"
          resources:
            requests:
              cpu: 8
              memory: 32Gi
              ephemeral-storage: "80Gi"
            limits:
              cpu: 24  # Throttle the container if using more CPU
              memory: 64Gi  # Terminate the container if using more memory
              ephemeral-storage: "2000Gi"
      volumes:
        - name: prp-s3-credentials
          secret:
            secretName: prp-s3-credentials
        - name: kube-config
          secret:
            secretName: kube-config
      restartPolicy: Never
  backoffLimit: 0  # k8 will reissue this Job this number of times if it fails (even if you kill it manually)
'''


def s3_uri_exists(s3_uri):
    bucket, key = s3_uri[len('s3://'):].split('/', maxsplit=1)
    try:
        print(f'Checking: {s3_uri}')
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


def s3_destination(src: str):
    """From an input s3 URI, determine the s3 destination URI where kilosort outputs should go."""
    if not src.startswith(f's3://{braingeneers_bucket}/ephys/'):
        print(f'URI is unexpected and non-canonical ({src})!  Skipping upload to s3.', file=sys.stderr)

    uuid = src[len(f's3://{braingeneers_bucket}/ephys/'):].split('/', maxsplit=1)[0]
    return f's3://{braingeneers_bucket}/ephys/{uuid}/derived/nwb/{os.path.basename(src)}.nwb'


i = 0
uris_processing = {}
with open('/home/quokka/git/kilosort4/dandi_s3_uris.txt', 'r') as r:
    for s3_uri in r.readlines():
        s3_uri = s3_uri.strip()
        s3_nwb = s3_destination(s3_uri)
        if not s3_uri_exists(s3_nwb):
            i += 1
            print(s3_uri)
            jobname = 'dandi-nwb-' + str(uuid4()).replace('-', '') + f'-{str(i)}'
            uris_processing[jobname] = s3_uri
            with open('tmp.yml', 'w') as w:
                w.write(dedent(yaml_format.format(jobname=jobname, s3_uri=s3_uri.strip()))[1:])
            subprocess.check_call(['kubectl', 'apply', '-f', 'tmp.yml'])
        else:
            print(f'Skipping (nwb file already found): {s3_uri}')

with open(f'running-{datetime.now()}.json', 'w') as w:
    w.write(str(uris_processing))

print(json.dumps(uris_processing, indent=4))
