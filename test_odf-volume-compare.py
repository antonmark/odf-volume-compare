import unittest
from unittest.mock import patch
from io import StringIO
import sys
import subprocess

from odf_volume_compare import *

class TestODFVolumeCompare(unittest.TestCase):

    def test_exec_subvol_info_(self):
        cmd = ['tools_pod', 'cephfs_vol', 'subvol_name']
        expected_output = {'subvol_name': 'subvol_info'}
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout.strip.return_value = 'subvol_info'
            output = exec_subvol_info_(cmd)
            self.assertEqual(output, expected_output)

    def test_exec_subvol_shapshot_ls_(self):
        cmd = ['tools_pod', 'cephfs_vol', 'subvol_name']
        expected_output = {'subvol_name': 'subvol_info'}
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout.strip.return_value = 'subvol_info'
            output = exec_subvol_shapshot_ls_(cmd)
            self.assertEqual(output, expected_output)

    def test_exec_subvol_shapshot_info_(self):
        cmd = ['tools_pod', 'cephfs_vol', 'subvol_name', 'snapshot_name']
        expected_output = {'subvol_name': 'subvol_info'}
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout.strip.return_value = 'subvol_info'
            output = exec_subvol_shapshot_info_(cmd)
            self.assertEqual(output, expected_output)

    def test_find_tools_pod_name(self):
        expected_output = 'tools_pod_name'
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout = 'tools_pod_name\n'
            output = find_tools_pod_name()
            self.assertEqual(output, expected_output)

    def test_gather_cephfs_pv_info(self):
        expected_output = {
            'pv1': {
                'meta_data_name': 'subvol1',
                'pv_capacity': '10Gi',
                'creation_time': '2022-01-01T00:00:00Z',
                'fs_name': 'cephfs1'
            },
            'pv2': {
                'meta_data_name': 'subvol2',
                'pv_capacity': '20Gi',
                'creation_time': '2022-01-02T00:00:00Z',
                'fs_name': 'cephfs2'
            }
        }
        pv_json = '''
        {
            "items": [
                {
                    "metadata": {
                        "name": "pv1",
                        "annotations": {
                            "pv.kubernetes.io/provisioned-by": "openshift-storage.cephfs.csi.ceph.com"
                        },
                        "creationTimestamp": "2022-01-01T00:00:00Z"
                    },
                    "spec": {
                        "csi": {
                            "volumeAttributes": {
                                "subvolumeName": "subvol1",
                                "fsName": "cephfs1"
                            }
                        },
                        "capacity": {
                            "storage": "10Gi"
                        }
                    }
                },
                {
                    "metadata": {
                        "name": "pv2",
                        "annotations": {
                            "pv.kubernetes.io/provisioned-by": "openshift-storage.cephfs.csi.ceph.com"
                        },
                        "creationTimestamp": "2022-01-02T00:00:00Z"
                    },
                    "spec": {
                        "csi": {
                            "volumeAttributes": {
                                "subvolumeName": "subvol2",
                                "fsName": "cephfs2"
                            }
                        },
                        "capacity": {
                            "storage": "20Gi"
                        }
                    }
                }
            ]
        }
        '''
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout = pv_json
            output = gather_cephfs_pv_info()
            self.assertEqual(output, expected_output)

    # Add more test cases for other functions...

if __name__ == '__main__':
    unittest.main()