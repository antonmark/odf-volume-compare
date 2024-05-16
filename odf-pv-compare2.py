#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from pprint import pprint

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--live', action="store_true",
                        help='Run against a live internal mode ODF cluster. Requires OpenShift CLI and active login to target cluster and ODF tools pod.')
    parser.add_argument('-mg', '--must-gather',
                        help='Run against an ODF must-gather data collection. WARNING: Can return false positives unless CSI provisioner pods are scaled down during collection.')
    return parser.parse_args()

def run_subprocess(cmd):
    try:
        result = subprocess.run(cmd, text=True, capture_output=True, check=True).stdout.strip()
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"Subprocess error: {e}")
        return None

def exec_subvol_info(cmd):
    return exec_ceph_command(cmd, 'ceph fs subvolume info')

def exec_subvol_snapshot_ls(cmd):
    return exec_ceph_command(cmd, 'ceph fs subvolume snapshot ls')

def exec_subvol_snapshot_info(cmd):
    return exec_ceph_command(cmd, 'ceph fs subvolume snapshot info')

def exec_ceph_command(cmd, ceph_command):
    vol_proc = {}
    result = run_subprocess(['oc', 'exec', cmd[0], '--', *ceph_command.split(), cmd[1], cmd[2], '--group-name', 'csi'])
    if result:
        vol_proc[cmd[2]] = result
        sys.stdout.write(".")
        sys.stdout.flush()
    return vol_proc

def find_tools_pod_name():
    tools_pod_name_out = run_subprocess(['oc', 'get', 'pods', '-l', 'app=rook-ceph-tools', '-o', 'name'])
    return tools_pod_name_out[4:].strip() if tools_pod_name_out else None

def gather_cephfs_pv_info():
    pv_json = run_subprocess(['oc', 'get', 'pv', '-o', 'json'])
    pv_output_json = json.loads(pv_json) if pv_json else {}
    
    cephfs_pv_info = {}
    for item in pv_output_json.get("items", []):
        if item["metadata"]["annotations"].get("pv.kubernetes.io/provisioned-by") == 'openshift-storage.cephfs.csi.ceph.com':
            cephfs_pv_info[item["metadata"]["name"]] = {
                "meta_data_name": item["spec"]["csi"]["volumeAttributes"]["subvolumeName"],
                "pv_capacity": item["spec"]["capacity"]["storage"],
                "creation_time": item["metadata"]["creationTimestamp"],
                "fs_name": item["spec"]["csi"]["volumeAttributes"]["fsName"]
            }
    return cephfs_pv_info

def gather_cephfs_subvol_list(tools_pod_name):
    cephfs_subvols_ls = {}
    vols = []

    cephfs_vol_ls_json = run_subprocess(['oc', 'exec', tools_pod_name, '--', 'ceph', 'fs', 'volume', 'ls'])
    cephfs_vol_ls_json_out = json.loads(cephfs_vol_ls_json) if cephfs_vol_ls_json else []

    for vol in cephfs_vol_ls_json_out:
        vols.append(vol["name"])
        cephfs_subvols_ls[vol["name"]] = []

    for vol in vols:
        cephfs_subvol_ls_json = run_subprocess(['oc', 'exec', tools_pod_name, '--', 'ceph', 'fs', 'subvolume', 'ls', vol, '--group-name', 'csi'])
        cephfs_subvol_ls_json_out = json.loads(cephfs_subvol_ls_json) if cephfs_subvol_ls_json else []

        for subvol in cephfs_subvol_ls_json_out:
            cephfs_subvols_ls[vol].append(subvol["name"])

    return cephfs_subvols_ls

def gather_cephfs_subvol_info(tools_pod_name, gathered_cephfs_ceph_info):
    sys.stdout.write("[*] Collecting subvol data from Ceph.")
    sys.stdout.flush()

    vols = []
    for fs_name, subvols in gathered_cephfs_ceph_info.items():
        for subvol in subvols:
            vols.append([tools_pod_name, fs_name, subvol])

    with ThreadPoolExecutor(max_workers=8) as executor:
        gathered_subvol_info = list(executor.map(exec_subvol_info, vols))

    subvol_info_out = {}
    for info in gathered_subvol_info:
        for key, value in info.items():
            json_out = json.loads(value)
            subvol_info_out[key] = {
                "created_at": json_out["created_at"],
                "bytes_quota": json_out["bytes_quota"],
                "bytes_used": json_out["bytes_used"],
                "type": json_out["type"]
            }
    return subvol_info_out

def gather_cephfs_ocp_vsc_info():
    vsc_json = run_subprocess(['oc', 'get', 'vsc', '-o', 'json'])
    vsc_output_json = json.loads(vsc_json) if vsc_json else {}

    cephfs_vsc_info_by_ocp_name = {}
    for item in vsc_output_json.get("items", []):
        if item["spec"]["volumeSnapshotClassName"] == 'ocs-storagecluster-cephfsplugin-snapclass' and item["status"]["readyToUse"]:
            cephfs_vsc_info_by_ocp_name[item["metadata"]["name"]] = {
                "source_volume_handle": item["spec"]["source"]["volumeHandle"],
                "creation_time": item["metadata"]["creationTimestamp"],
                "vsc_ready_status": item["status"]["readyToUse"],
                "snapshot_handle": item["status"]["snapshotHandle"]
            }
    return cephfs_vsc_info_by_ocp_name

def check_ceph_health(tools_pod_name):
    return run_subprocess(['oc', 'exec', tools_pod_name, '--', 'ceph', 'health'])

def find_stale_volumes(gathered_ceph_subvol_info, gathered_cephfs_pv_info):
    vols_ceph = set(gathered_ceph_subvol_info.keys())
    vols_ocp = {data['meta_data_name'] for data in gathered_cephfs_pv_info.values()}

    diff_ocp = vols_ocp - vols_ceph
    diff_ceph = vols_ceph - vols_ocp

    if diff_ocp:
        logging.info(f'{len(diff_ocp)} stale volumes in OpenShift without matching CephFS subvol: {", ".join(diff_ocp)}')
    if diff_ceph:
        logging.info(f'{len(diff_ceph)} stale CephFS subvols without matching OpenShift volumes: {", ".join(diff_ceph)}')

    return diff_ocp, diff_ceph

def main():
    args = parse_arguments()

    if args.live:
        logging.info('Running against a live internal mode ODF cluster.')

        if not run_subprocess(['oc', 'whoami']):
            logging.error('Failed to run oc whoami.')
            sys.exit(1)

        tools_pod_name = find_tools_pod_name()
        if not tools_pod_name:
            logging.error('ODF tools pod is not running. Please enable the tools pod and try again.')
            sys.exit(1)

        logging.info(f'Ceph health: {check_ceph_health(tools_pod_name)}')

        gathered_cephfs_pv_info = gather_cephfs_pv_info()
        if not gathered_cephfs_pv_info:
            logging.info('No CephFS volumes found in OpenShift.')
            return

        cephfs_subvol_list = gather_cephfs_subvol_list(tools_pod_name)
        gathered_ceph_subvol_info = gather_cephfs_subvol_info(tools_pod_name, cephfs_subvol_list)

        logging.info(f'Comparing data for possible stale volumes.')
        find_stale_volumes(gathered_ceph_subvol_info, gathered_cephfs_pv_info)

if __name__ == "__main__":
    main()
