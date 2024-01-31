#!/usr/bin/env python3

# odf-volume-compare.py
# Tool to identify potential disagreements between OpenShift 
# and ODF storage cluster persistent volume state.

import argparse
import json
import os
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from pprint import pprint


parser = argparse.ArgumentParser()
parser.add_argument('-l', '--live', action="store_true",
                    help='Run aginast a live internal mode ODF cluster. Requires OpenShift cli and active login to target cluster and ODF tools pod.')
parser.add_argument('-mg', '--must-gather',
                    help='Run against an ODF must-gather data collection. WARNING: Can return false positives unless CSI provisioner pods are scaled down during collection.')
args = parser.parse_args()


# returns dict
def exec_subvol_info_(cmd):
    vol_proc = {}
    proc = subprocess.run(['oc', 'exec', cmd[0], '--', 'ceph', 'fs', 'subvolume', 'info', cmd[1], cmd[2], '--group-name', 'csi'], text=True, capture_output=True).stdout.strip()
    vol_proc.update({cmd[2]:proc})
    sys.stdout.write(f".")
    sys.stdout.flush()
    return(vol_proc)

def exec_subvol_shapshot_ls_(cmd):
    vol_proc = {}
    proc = subprocess.run(['oc', 'exec', cmd[0], '--', 'ceph', 'fs', 'subvolume', 'snapshot', 'ls', cmd[1], cmd[2], '--group-name', 'csi'], text=True, capture_output=True).stdout.strip()
    vol_proc.update({cmd[2]:proc})
    sys.stdout.write(f".")
    sys.stdout.flush()
    return(vol_proc)

def exec_subvol_shapshot_info_(cmd):
    vol_proc = {}
    proc = subprocess.run(['oc', 'exec', cmd[0], '--', 'ceph', 'fs', 'subvolume', 'snapshot', 'info', cmd[1], cmd[2], cmd[3], '--group-name', 'csi'], text=True, capture_output=True).stdout.strip()
    vol_proc.update({cmd[2]:proc})
    sys.stdout.write(f".")
    sys.stdout.flush()
    return(vol_proc)
    


def find_tools_pod_name():

    tools_pod_name_out = subprocess.run(['oc', 'get', 'pods', '-l', 'app=rook-ceph-tools', '-oname'],text=True, capture_output=True).stdout
    
    if not tools_pod_name_out:
        return False
    else:
        return tools_pod_name_out[4:].strip()



# returns volume size (Gi), PV name, volume handle, creation time, and total number of cephfs PV's currently provisioned.
def gather_cephfs_pv_info():

    pv_json = subprocess.run(['oc', 'get', 'pv', '-ojson'],text=True, capture_output=True).stdout
    pv_output_json = json.loads(pv_json)
    
    cephfs_pv_info = {}
    cephfs_pv_count = 0

    for X in pv_output_json["items"]:
        if X["metadata"]["annotations"]["pv.kubernetes.io/provisioned-by"] == 'openshift-storage.cephfs.csi.ceph.com':
            cephfs_pv_metadata_name = (X["metadata"]["name"])
            cephfs_rhcs_subvol_name = (X["spec"]["csi"]["volumeAttributes"]["subvolumeName"])
            cephfs_pv_capacity = (X["spec"]["capacity"]["storage"])
            cephfs_pv_creation_time = (X["metadata"]["creationTimestamp"])
            cephfs_pv_ceph_vol = (X["spec"]["csi"]["volumeAttributes"]["fsName"])

            cephfs_pv_info.update({cephfs_pv_metadata_name:{"meta_data_name":cephfs_rhcs_subvol_name, "pv_capacity":cephfs_pv_capacity, "creation_time":cephfs_pv_creation_time, "fs_name":cephfs_pv_ceph_vol}})
        
        #pprint(X["metadata"]["annotations"]["pv.kubernetes.io/provisioned-by"])
        #pprint(X["kind"])
        #print(json.dumps(X, indent=2))
    
    return(cephfs_pv_info)


# Pass tools pod name first and gathered_cephfs_pv_info dict second
def gather_cephfs_subvol_list(tools_pod_name):

    cephfs_subvols_ls = {}
    
    # Need to find the unique instances of cephfs volume names. This is because of external mode ODF. 
    vols = []
    
    cephfs_vol_ls_json = subprocess.run(['oc', 'exec', tools_pod_name, '--', 'ceph', 'fs', 'volume', 'ls' ],text=True, capture_output=True).stdout.strip()
    cephfs_vol_ls_json_out = json.loads(cephfs_vol_ls_json) 

    for X in cephfs_vol_ls_json_out:
        vols.append(X["name"])
        
    for X in vols:
        cephfs_subvols_ls[X] = []
    
        cephfs_subvol_ls_json = subprocess.run(['oc', 'exec', tools_pod_name, '--', 'ceph', 'fs', 'subvolume', 'ls', X, '--group-name', 'csi'],text=True, capture_output=True).stdout.strip()
        cephfs_subvol_ls_json_out = json.loads(cephfs_subvol_ls_json)
        
        for Y in cephfs_subvol_ls_json_out:
            cephfs_subvols_ls[X].append(Y["name"])

    return(cephfs_subvols_ls)


# Returns subvolume sizes and creation times as a dict. 
def gather_cephfs_subvol_info(tools_pod_name, gathered_cephfs_ceph_info):
    
    sys.stdout.write(f"[*] Collecting subvol data from Ceph.")
    sys.stdout.flush()

    vols = []
    gathered_subvol_info = []
    subvol_info_out = {}

    for X in gathered_cephfs_ceph_info:
        for Y in gathered_cephfs_ceph_info[X]:
            
            vols.append([tools_pod_name, X, Y])


    with ThreadPoolExecutor(max_workers=8) as executor:

        futures = executor.map(exec_subvol_info_, vols)

        for future in futures:
            #sys.stdout.write(f".")
            #sys.stdout.flush()
            gathered_subvol_info.append(future)

    #print(gathered_subvol_info)

    for X in gathered_subvol_info:
        for key, value in X.items():
            json_out = (json.loads(value))
            subvol_created_at = json_out["created_at"]
            subvol_bytes_quota = json_out["bytes_quota"]
            subvol_bytes_used = json_out["bytes_used"]
            subvol_type = json_out["type"]
            subvol_info_out.update({key:{"created_at":subvol_created_at,"bytes_quota":subvol_bytes_quota,"bytes_used":subvol_bytes_used,"type":subvol_type}})

    return(subvol_info_out)

def gather_cephfs_subvol_snapshot_list(tools_pod_name, gathered_cephfs_ceph_info):

    sys.stdout.write(f"[*] Collecting subvol snapshot list from Ceph.")
    sys.stdout.flush()

    vols = []
    gathered_subvol_snapshot_info = []
    subvol_snapshot_list_out = {}

    for X in gathered_cephfs_ceph_info:
        for Y in gathered_cephfs_ceph_info[X]:
            
            vols.append([tools_pod_name, X, Y])


    with ThreadPoolExecutor(max_workers=8) as executor:

        futures = executor.map(exec_subvol_shapshot_ls_, vols)

        for future in futures:
            #sys.stdout.write(f".")
            #sys.stdout.flush()
            gathered_subvol_snapshot_info.append(future)
            

    #print(gathered_subvol_info)

    for X in gathered_subvol_snapshot_info:
        for key, value in X.items():
            json_out = (json.loads(value))
            pprint(json_out)
            type(json_out)
            type({key:json_out})
            subvol_snapshot_list_out.update({key:json_out})
        
    return(subvol_snapshot_list_out)


# gathers all the volumesnapshotcontents info from ocp
def gather_cephfs_ocp_vsc_info(tools_pod_name):
    
    vsc_json = subprocess.run(['oc', 'get', 'vsc', '-ojson'],text=True, capture_output=True).stdout
    vsc_output_json = json.loads(vsc_json)
       
    cephfs_vsc_info_by_ocp_name = {}
    cephfs_vsc_info_by_ceph_name = {}
    cephfs_vsc_count = 0

    for X in vsc_output_json["items"]:
        if X["spec"]["volumeSnapshotClassName"] == 'ocs-storagecluster-cephfsplugin-snapclass':
            if X["status"]["readyToUse"]:
                cephfs_vsc_metadata_name = (X["metadata"]["name"])
                cephfs_vsc_creation_time = (X["metadata"]["creationTimestamp"])
                cephfs_vsc_volume_handle_source = (X["spec"]["source"]["volumeHandle"])
                cephfs_vsc_volume_snapshot_handle = (X["status"]["snapshotHandle"])
                cephfs_vsc_ready = (X["status"]["readyToUse"])                                   

                cephfs_vsc_info_by_ocp_name.update({cephfs_vsc_metadata_name:{"source_volume_handle":cephfs_vsc_volume_handle_source, "creation_time":cephfs_vsc_creation_time, "vsc_ready_status":cephfs_vsc_ready, "snapshot_handle":cephfs_vsc_volume_snapshot_handle}})
                cephfs_vsc_info_by_ceph_name.update({cephfs_vsc_volume_snapshot_handle:{"source_volume_handle":cephfs_vsc_volume_handle_source, "meta_data_name":cephfs_vsc_metadata_name, "creation_time":cephfs_vsc_creation_time, "vsc_ready_status":cephfs_vsc_ready}})

            else:

                cephfs_vsc_metadata_name = (X["metadata"]["name"])
                cephfs_vsc_creation_time = (X["metadata"]["creationTimestamp"])
                cephfs_vsc_volume_handle_source = (X["spec"]["source"]["volumeHandle"])
                cephfs_vsc_ready = (X["status"]["readyToUse"])
  
        #print(json.dumps(X, indent=2))
    return(cephfs_vsc_info_by_ocp_name, cephfs_vsc_info_by_ceph_name)


def gather_cephfs_snapshots_list(tools_pod_name, gathered_cephfs_ceph_info):

    sys.stdout.write(f"[*] Collecting subvol data from Ceph.")
    sys.stdout.flush()

    vols = []
    gathered_subvol_info = []
    subvol_info_out = {}

    for X in gathered_cephfs_ceph_info:
        for Y in gathered_cephfs_ceph_info[X]:
            
            vols.append([tools_pod_name, X, Y])


    with ThreadPoolExecutor(max_workers=8) as executor:

        futures = executor.map(exec_subvol_info_, vols)

        for future in futures:
            #sys.stdout.write(f".")
            #sys.stdout.flush()
            gathered_subvol_info.append(future)

    #print(gathered_subvol_info)

    for X in gathered_subvol_info:
        for key, value in X.items():
            json_out = (json.loads(value))
            subvol_created_at = json_out["created_at"]
            subvol_bytes_quota = json_out["bytes_quota"]
            subvol_bytes_used = json_out["bytes_used"]
            subvol_type = json_out["type"]
            subvol_info_out.update({key:{"created_at":subvol_created_at,"bytes_quota":subvol_bytes_quota,"bytes_used":subvol_bytes_used,"type":subvol_type}})

    return(subvol_info_out)



def total_cephfs_pv_capacity(gathered_cephfs_pv_info):

    total_size_gi = 0
    
    for X, Y in gathered_cephfs_pv_info.items():
        total_size_gi += int(Y['pv_capacity'][:-2])

    return(total_size_gi)


# Pass tools pod name
def check_ceph_health(tools_pod_name):
    
    ceph_health_output = subprocess.run(['oc', 'exec', tools_pod_name, '--', 'ceph', 'health'],text=True, capture_output=True).stdout.strip()
    return(ceph_health_output)


def find_stale_volumes(gathered_ceph_subvol_info, gathered_cephfs_pv_info):

    vols_ceph = []
    vols_ocp = []
    report = {}
           
    for pvc, data in gathered_cephfs_pv_info.items():
        vols_ocp.append(data['meta_data_name'])

    for subvol, data in gathered_ceph_subvol_info.items():
        vols_ceph.append(subvol)
    
    diff_ocp = set(vols_ocp) - set(vols_ceph)
    diff_ceph = set(vols_ceph) - set(vols_ocp)

    if diff_ocp and diff_ceph:
        print (f'[*] Both CephFS and OpenShift have inconsistencies.')
    if diff_ocp:
        print(f'[*] {len(diff_ocp)} stale volume{"" if len(diff_ocp) == 1 else "s"} exist{"s" if len(diff_ocp) == 1 else ""} in OpenShift that have no matching CephFS subvol.')
        print('[*] Volume(s): ', end="")
        print(*diff_ocp, sep=", ")
    if diff_ceph:
        print(f'[*] {len(diff_ceph)} stale subvol{"" if len(diff_ceph) == 1 else "s"} exist{"s" if len(diff_ceph) == 1 else ""} in CephFS that have no matching OpenShift volume.')
        print(f'[*] Volume(s): ', end="")
        print(*diff_ceph, sep=", ")
    
    report.update({'missing_in_ceph': diff_ocp,'missing_in_shift': diff_ceph})
    #print('[DEBUG] ', report)
    return (diff_ocp, diff_ceph)

def find_stale_snapshots(gathered_cephfs_ocp_vsc_info, gathered_cephfs_ceph_info):
    
        vsc_ceph = []
        vsc_ocp = []
        report = {}
            
        for vsc, data in gathered_cephfs_ocp_vsc_info.items():
            vsc_ocp.append(data['meta_data_name'])
    
        for subvol, data in gathered_cephfs_ceph_info.items():
            vsc_ceph.append(subvol)
        
        diff_ocp = set(vsc_ocp) - set(vsc_ceph)
        diff_ceph = set(vsc_ceph) - set(vsc_ocp)
    
        if diff_ocp and diff_ceph:
            print (f'[*] Both CephFS and OpenShift have inconsistencies.')
        if diff_ocp:
            print(f'[*] {len(diff_ocp)} stale volume{"" if len(diff_ocp) == 1 else "s"} exist{"s" if len(diff_ocp) == 1 else ""} in OpenShift that have no matching CephFS subvol.')
            print('[*] Volume(s): ', end="")
            print(*diff_ocp, sep=", ")
        if diff_ceph:
            print(f'[*] {len(diff_ceph)} stale subvol{"" if len(diff_ceph) == 1 else "s"} exist{"s" if len(diff_ceph) == 1 else ""} in CephFS that have no matching OpenShift volume.')
            print(f'[*] Volume(s): ', end="")
            print(*diff_ceph, sep=", ")
        
        report.update({'missing_in_ceph': diff_ocp,'missing_in_shift': diff_ceph})
        #print('[DEBUG] ', report)
        return (diff_ocp, diff_ceph)



def main():

    #print(args)
    while True:
        if args.live:

            print(f'[*] Checking if "oc" commands works.')

            oc_whoami_run = subprocess.run(['oc','whoami'],text=True, capture_output=True)
            subprocess.run(['oc','project', 'openshift-storage'],text=True, capture_output=True)
            oc_whoami_output = oc_whoami_run.stdout.strip()

            print(f'[*] The output of \'oc whoami\' is:', oc_whoami_output)

            if oc_whoami_output == 'system:admin':
                print(f'[*] Continuing as Admin...')
            else:
                print(f'[*] Was expecting system:admin but got ', oc_whoami_output, 'instead.')
                user_input = input(f'[*] Continue (y/n)? ')

                if user_input != 'y' or 'Y' or 'yes' or 'Yes':
                    break

            print(f'[*] Checking for ODF storagecluster CR...')
                        
            if not subprocess.run(['oc','get','storagecluster','ocs-storagecluster'],text=True, capture_output=True).returncode:
                print(f'[*] Storage cluster CR exists.')

                if subprocess.run(['oc','get','storagecluster','-o=jsonpath="{.items[].status.phase}"'],text=True, capture_output=True).stdout.strip() == '"Ready"':                    
                    print(f'[*] ODF cluster status is ready.')
                else:
                    print(subprocess.run(['oc','get','storagecluster','-o=jsonpath="{.items[].status.phase}"'],text=True, capture_output=True).stdout.strip())
                    print(f'[*] ODF cluster exists, but is not in ready state, giving up.')
                    break

            else:
                print(f'[*] No storagecluster CR detected... giving up.')
                break

            print(f'[*] Checking for ODF tools pod.')

            tools_pod_name = find_tools_pod_name()
            
            if tools_pod_name:
                print(f'[*] ODF tools pod is running.')
            else:
                print(f'[*] ODF tools pod is not running. Please enable the tools pod and try again.')
                break

            print(f'[*] Checking Ceph health.')
            ceph_health = check_ceph_health(tools_pod_name)
            print(f'[*] Ceph health is: ' + ceph_health)

            print(f'[*] Gathering data for CephFS volumes from OpenShift.')
            gathered_cephfs_pv_info = gather_cephfs_pv_info()
            
            if not len(gathered_cephfs_pv_info):
                print(f'[*] No CephFS volumes found in OpenShift.')
            else:
                print(f'[*] ' + str(len(gathered_cephfs_pv_info)) + ' CephFS volumes found in OpenShift.')
                total_pv_size = total_cephfs_pv_capacity(gathered_cephfs_pv_info)
                print(f'[*] ' + str(total_pv_size) + ' Gi total of CephFS volumes found in OpenShift.')
                
            cephfs_subvol_list = gather_cephfs_subvol_list(tools_pod_name)
            #cephfs_num_vols = str(len(cephfs_subvol_list))

            subvol_count = 0
            for X in cephfs_subvol_list:
                subvol_count += (len((cephfs_subvol_list[X])))

            print(f'[*] ' + str(subvol_count) + f' CephFS volumes found in Ceph.')
            
            gathered_ceph_subvol_info = gather_cephfs_subvol_info(tools_pod_name, cephfs_subvol_list)
            print(gathered_ceph_subvol_info)
            print(f'')
            print(f'[*] Gathering data for CephFS snapshots from OpenShift.')
            
            gathered_snaps_ocp = gather_cephfs_ocp_vsc_info(tools_pod_name)
            snap_count = str(len(gathered_snaps_ocp[0]))
            print(f'[*] ' + snap_count + ' CephFS snapshots found in OpenShift.')
            #print(f'[*] Gathering data for CephFS snapshots from Ceph.')
            gathered_snaps_ceph = gather_cephfs_subvol_snapshot_list(tools_pod_name, cephfs_subvol_list)

            pprint(gathered_snaps_ceph)

            pprint(snap_count)

            #print(f'')
            print(f'[*] Compairing the data for possible stale volumes.')

            find_stale_volumes(gathered_ceph_subvol_info, gathered_cephfs_pv_info)
           

        break

if __name__ == "__main__":
    main()


