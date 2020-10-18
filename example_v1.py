import hashlib
import base64
import boto3
import time

REGION='us-west-2'
CONNECTION_POOL_SIZE = 16
DUMMY_DATA_FILE='./512kb_data'
BLOCK_SIZE_512KB = 512 * 1024 

EBS_SNAPSHOT_SIZE = 1
EBS_SNAPSHOT_SIZE_IN_BYTES = EBS_SNAPSHOT_SIZE * 1024 * 1024 * 1024
TOTAL_512KB_BLOCKS = EBS_SNAPSHOT_SIZE_IN_BYTES / BLOCK_SIZE_512KB 

def create_ec2_client():
   ec2 = boto3.client('ec2',
                        verify=True,
                        #aws_access_key_id=access_key,
                        #aws_secret_access_key=secret_key,
                        region_name = REGION)
   return ec2	

def create_ebs_client():
   ebs = boto3.client('ebs', region_name=REGION)
   return ebs

def ebs_start_snapshot(ebs, vol_size, desc, parent_snapshot = None):
   if parent_snapshot:
   	rsp = ebs.start_snapshot(VolumeSize=vol_size,
	 ParentSnapshotId=parent_snapshot,
	 Description = desc) 
   else:
   	rsp = ebs.start_snapshot(VolumeSize=vol_size,
	 Description = desc) 
   print("Start_Snapshot:", rsp)
   return rsp['SnapshotId']

def ebs_list_snapshot_blocks(ebs, snapshot_id):
    rsp= ebs.list_snapshot_blocks(SnapshotId=snapshot_id)
    print("List_Snapshot_blocks:", rsp)

def ebs_complete_snapshot(ebs, snapshot_id, changed_blocks_count):
    rsp = ebs.complete_snapshot(SnapshotId=snapshot_id, ChangedBlocksCount=changed_blocks_count)
    print("Complete_Snapshot:", rsp)

def ebs_put_block_on_snapshot(ebs, snapshot_id, block_index, data, data_length, cksum, cksum_algo):
    rsp = ebs.put_snapshot_block(SnapshotId=snapshot_id, BlockIndex=block_index, BlockData=data, DataLength=data_length, Checksum=cksum, ChecksumAlgorithm=cksum_algo)
    #print("Put_Snapshot_block:", rsp)

def read_dummy_data():
    with open(DUMMY_DATA_FILE, "r") as fd:
        data = fd.read()
    return data

def write_into_snapshot(ebs, snapshot_id):
    data = read_dummy_data()
    c = hashlib.sha256(data)
    cksum = c.digest()
    base64_encoded_cksum = base64.b64encode(cksum)
    print("c",c)
    print("cksum",cksum)
    print("b64 encoded cksum",base64_encoded_cksum)
    total_blocks = 0
    for block_index in range(TOTAL_512KB_BLOCKS):
        total_blocks = total_blocks + 1
        rsp = ebs_put_block_on_snapshot(ebs, snapshot_id, block_index, data, BLOCK_SIZE_512KB,  base64_encoded_cksum, 'SHA256')
        print("[block_index]" + str(block_index) +"] written\n")
    print("Data-Write Complete ! Total_blocks ="+ str(total_blocks))


def ec2_list_ebs_snapshots(ec2,snapshot_id):
    rsp = ec2.describe_snapshots(SnapshotIds=[snapshot_id])
    print(rsp)

ec2 = create_ec2_client()
ebs = create_ebs_client()
snapshot_id = ebs_start_snapshot(ebs, EBS_SNAPSHOT_SIZE, "EBS_Direct_v2")

start_time = time.time()
write_into_snapshot(ebs, snapshot_id)
write_complete_time = time.time()

ebs_complete_snapshot(ebs, snapshot_id, TOTAL_512KB_BLOCKS)
snapshot_complete_time = time.time()

print("Total time required to write : %s", str(write_complete_time - start_time))
print("Total time required to complete snapshot : %s", str(snapshot_complete_time - write_complete_time))

ec2_list_ebs_snapshots(ec2, snapshot_id)
