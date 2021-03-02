# usage:
#   python dist.py [stripenum] [ratio]

import math
import random
import os
import sys
import subprocess

STRIPENUM=int(sys.argv[1])
RATIOK=float(sys.argv[2])

filepath=os.path.realpath(__file__)
script_dir = os.path.dirname(os.path.normpath(filepath))
home_dir = os.path.dirname(os.path.normpath(script_dir))
conf_dir = home_dir+"/metadata"
CONF = conf_dir+"/config.xml"

f = open(CONF)
start = False
concactstr = ""
for line in f:
    if line.find("setting") == -1:
        line = line[:-1]
    concactstr += line
res=concactstr.split("<attribute>")


# figure out slave list
CLUSTER=[]
fstype=""
for attr in res:
    if attr.find("peer_node_ips") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("</attribute>")
        attrtmp=attr[valuestart:valueend]
        slavestmp=attrtmp.split("<value>")
        for slaveentry in slavestmp:
            if slaveentry.find("</value>") != -1:
                entrysplit=slaveentry.split("/")
                slave=entrysplit[0][0:-1]
                CLUSTER.append(slave)

print(CLUSTER)


STRIPE_FOLDER = home_dir+"/genlrc"
ECN=0
ECK=0
ECR=0
BLKDIR=""
for attr in res:
    if attr.find("erasure_code_n") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("</value>")
        ECN=int(attr[valuestart+7:valueend])
        print(ECN)
    if attr.find("erasure_code_k") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("</value>")
        ECK=int(attr[valuestart+7:valueend])
        print(ECK)
    if attr.find("erasure_code_r") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("</value>")
        ECR=int(attr[valuestart+7:valueend])
        print(ECR)
    if attr.find("local_data_path") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("</value>")
        BLKDIR=attr[valuestart+7:valueend]
        print(BLKDIR)

ECL=ECK/ECR
ECG=ECN-ECK-ECL

BLKNAMELEN=24
STRIPENAMELEN=4
STFNODE=0


for node in CLUSTER:
    cmd = "ssh "+node+" \"mkdir -p "+BLKDIR+"\""
    print(cmd)
    #os.system(cmd)

NODENUM=len(CLUSTER)
SRCBLKS=[]
for i in range(ECN):
    blk = STRIPE_FOLDER+"/"+"blk_"+str(i)
    SRCBLKS.append(blk)
print(SRCBLKS)

datainstf=0
targetdata = math.ceil(STRIPENUM * RATIOK)
parityinstf=0
targetparity=STRIPENUM-targetdata

stripedict=[]

for stripeidx in range(STRIPENUM):

    # stripename
    stripename=""
    zerolen = STRIPENAMELEN - len(str(stripeidx))
    for i in range(zerolen):
        stripename += "0"
    stripename += str(stripeidx)

    # generate n block names
    blklist = []
    for blkidx in range(ECN):
        blkname = "blk_"
        curblkidx = stripeidx * ECN + blkidx
        zerolen = BLKNAMELEN - len(blkname)
        len(str(curblkidx))
        for i in range(zerolen):
            blkname += "0"
        blkname += str(curblkidx)
        blklist.append(blkname)
    print(blklist)

    # generate placement for each block
    blkplace = []
    curmap = {}
    curvalue = []

    # assign stfnode
    if RATIOK == 2:
        # for random placement
        # assign stfnode to either one of the index in [0, ecn-1]
        stfidx = random.randint(0, ECN-1)
        curmap[stfidx] = STFNODE
        curvalue.append(STFNODE)
    else:
        # for ratio placement
        if datainstf < targetdata and parityinstf < targetparity:
            stfidx = random.randint(0, ECN-1)
        elif datainstf < targetdata and parityinstf >= targetparity:
            stfidx = random.randint(0, ECK-1)
        elif datainstf >= targetdata and parityinstf < targetparity:
            stfidx = random.randint(ECK, ECN-1)

        curmap[stfidx] = STFNODE
        curvalue.append(STFNODE)
        if stfidx < ECK:
            datainstf += 1
        else:
            parityinstf += 1

    # assign remaining index
    for chunkid in range(ECN):
        if chunkid in curmap:
            continue

        randomidx = random.randint(0, NODENUM-1)
        while randomidx in curvalue:
            randomidx = random.randint(0, NODENUM-1)

        curmap[chunkid] = randomidx
        curvalue.append(randomidx)

    for chunkid in range(ECN):
        blkplace.append(curmap[chunkid])

    # distribute blocks to corresponding nodes
    for i in range(ECN):
        srcblk = SRCBLKS[i]
        targetblk = blklist[i]
        locationidx = blkplace[i]
        location = CLUSTER[locationidx]
        cmd = "scp "+srcblk+" "+location+":"+BLKDIR+"/"+targetblk
        print(cmd)
        #os.system(cmd)

    # stripeinfo
    stripeinfo = stripename+";"
    for i in range(ECN):
        idx = blkplace[i]
        loc = CLUSTER[idx]
        stripeinfo+=blklist[i]+":"+loc+";"
    stripeinfo += "\n"
    stripedict.append(stripeinfo)

f=open(home_dir+"/metadata/placement", "w")
for line in stripedict:
    f.write(line)
f.close()

