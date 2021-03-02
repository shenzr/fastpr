import os
import subprocess

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

# find all slave list and hotstands list
slavelist=[]
fstype=""
hotstands=[]
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
                slavelist.append(slave)
    if attr.find("hotstandby_node_ips") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("/<attribute>")
        attrtmp=attr[valuestart:valueend]
        standstmp=attrtmp.split("<value>")
        for standsentry in standstmp:
            if standsentry.find("</value>") != -1:
                entrysplit=standsentry.split("/")
                stand=entrysplit[0][0:-1]
                hotstands.append(stand)


for slave in slavelist:
    print("Clean blkDir on " + slave)
    os.system("ssh " + slave + " \"cd "+home_dir+"; rm ./blkDir/subdir0/* \"")

for slave in hotstands:
    print("Clean blkDir on " + slave)
    os.system("ssh " + slave + " \"cd "+home_dir+"; rm ./blkDir/subdir0/* \"")
