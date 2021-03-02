import os
import subprocess

filepath=os.path.realpath(__file__)
script_dir = os.path.dirname(os.path.normpath(filepath))
home_dir = os.path.dirname(os.path.normpath(script_dir))
conf_dir = home_dir+"/conf"
CONF = conf_dir+"/config.xml"

f = open(CONF)
start = False
concactstr = ""
for line in f:
    if line.find("setting") == -1:
        line = line[:-1]
    concactstr += line
res=concactstr.split("<attribute>")

slavelist=[]
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
                slavelist.append(slave)

for slave in slavelist:
    print "start FastPRPeerNode on " + slave
    os.system("ssh " + slave + " \"killall FastPRPeerNode \"")
    command="scp "+home_dir+"/FastPRPeerNode "+slave+":"+home_dir+"/"
    os.system(command)
    command="ssh "+slave+" \"cd "+home_dir+"; ./FastPRPeerNode &> "+home_dir+"/peer_output &\""
    os.system(command)

