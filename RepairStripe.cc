#include "RepairStripe.hh"

RepairStripe::RepairStripe(int stripe_id, vector<vector<int>> chunks, int rpnodeid, ECBase* ec, unordered_map<int, string> chunk2name, string stripename) {
    _stripe_id = stripe_id;
    _chunks = chunks;
    _repair_nodeid = rpnodeid;
    _ec = ec;
    _chunk2name = chunk2name;
    _stripe_name = stripename;

    for (int i=0; i<chunks.size(); i++) {
        vector<int> chunkinfo = chunks[i];
        int chunkid = chunkinfo[0];
        int nodeid = chunkinfo[1];
        _chunk2node[chunkid] = nodeid;                                    
        _node2chunk[nodeid] = chunkid;
        if (nodeid == rpnodeid)
            _repair_chunk_idx = chunkid;

    }
}

void RepairStripe::setRepairNodeId(int nodeid) {
    _receiver_nodeid = nodeid;
}

void RepairStripe::setSourceNode(int nodeid) {
    int chunkid = _node2chunk[nodeid];
    _src_chunks.push_back(chunkid);
    _src_nodeid.push_back(nodeid);
}

void RepairStripe::genReconstructionCommands(Config* conf, unordered_map<int, vector<string>>& cmds, string scenario) {
    cout << "stripe_id: " << _stripe_id << endl;
    
    // get coeff
    vector<int> coeff = _ec->getDecodeCoef(_src_chunks, _repair_chunk_idx);
    for (int i=0; i<coeff.size(); i++)
        cout << coeff[i] << " ";
    cout << endl;

    // gen repair sender commands
    vector<unsigned int> srciplist;
    for (int i=0; i<_src_chunks.size(); i++) {
        int chunkid = _src_chunks[i];
        string chunkname = _chunk2name[chunkid];
        string stripename = _stripe_name;
        int coef = coeff[i];
        int nodeid = _receiver_nodeid;
        unsigned int nextip;
        if (scenario == "scatteredRepair")
            nextip = conf->_peerNodeIPs[nodeid];
        else
            nextip = conf->_hotStandbyNodeIPs[nodeid - conf->_peerNodeIPs.size()];
        string cmd = genRepairSenderCommand(chunkname, stripename, coef, nextip);
        //cout << cmd << endl;

        int srcnodeid = _src_nodeid[i];
        unsigned int srcnodeip = conf->_peerNodeIPs[srcnodeid];
        srciplist.push_back(srcnodeip);
        if (cmds.find(srcnodeid) == cmds.end())
            cmds[srcnodeid] = {cmd};
        else
            cmds[srcnodeid].push_back(cmd);
    }

    // gen repair receiver commands
    int repair_chunk_id = _repair_chunk_idx;
    string repair_chunk_name = _chunk2name[repair_chunk_id];
    string receiverCmd = genRepairReceiverCommand(repair_chunk_name, _stripe_name, srciplist, scenario);
    //cout << receiverCmd << endl;
    if (cmds.find(_receiver_nodeid) == cmds.end())
        cmds[_receiver_nodeid] = {receiverCmd};
    else
        cmds[_receiver_nodeid].push_back(receiverCmd);

//    cout << "debug genReconstructionCommands:" << endl;
//    for (auto item: cmds) {
//        int nodeid = item.first;
//        cout << nodeid << ": ";
//        vector<string> cmdlist = item.second;
//        for (int i=0; i<cmdlist.size(); i++)
//            cout << cmdlist[i] << " ";
//        cout << endl;
//    }
}

void RepairStripe::genMigrationCommands(Config* conf, unordered_map<int, vector<string>>& cmds, string scenario) {
    
    // gen migration sender commands
    string repair_chunk_name = _chunk2name[_repair_chunk_idx];
    unsigned int nextip;
    if (scenario == "scatteredRepair")
        nextip = conf->_peerNodeIPs[_receiver_nodeid];
    else
        nextip = conf->_hotStandbyNodeIPs[_receiver_nodeid - conf->_peerNodeIPs.size()];
    string sendercmd = genMigrationSenderCommand(repair_chunk_name, _stripe_name, nextip);
    if (cmds.find(_repair_nodeid) == cmds.end())
        cmds[_repair_nodeid] = {sendercmd};
    else
        cmds[_repair_nodeid].push_back(sendercmd);

    // gen migration receiver commands
    string receivercmd = genMigrationReceiverCommand(repair_chunk_name, _stripe_name);
    if (cmds.find(_receiver_nodeid) == cmds.end())
        cmds[_receiver_nodeid] = {receivercmd};
    else
        cmds[_receiver_nodeid].push_back(receivercmd);
}

string RepairStripe::genRepairSenderCommand(string chunkname, string stripename, int coeff,
        unsigned int nextip) {
    string prefix = "RS"; 

    string cmd;
    // 0. chunkname
    for (int i=chunkname.length(); i<BLK_NAME_LEN; i++)
        cmd += "0";
    cmd += chunkname;

    // 1. stripename
    for (int i=stripename.length(); i<STRIPE_NAME_LEN; i++)
        cmd += "0";
    cmd += stripename;

    // 2. coeff
    assert(to_string(coeff).length()<4);
    for (int i=0; i<COEFF_LEN-(int)to_string(coeff).length(); i++)
        cmd += "0";
    cmd += to_string(coeff);

    // 3. nextip (receiver ip)
    string nextipstr = to_string(nextip);
    for (int i=nextipstr.length(); i<NEXT_IP_LEN; i++)
        cmd += "0";
    cmd += nextipstr;

    // content len, does not include header
    int len = cmd.length();
    string sizestr;
    for (int i=0; i<4 - (int)to_string(len).length(); i++)
        sizestr += "0";
    sizestr += to_string(len);
    
    //cmd = prefix + sizestr + cmd;
    cmd = prefix + cmd;

    return cmd;
}

string RepairStripe::genRepairReceiverCommand(string chunkname, string stripename, vector<unsigned int> srclist, string scenario) {

    string prefix;
    if (scenario == "scatteredRepair")
        prefix = "RR";
    else
        prefix = "HR";
    
    string cmd;
    // 0. chunkname
    for (int i=chunkname.length(); i<BLK_NAME_LEN; i++)
        cmd += "0";
    cmd += chunkname;

    // 1. stripename
    for (int i=stripename.length(); i<STRIPE_NAME_LEN; i++)
        cmd += "0";
    cmd += stripename;

    // 2. number received chunks = 0
    int num = srclist.size();
    for (int i=0; i<COEFF_LEN - (int)to_string(num).length(); i++)
        cmd += "0";
    cmd += to_string(num);
    //for (int i=0; i<COEFF_LEN; i++)
    //    cmd += "0";

    // 3. nextip (receiver ip) = 0
    for (int i=0; i<NEXT_IP_LEN; i++)
        cmd += "0";

    if (scenario == "hotStandbyRepair") {
        for (int i=0; i<srclist.size(); i++) {
            string ipstr = to_string(srclist[i]);
            for (int i=ipstr.length(); i<NEXT_IP_LEN; i++)
                cmd += "0";
            cmd += ipstr;
        }
    }

    // content len, does not include header
    int len = cmd.length();
    string sizestr;
    for (int i=0; i<4 - (int)to_string(len).length(); i++)
        sizestr += "0";
    sizestr += to_string(len);
    
    if (scenario == "scatteredRepair")
        cmd = prefix + cmd;
    else
        cmd = prefix + sizestr + cmd;

    return cmd;
}

string RepairStripe::genMigrationSenderCommand(string chunkname, string stripename,
        unsigned int nextip) {

    string prefix = "MS";
    string cmd;
    
    // 0. chunkname
    for (int i=chunkname.length(); i<BLK_NAME_LEN; i++)
        cmd += "0";
    cmd += chunkname;

    // 1. stripename
    for (int i=stripename.length(); i<STRIPE_NAME_LEN; i++)
        cmd += "0";
    cmd += stripename;

    // 2. coeff = 0
    for (int i=0; i<COEFF_LEN; i++)
        cmd += "0";

    // 3. nextip (receiver ip)
    string nextipstr = to_string(nextip);
    for (int i=nextipstr.length(); i<NEXT_IP_LEN; i++)
        cmd += "0";
    cmd += nextipstr;

    // content len, does not include header
    int len = cmd.length();
    string sizestr;
    for (int i=0; i<4 - (int)to_string(len).length(); i++)
        sizestr += "0";
    sizestr += to_string(len);
    
    cmd = prefix + cmd;

    return cmd;
}

string RepairStripe::genMigrationReceiverCommand(string chunkname, string stripename) {

    string prefix = "MR";
    string cmd;
    
    // 0. chunkname
    for (int i=chunkname.length(); i<BLK_NAME_LEN; i++)
        cmd += "0";
    cmd += chunkname;

    // 1. stripename
    for (int i=stripename.length(); i<STRIPE_NAME_LEN; i++)
        cmd += "0";
    cmd += stripename;

    // 2. coeff = 0
    for (int i=0; i<COEFF_LEN; i++)
        cmd += "0";

    // 3. nextip (receiver ip) = 0
    for (int i=0; i<NEXT_IP_LEN; i++)
        cmd += "0";

    // content len, does not include header
    int len = cmd.length();
    string sizestr;
    for (int i=0; i<4 - (int)to_string(len).length(); i++)
        sizestr += "0";
    sizestr += to_string(len);
    
    cmd = prefix + cmd;

    return cmd;
}
