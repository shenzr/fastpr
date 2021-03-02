#include "RepairGroup.hh"

RepairGroup::RepairGroup(vector<RepairStripe*> reconstruction, vector<RepairStripe*> migration) {
    _reconstruction = reconstruction;
    _migration = migration;
}

void RepairGroup::setRepairTime(double time) {
    _repair_time = time;
}

void RepairGroup::setReconstructionTime(double time) {
    _reconstruction_time = time;
}

void RepairGroup::setMigrationTime(double time) {
    _migration_time = time;
}

double RepairGroup::getRepairTime() {
    return _repair_time;
}

double RepairGroup::getReconstructionTime() {
    return _reconstruction_time;
}

void RepairGroup::findReceiverNode(Config* conf, vector<ECStripe*> stripes, string scenario) {
    if (scenario == "scatteredRepair")
        scatterReceiverNode(conf, stripes, scenario);
    else if (scenario == "hotStandbyRepair")
        hotstandbyReceiverNode(conf);
}

void RepairGroup::hotstandbyReceiverNode(Config* conf) {
    int _peer_node_num = conf->_peerNodeIPs.size();
    cout << "hotstandbyReceiver::peer node num = " << _peer_node_num << endl;
    int _hot_standby_num = conf->_hotStandbyNodeIPs.size();

    if (_migration.size() > 0 && _reconstruction.size() > 0) {
        for (int i=0; i<_migration.size(); i++) {
            RepairStripe* curstripe = _migration[i];
            curstripe->setRepairNodeId(_peer_node_num);
        }
        for (int i=0; i<_reconstruction.size(); i++) {
            RepairStripe* curstripe = _reconstruction[i];
            curstripe->setRepairNodeId(i%(_hot_standby_num-1) + 1 + _peer_node_num);
        }
    } else if (_migration.size() == 0) {
        // reconstruction only
        for (int i=0; i<_reconstruction.size(); i++) {
            RepairStripe* curstripe = _reconstruction[i];
            curstripe->setRepairNodeId(i%(_hot_standby_num) + _peer_node_num);
        }
    } else if (_reconstruction.size() == 0) {
        // migration only
        for (int i=0; i<_migration.size(); i++) {
            RepairStripe* curstripe = _migration[i];
            curstripe->setRepairNodeId(i%_hot_standby_num + _peer_node_num);
        }
    }
    cout << "receiver nodes: ";
    for (int i=0; i<_reconstruction.size(); i++) {
        cout << _reconstruction[i]->_receiver_nodeid << " ";
    }
    for (int i=0; i<_migration.size(); i++) {
        cout << _migration[i]->_receiver_nodeid << " ";
    }
    cout << endl;
}

void RepairGroup::scatterReceiverNode(Config* conf, vector<ECStripe*> stripes, string scenario) {

    int i;
    int j;

    int stripe;                                                                                                                                                                                                                                                                   
    int ret;

    int repaircnt = _reconstruction.size() + _migration.size();
    _peer_node_num = conf->_peer_node_num;
    int* distribution = (int*)malloc(sizeof(int)*repaircnt*_peer_node_num);
    int* receiver_node_dist=(int*)malloc(sizeof(int)*_peer_node_num);

    memset(receiver_node_dist, -1, sizeof(int)*_peer_node_num);

    for(i=0; i<repaircnt*_peer_node_num; i++)                                                                                                                                                                                                          
        distribution[i]=1;

    // establish the candidate nodes
    for(int i=0; i<repaircnt; i++) {
        RepairStripe* currpstripe;
        if (i < _reconstruction.size()) 
            currpstripe = _reconstruction[i];
        else
            currpstripe = _migration[i-_reconstruction.size()];
        int global_stripe_id = currpstripe->_stripe_id;
        ECStripe* ecstripe = stripes[global_stripe_id];

        vector<int> chunklist = ecstripe->_chunk_list;
        for (int j=0; j<chunklist.size(); j++) {
            int nodeid = chunklist[j];
            distribution[i*_peer_node_num + nodeid] = 0;
        }
    }

    _mark=(int*)malloc(sizeof(int)*_peer_node_num);

    // find the receiver node distribution
    for(int i=0; i<repaircnt; i++) {
        memset(_mark, 0, sizeof(int)*_peer_node_num);
        ret = hungary(0, i, distribution, 0, receiver_node_dist);
        if(ret!=1){
            cout << "ERR: receiver matching fails" << std::endl;
            exit(-1);
        }
    }
    int* receiver_node = (int*)malloc(sizeof(int)*repaircnt);

    for(i=0; i<_peer_node_num; i++){
        if(receiver_node_dist[i]==-1)
            continue;
        receiver_node[receiver_node_dist[i]] = i;
    }

    for (int i=0; i<repaircnt; i++) {
        RepairStripe* curstripe;
        if (i < _reconstruction.size())
            curstripe = _reconstruction[i];
        else
            curstripe = _migration[i - _reconstruction.size()];
        curstripe->setRepairNodeId(receiver_node[i]);
    }

    cout << "receiver nodes: ";
    for (int i=0; i<repaircnt; i++) {
        cout << receiver_node[i] << " ";
    }
    cout << endl;
}

int RepairGroup::hungary(int rg_id, int cur_match_stripe, int* matrix, int matrix_start_addr, int* node_selection) {
    int i;                                                                                                                                                                                                                                                                        
    int ret;

    for(i=0; i<_peer_node_num; i++){
        if((matrix[matrix_start_addr + cur_match_stripe*_peer_node_num + i]==1) && (_mark[i]==0)){
            _mark[i]=1; //this node is now under checking

            if(node_selection[rg_id*_peer_node_num+i]==-1){
                node_selection[rg_id*_peer_node_num+i]=cur_match_stripe;
                return 1;
            } else if ((ret=hungary(rg_id, node_selection[rg_id*_peer_node_num+i], matrix, matrix_start_addr, node_selection))==1) {
                node_selection[rg_id*_peer_node_num+i] = cur_match_stripe;
                return 1;
            }
        }
    }
    return 0;
}

void RepairGroup::generateCommands(Config* conf, unordered_map<int, string>& cmdres, string scenario) {
    unordered_map<int, vector<string>> commands;
    //cmds for reconstruction
    for (int i=0; i<_reconstruction.size(); i++) {
        RepairStripe* rpstripe = _reconstruction[i];
        rpstripe->genReconstructionCommands(conf, commands, scenario);
    }

    //cmds for migration
    for (int i=0; i<_migration.size(); i++) {
        RepairStripe* rpstripe = _migration[i];
        rpstripe->genMigrationCommands(conf, commands, scenario);
    }

    // append commands
    for (auto item: commands) {
        int nodeid = item.first;
        vector<string> cmds = item.second;

        string cmd;

        cmd += to_string(cmds.size());
        for (int i=0; i<cmds.size(); i++)
            cmd += cmds[i];

        cmdres[nodeid] = cmd;
        cout << nodeid << ": " << cmd << endl;
    }
}

void RepairGroup::enforceCommands(Config* conf, unordered_map<int, string>& commands) {
    int num_acks = 0;
    for (auto item: commands) {
        int nodeid = item.first;
        string cmd = item.second;

        if (cmd.find("RR") != -1)
            num_acks++;
        else if (cmd.find("MR") != -1)
            num_acks++;
        else if (cmd.find("TR") != -1)
            num_acks++;
        else if (cmd.find("HR") != -1)
            num_acks++;
        
        unsigned int nodeip;
        if (nodeid < conf->_peerNodeIPs.size())
            nodeip = conf->_peerNodeIPs[nodeid];
        else
            nodeip = conf->_hotStandbyNodeIPs[nodeid - conf->_peerNodeIPs.size()];

        cout << FastPRUtil::ip2Str(nodeip) << ": " << cmd << endl;

        // send
        Socket* sock = new Socket(); 
        sock->sendData((char*)cmd.c_str(), cmd.length(), (char*)FastPRUtil::ip2Str(nodeip).c_str(), PN_RECV_CMD_PORT);

        delete sock;
    }
    
    char* recv_ack = (char*)malloc(sizeof(char)*(ACK_LEN+1)*num_acks);
    Socket* sock_recv_cmmt = new Socket();
    sock_recv_cmmt->paraRecvData(CD_RECV_ACK_PORT, ACK_LEN+1, recv_ack, num_acks, NULL, -1, -1, ACK_INFO, "");

    free(recv_ack);
    delete sock_recv_cmmt;

    cout << "end commands---------------" << endl;
}
