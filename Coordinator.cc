#include "Coordinator.hh"

Coordinator::Coordinator(Config* conf) {
    _conf = conf;

    if (_conf->_code_type == "rs") {
        vector<int> param;
        param.push_back(_conf->_ecN);
        param.push_back(_conf->_ecK);

        _ec = new RS(param);
    } else if (_conf->_code_type == "azurelrc") {
        vector<int> param;
        param.push_back(_conf->_ecN);
        param.push_back(_conf->_ecK);
        param.push_back(_conf->_ecL);
        param.push_back(_conf->_ecG);

        _ec = new AzureLRC(param);
    } else if (_conf->_code_type == "azurelrcplus") {
        vector<int> param;
        param.push_back(_conf->_ecN);
        param.push_back(_conf->_ecK);
        param.push_back(_conf->_ecL);
        param.push_back(_conf->_ecG);

        _ec = new AzureLRCPlus(param);
    }

    initPlacement();
}

void Coordinator::initPlacement() {
    if (_conf->_fs_type == "standalone") {
        cout << "standalone!" << endl;
        readPlacementFromFile();
    }
}

void Coordinator::readPlacementFromFile() {
    string placefile = "conf/placement";
    ifstream file(placefile);
    string line;
    int stripeid = 0;
    while (std::getline(file, line)) {
        ECStripe* ecstripe = new ECStripe(_conf, line, stripeid++);
        _stripes.push_back(ecstripe);
    }

    cout << "Total number of stripes: " << _stripes.size() << endl;
}

void Coordinator::getLostInfo(int stfnode) {
    _stfnode_idx = stfnode;
    _stfnode_ip = _conf->_peerNodeIPs[stfnode];

    for (int i=0; i<_stripes.size(); i++) {
        ECStripe* curstripe = _stripes[i];
        if (curstripe->containsNodeId(_stfnode_idx))
            _lostStripeIndices.push_back(i);
    }
}

int Coordinator::getLostNum() {
    return _lostStripeIndices.size();
}

void Coordinator::fastprRSRepair(string scenario) {
    
    // get repair stripes for chunks in STF node
    vector<RepairStripe*> repaircollections = getRSRepairStripes();
    cout << "number of repair collections: " << repaircollections.size() << endl;

    int repairk = _conf->_ecK;
    int repairn = _conf->_ecN;
   
    // given repair collections, find reconstruction sets
    ReconstructionSets* reconsetsclass = new ReconstructionSets(repaircollections, repairk, repairn, _conf, scenario);
    vector<ReconstructionSet*> reconstructionsets = reconsetsclass->findReconstructionSets(_stfnode_idx);

    // scheduling
    vector<RepairGroup*> repairgroups = naiveRepairScheduling(reconstructionsets, scenario);
    
    // for each repair group, find the receiver nodes 

    for (int i=0; i<repairgroups.size(); i++) {
        RepairGroup* curgroup = repairgroups[i];
        curgroup->findReceiverNode(_conf, _stripes, scenario);

        unordered_map<int, string> cmdmap;
        curgroup->generateCommands(_conf, cmdmap, scenario);
        struct timeval t1, t2;
        gettimeofday(&t1, NULL); 
        curgroup->enforceCommands(_conf, cmdmap);
        gettimeofday(&t2, NULL);
        double t = FastPRUtil::duration(t1, t2);
        //cout << "real repair time = " << t << endl;
        //break;
    }
}

void Coordinator::randomRSRepair(string scenario) {

    // get repair stripes for chunks in STF node
    vector<RepairStripe*> repaircollections = getRSRepairStripes();
    cout << "number of repair collections: " << repaircollections.size() << endl;

    int repairk = _conf->_ecK;
    int repairn = _conf->_ecN;

    // given repair collections, find reconstruction sets
    ReconstructionSets* reconsetsclass = new ReconstructionSets(repaircollections, repairk, repairn, _conf, scenario);
    vector<ReconstructionSet*> reconstructionsets = reconsetsclass->findReconstructionSets(_stfnode_idx);

    for (int i=0; i<reconstructionsets.size(); i++) {
        ReconstructionSet* reconset = reconstructionsets[i];
        vector<RepairStripe*> repairstripes = reconset->_repair_stripes;
        vector<RepairStripe*> migrationstripes;
        RepairGroup* curgroup = new RepairGroup(repairstripes, migrationstripes);

        curgroup->findReceiverNode(_conf, _stripes, scenario);

        unordered_map<int, string> cmdmap;
        curgroup->generateCommands(_conf, cmdmap, scenario);
        curgroup->enforceCommands(_conf, cmdmap);
        //break;
    }
}

void Coordinator::migrationRSRepair(string scenario) {
    // get repair stripes for chunks in STF node                                                                                                                                                                                                                                  
    vector<RepairStripe*> repaircollections = getRSRepairStripes(); 
    
    for (int i=0; i<repaircollections.size(); i++) {
        vector<RepairStripe*> repairstripes;
        vector<RepairStripe*> migrationstripes;
        migrationstripes.push_back(repaircollections[i]);

        RepairGroup* curgroup = new RepairGroup(repairstripes, migrationstripes);

        curgroup->findReceiverNode(_conf, _stripes, scenario);

        unordered_map<int, string> cmdmap;
        curgroup->generateCommands(_conf, cmdmap, scenario);
        curgroup->enforceCommands(_conf, cmdmap);
        //break;
    }

}

void Coordinator::fastprAzureLRCRepair(string scenario) {

    int eck = _conf->_ecK;
    int ecn = _conf->_ecN;
    int ecl = _conf->_ecL;
    int ecg = _conf->_ecG;
    
    // get local stripes for chunks in STF node
    vector<RepairStripe*> localcollection = getAzureLRCRepairStripes(true);
    cout << "local.num = " << localcollection.size() << endl;
    vector<RepairStripe*> globalcollection = getAzureLRCRepairStripes(false);
    cout << "global.num = " << globalcollection.size() << endl;

    int localk = eck/ecl;
    int localn = localk+1;
    ReconstructionSets* lreconsetsclass = new ReconstructionSets(localcollection, localk, localn, _conf, scenario);
    vector<ReconstructionSet*> localsets = lreconsetsclass->findReconstructionSets(_stfnode_idx);

    int globalk = eck;
    int globaln = eck+ecg;
    ReconstructionSets* greconsetsclass = new ReconstructionSets(globalcollection, globalk, globaln, _conf, scenario);
    vector<ReconstructionSet*> globalsets = greconsetsclass->findReconstructionSets(_stfnode_idx);

    if (localsets.size() > 0)
        cout << "t_l: " << localsets.at(0)->getReconstructionTime(scenario) << endl;
    if (globalsets.size() > 0)
        cout << "t_g: " << globalsets.at(0)->getReconstructionTime(scenario) << endl;

    localsets.insert(localsets.end(), globalsets.begin(), globalsets.end());

    vector<RepairGroup*> repairgroups = weightedRepairScheduling(localsets, scenario);
    
    
    // for each repair group, find the receiver nodes 

    for (int i=0; i<repairgroups.size(); i++) {
        RepairGroup* curgroup = repairgroups[i];
        curgroup->findReceiverNode(_conf, _stripes, scenario);

        unordered_map<int, string> cmdmap;
        curgroup->generateCommands(_conf, cmdmap, scenario);
        curgroup->enforceCommands(_conf, cmdmap);
        //break;
    }
}

void Coordinator::randomAzureLRCRepair(string scenario) {

    int eck = _conf->_ecK;
    int ecn = _conf->_ecN;
    int ecl = _conf->_ecL;
    int ecg = _conf->_ecG;
    
    // get local stripes for chunks in STF node
    vector<RepairStripe*> localcollection = getAzureLRCRepairStripes(true);
    cout << "local.num = " << localcollection.size() << endl;
    vector<RepairStripe*> globalcollection = getAzureLRCRepairStripes(false);
    cout << "global.num = " << globalcollection.size() << endl;

    int localk = eck/ecl;
    int localn = localk+1;
    ReconstructionSets* lreconsetsclass = new ReconstructionSets(localcollection, localk, localn, _conf, scenario);
    vector<ReconstructionSet*> localsets = lreconsetsclass->findReconstructionSets(_stfnode_idx);

    int globalk = eck;
    int globaln = eck+ecg;
    ReconstructionSets* greconsetsclass = new ReconstructionSets(globalcollection, globalk, globaln, _conf, scenario);
    vector<ReconstructionSet*> globalsets = greconsetsclass->findReconstructionSets(_stfnode_idx);

    if (localsets.size() > 0)
        cout << "t_l: " << localsets.at(0)->getReconstructionTime(scenario) << endl;
    if (globalsets.size() > 0)
        cout << "t_g: " << globalsets.at(0)->getReconstructionTime(scenario) << endl;

    localsets.insert(localsets.end(), globalsets.begin(), globalsets.end());
    
    for (int i=0; i<localsets.size(); i++) {
        ReconstructionSet* reconset = localsets[i];
        vector<RepairStripe*> repairstripes = reconset->_repair_stripes;
        vector<RepairStripe*> migrationstripes;
        RepairGroup* curgroup = new RepairGroup(repairstripes, migrationstripes);

        curgroup->findReceiverNode(_conf, _stripes, scenario);

        unordered_map<int, string> cmdmap;
        curgroup->generateCommands(_conf, cmdmap, scenario);
        curgroup->enforceCommands(_conf, cmdmap);
        //break;
    }
}

void Coordinator::migrationAzureLRCRepair(string scenario) {

    // get local stripes for chunks in STF node
    vector<RepairStripe*> localcollection = getAzureLRCRepairStripes(true);
    cout << "local.num = " << localcollection.size() << endl;
    vector<RepairStripe*> globalcollection = getAzureLRCRepairStripes(false);
    cout << "global.num = " << globalcollection.size() << endl;

    localcollection.insert(localcollection.end(), globalcollection.begin(), globalcollection.end());

    // for each repair group, find the receiver nodes 
    for (int i=0; i<localcollection.size(); i++) {
        vector<RepairStripe*> repairstripes;
        vector<RepairStripe*> migrationstripes;
        migrationstripes.push_back(localcollection[i]);

        RepairGroup* curgroup = new RepairGroup(repairstripes, migrationstripes);

        curgroup->findReceiverNode(_conf, _stripes, scenario);

        unordered_map<int, string> cmdmap;
        curgroup->generateCommands(_conf, cmdmap, scenario);
        curgroup->enforceCommands(_conf, cmdmap);
        //break;
    }
}

void Coordinator::fastprAzureLRCPlusRepair(string scenario) {

    int eck = _conf->_ecK;
    int ecn = _conf->_ecN;
    int ecl = _conf->_ecL;
    int ecg = _conf->_ecG;
    
    // get local stripes for chunks in STF node
    vector<RepairStripe*> localcollection = getAzureLRCPlusRepairStripes(true);
    cout << "local.num = " << localcollection.size() << endl;
    vector<RepairStripe*> globalcollection = getAzureLRCPlusRepairStripes(false);
    cout << "global.num = " << globalcollection.size() << endl;

    int localk = eck/ecl;
    int localn = localk+1;
    ReconstructionSets* lreconsetsclass = new ReconstructionSets(localcollection, localk, localn, _conf, scenario);
    vector<ReconstructionSet*> localsets = lreconsetsclass->findReconstructionSets(_stfnode_idx);

    //cout << "localsets:" << endl;
    //for (int i=0; i<localsets.size(); i++)
    //    cout << "size: " << localsets[i]->_repair_stripes.size() << ", time: " << localsets[i]->getReconstructionTime(scenario) << endl;

    int globalk = ecg;
    int globaln = ecg+1;
    ReconstructionSets* greconsetsclass = new ReconstructionSets(globalcollection, globalk, globaln, _conf, scenario);
    vector<ReconstructionSet*> globalsets = greconsetsclass->findReconstructionSets(_stfnode_idx);

    //cout << "globalsets:" << endl;
    //for (int i=0; i<globalsets.size(); i++)
    //    cout << "size: " << globalsets[i]->_repair_stripes.size() << ", time: " << globalsets[i]->getReconstructionTime(scenario) << endl;

    if (localsets.size() > 0)
        cout << "t_l: " << localsets.at(0)->getReconstructionTime(scenario) << endl;
    if (globalsets.size() > 0)
        cout << "t_g: " << globalsets.at(0)->getReconstructionTime(scenario) << endl;

    localsets.insert(localsets.end(), globalsets.begin(), globalsets.end());

    vector<RepairGroup*> repairgroups = weightedRepairScheduling(localsets, scenario);
    
    
    // for each repair group, find the receiver nodes 
    for (int i=0; i<repairgroups.size(); i++) {
        RepairGroup* curgroup = repairgroups[i];
        curgroup->findReceiverNode(_conf, _stripes, scenario);

        unordered_map<int, string> cmdmap;
        curgroup->generateCommands(_conf, cmdmap, scenario);
        curgroup->enforceCommands(_conf, cmdmap);
        //break;
    }
}

void Coordinator::randomAzureLRCPlusRepair(string scenario) {

    int eck = _conf->_ecK;
    int ecn = _conf->_ecN;
    int ecl = _conf->_ecL;
    int ecg = _conf->_ecG;
    
    // get local stripes for chunks in STF node
    vector<RepairStripe*> localcollection = getAzureLRCPlusRepairStripes(true);
    cout << "local.num = " << localcollection.size() << endl;
    vector<RepairStripe*> globalcollection = getAzureLRCPlusRepairStripes(false);
    cout << "global.num = " << globalcollection.size() << endl;

    int localk = eck/ecl;
    int localn = localk+1;
    ReconstructionSets* lreconsetsclass = new ReconstructionSets(localcollection, localk, localn, _conf, scenario);
    vector<ReconstructionSet*> localsets = lreconsetsclass->findReconstructionSets(_stfnode_idx);

    int globalk = ecg;
    int globaln = ecg+1;
    ReconstructionSets* greconsetsclass = new ReconstructionSets(globalcollection, globalk, globaln, _conf, scenario);
    vector<ReconstructionSet*> globalsets = greconsetsclass->findReconstructionSets(_stfnode_idx);

    if (localsets.size() > 0)
        cout << "t_l: " << localsets.at(0)->getReconstructionTime(scenario) << endl;
    if (globalsets.size() > 0)
        cout << "t_g: " << globalsets.at(0)->getReconstructionTime(scenario) << endl;

    localsets.insert(localsets.end(), globalsets.begin(), globalsets.end());
    
    for (int i=0; i<localsets.size(); i++) {
        ReconstructionSet* reconset = localsets[i];
        vector<RepairStripe*> repairstripes = reconset->_repair_stripes;
        vector<RepairStripe*> migrationstripes;
        RepairGroup* curgroup = new RepairGroup(repairstripes, migrationstripes);

        curgroup->findReceiverNode(_conf, _stripes, scenario);

        unordered_map<int, string> cmdmap;
        curgroup->generateCommands(_conf, cmdmap, scenario);
        curgroup->enforceCommands(_conf, cmdmap);
        //break;
    }
}

void Coordinator::migrationAzureLRCPlusRepair(string scenario) {

    // get local stripes for chunks in STF node
    vector<RepairStripe*> localcollection = getAzureLRCPlusRepairStripes(true);
    cout << "local.num = " << localcollection.size() << endl;
    vector<RepairStripe*> globalcollection = getAzureLRCPlusRepairStripes(false);
    cout << "global.num = " << globalcollection.size() << endl;

    localcollection.insert(localcollection.end(), globalcollection.begin(), globalcollection.end());

    // for each repair group, find the receiver nodes 
    for (int i=0; i<localcollection.size(); i++) {
        vector<RepairStripe*> repairstripes;
        vector<RepairStripe*> migrationstripes;
        migrationstripes.push_back(localcollection[i]);

        RepairGroup* curgroup = new RepairGroup(repairstripes, migrationstripes);

        curgroup->findReceiverNode(_conf, _stripes, scenario);

        unordered_map<int, string> cmdmap;
        curgroup->generateCommands(_conf, cmdmap, scenario);
        curgroup->enforceCommands(_conf, cmdmap);
        //break;
    }
}

vector<RepairStripe*> Coordinator::getRSRepairStripes() {
    vector<RepairStripe*> toret;
    for (int i=0; i<_lostStripeIndices.size(); i++) {
        int lostidx = i;
        int idx = _lostStripeIndices[i];
        ECStripe* ecstripe = _stripes[idx];
        vector<vector<int>> chunks;
        unordered_map<int, string> chunk2name;
        for (int i=0; i<ecstripe->_chunk_list.size(); i++) {
            vector<int> curpair;
            curpair.push_back(i);
            curpair.push_back(ecstripe->_chunk_list[i]);
            chunks.push_back(curpair);

            chunk2name[i] = ecstripe->_chunk_name_list[i];
        }
        RepairStripe* rpstripe = new RepairStripe(ecstripe->_stripe_id,
                chunks, _stfnode_idx, _ec, chunk2name, ecstripe->_stripe_name);
        toret.push_back(rpstripe);
    }
    return toret;
}

vector<RepairStripe*> Coordinator::getAzureLRCRepairStripes(bool local) {
    int eck = _conf->_ecK;
    int ecn = _conf->_ecN;
    int ecl = _conf->_ecL;
    int ecg = _conf->_ecG;

    int localdatasize = eck / ecl;

    unordered_map<int, vector<int>> groupmap;

    // local groups
    for (int i=0; i<ecl; i++) {
        vector<int> curgroup;
        for (int cid = i * localdatasize; cid < (i+1) * localdatasize; cid++) {
            curgroup.push_back(cid);
        }
        int pid = eck + i;
        curgroup.push_back(pid);

        for(auto cid: curgroup) {
            groupmap.insert(make_pair(cid, curgroup));
        }
    }

    // global groups
    vector<int> globalgroup;
    for (int i=0; i<eck; i++)
        globalgroup.push_back(i);
    for (int i=eck+ecl; i<ecn; i++)
        globalgroup.push_back(i);
    for (auto cid: globalgroup)
        groupmap.insert(make_pair(cid, globalgroup));

    vector<RepairStripe*> toret;
    for(int i=0; i<_lostStripeIndices.size(); i++) {
        int lostidx = i;
        int idx = _lostStripeIndices[i];
        ECStripe* ecstripe = _stripes[idx];
        vector<vector<int>> chunks;

        int chunkidx = ecstripe->getChunkIndex(_stfnode_idx);
        if ( (chunkidx < eck+ecl) && !local  )
            continue;

        if ( (chunkidx >= eck+ecl) && local  )
            continue;

        vector<int> relatedindex = groupmap[chunkidx] ;
        unordered_map<int, string> chunk2name;
        for (int i=0; i<relatedindex.size(); i++) {
            int chunkidx = relatedindex[i];
            int nodeid = ecstripe->_chunk_list[chunkidx];
            vector<int> curpair;
            curpair.push_back(chunkidx);
            curpair.push_back(nodeid);
            chunks.push_back(curpair);                                                                                            

            chunk2name[chunkidx] = ecstripe->_chunk_name_list[chunkidx];
        }
        RepairStripe* rpstripe = new RepairStripe(ecstripe->_stripe_id,
                chunks, _stfnode_idx, _ec, chunk2name, ecstripe->_stripe_name);

        toret.push_back(rpstripe);
    }
    return toret;
}

vector<RepairStripe*> Coordinator::getAzureLRCPlusRepairStripes(bool local) {
    int eck = _conf->_ecK;
    int ecn = _conf->_ecN;
    int ecl = _conf->_ecL;
    int ecg = _conf->_ecG;

    int localdatasize = eck / ecl;

    unordered_map<int, vector<int>> groupmap;

    // local groups
    for (int i=0; i<ecl; i++) {
        vector<int> curgroup;
        for (int cid = i * localdatasize; cid < (i+1) * localdatasize; cid++) {
            curgroup.push_back(cid);
        }
        int pid = eck + i;
        curgroup.push_back(pid);

        for(auto cid: curgroup) {
            groupmap.insert(make_pair(cid, curgroup));
        }
    }

    // global groups
    vector<int> globalgroup;
    for (int i=eck+ecl; i<ecn; i++)
        globalgroup.push_back(i);
    for (auto cid: globalgroup)
        groupmap.insert(make_pair(cid, globalgroup));

    vector<RepairStripe*> toret;
    for(int i=0; i<_lostStripeIndices.size(); i++) {
        int lostidx = i;
        int idx = _lostStripeIndices[i];
        ECStripe* ecstripe = _stripes[idx];
        vector<vector<int>> chunks;

        int chunkidx = ecstripe->getChunkIndex(_stfnode_idx);
        if ( (chunkidx < eck+ecl) && !local  )
            continue;

        if ( (chunkidx >= eck+ecl) && local  )
            continue;

        vector<int> relatedindex = groupmap[chunkidx] ;
        unordered_map<int, string> chunk2name;
        for (int i=0; i<relatedindex.size(); i++) {
            int chunkidx = relatedindex[i];
            int nodeid = ecstripe->_chunk_list[chunkidx];
            vector<int> curpair;
            curpair.push_back(chunkidx);
            curpair.push_back(nodeid);
            chunks.push_back(curpair);                                                                                            

            chunk2name[chunkidx] = ecstripe->_chunk_name_list[chunkidx];
        }
        RepairStripe* rpstripe = new RepairStripe(ecstripe->_stripe_id,
                chunks, _stfnode_idx, _ec, chunk2name, ecstripe->_stripe_name);

        toret.push_back(rpstripe);
    }
    return toret;
}

vector<ReconstructionSet*> Coordinator::naiveSorting(vector<ReconstructionSet*> reconstructionsets) {
    sort(reconstructionsets.begin(), reconstructionsets.end(),
            [](ReconstructionSet* x, ReconstructionSet* y) {
            return x->size() > y->size();
            });

    return reconstructionsets;
}

vector<RepairGroup*> Coordinator::naiveRepairScheduling(vector<ReconstructionSet*> reconstructionsets, string scenario) {
    vector<RepairGroup*> toret;

    vector<ReconstructionSet*> sortedsets = naiveSorting(reconstructionsets);

    // cout << "after sorting" << endl;
    // for (int i=0; i<sortedsets.size(); i++) {
    //     ReconstructionSet* curset = sortedsets[i];

    //     for (int j=0; j<curset->_repair_stripes.size(); j++) {
    //         RepairStripe* repstripe = curset->_repair_stripes[j];
    //         cout << repstripe->_stripe_id << " ";
    //     }
    //     cout << endl;
    // }

    int reconstructionidx = 0;
    int migrationidx = sortedsets.size() - 1;

    while (true) {
        if (reconstructionidx >= sortedsets.size()) { // keyun bug fix
            break;
        }

        // get one reconstructionset for reconstruction
        ReconstructionSet* reconset = sortedsets[reconstructionidx];
        reconstructionidx++;

        if (reconset->_scheduled)
            break;

        // get reconstruction time
        double repair_chunk_time = reconset->getReconstructionTime(scenario);
        double migrate_chunk_time = getChunkMigrationTime();
        int num_migrate_chunk = (int)floor(repair_chunk_time/migrate_chunk_time);
        cout << "Estimation:: repairtime = " << repair_chunk_time << ", migratetime = " << migrate_chunk_time << endl;

        // figure out the stripes for reconstruction
        vector<RepairStripe*> reconstripes = reconset->getRepairStripes4Reconstruction();

        // figure out the chunks for migration
        int migrate_remain=num_migrate_chunk;
        vector<RepairStripe*> migstripes;
        while(migrate_remain > 0) {
            ReconstructionSet* migrationset = sortedsets[migrationidx];
            
            if (migrationset->_scheduled) {
                // no reconstruction set left
                break;
            }

            vector<RepairStripe*> curmigstripes = migrationset->getRepairStripes4Migration(migrate_remain, true);
            if (curmigstripes.size() > 0) {
                migrate_remain -= curmigstripes.size();
                migstripes.insert(migstripes.end(), curmigstripes.begin(), curmigstripes.end());
            }

            if (migrationset->size() == 0) {
                migrationidx--;
            }
        }

        RepairGroup* rg = new RepairGroup(reconstripes, migstripes);
        if (migrate_chunk_time * (reconstripes.size() + migstripes.size()) < repair_chunk_time) 
            rg->setRepairTime(migrate_chunk_time * (reconstripes.size() + migstripes.size()));
        else
            rg->setRepairTime(repair_chunk_time);
        cout << "reconstruction: " << reconstripes.size() << ", migration: " << migstripes.size() << ", time: " << repair_chunk_time << endl;

        toret.push_back(rg);
    }

    return toret;
}

vector<ReconstructionSet*> Coordinator::weightedSorting(vector<ReconstructionSet*> reconstructionsets, string scenario) {
    double t_m = getChunkMigrationTime();
    sort(reconstructionsets.begin(), reconstructionsets.end(),
            [scenario, t_m](ReconstructionSet* x, ReconstructionSet* y) {
            return x->weight(scenario, t_m) > y->weight(scenario, t_m);
            });
    return reconstructionsets;
}

vector<RepairGroup*> Coordinator::weightedRepairScheduling(vector<ReconstructionSet*> reconstructionsets, string scenario) {
    vector<RepairGroup*> toret;

    vector<ReconstructionSet*> sortedsets = weightedSorting(reconstructionsets, scenario);

    int reconstructionidx = 0;
    int migrationidx = sortedsets.size() - 1;

    while (true) {
        if (reconstructionidx >= sortedsets.size())
            break;

        // get one reconstructionset for reconstruction
        ReconstructionSet* reconset = sortedsets[reconstructionidx];
        reconstructionidx++;

        if (reconset->_scheduled)
            break;

        // get reconstruction time
        double repair_chunk_time = reconset->getReconstructionTime(scenario);
        double migrate_chunk_time = getChunkMigrationTime();
        int num_migrate_chunk = (int)floor(repair_chunk_time/migrate_chunk_time);

        // figure out the stripes for reconstruction
        vector<RepairStripe*> reconstripes = reconset->getRepairStripes4Reconstruction();

        // figure out the chunks for migration
        int migrate_remain=num_migrate_chunk;
        vector<RepairStripe*> migstripes;

        while(migrate_remain > 0) {
            ReconstructionSet* migrationset = sortedsets[migrationidx];

            if (migrationset->_scheduled)
                break;

            vector<RepairStripe*> curmigstripes = migrationset->getRepairStripes4Migration(migrate_remain, true);

            if (curmigstripes.size() > 0) {
                migrate_remain -= curmigstripes.size();
                migstripes.insert(migstripes.end(), curmigstripes.begin(), curmigstripes.end());                                            
            } 

            if (migrationset->size() == 0) {
                migrationidx--;                            
            } 
        }

        RepairGroup* rg = new RepairGroup(reconstripes, migstripes);
        rg->setReconstructionTime(repair_chunk_time);
        rg->setMigrationTime(migrate_chunk_time * (reconstripes.size() + migstripes.size()));
        if (migrate_chunk_time * (reconstripes.size() + migstripes.size()) < repair_chunk_time) 
            rg->setRepairTime(migrate_chunk_time * (reconstripes.size() + migstripes.size()));
        else
            rg->setRepairTime(repair_chunk_time);

        toret.push_back(rg);
    }
    return toret;
}

double Coordinator::getChunkMigrationTime() {
    // for case of pipelined repair
    double migrate_chunk_time;
    int packet_num;

    size_t _chunk_size = _conf->_chunk_size / 1048576; // MB
    size_t _packet_size = _conf->_packet_size / 1048576; // MB

    packet_num = _chunk_size/_packet_size;
    
    // if transmission time > read/write time
    double dir_transit_pkt_time = _packet_size*8/(1024*_conf->_network_bandwidth);
    double rd_pkt_time = _packet_size*1.0/_conf->_disk_bandwidth;
    double wr_pkt_time = _packet_size*1.0/_conf->_disk_bandwidth;

    // determine the migration time
    if(dir_transit_pkt_time > wr_pkt_time)
        migrate_chunk_time = dir_transit_pkt_time*packet_num + wr_pkt_time + rd_pkt_time;
    else
        migrate_chunk_time = wr_pkt_time*packet_num + dir_transit_pkt_time + rd_pkt_time;

    return migrate_chunk_time; 
}


