#include "ReconstructionSets.hh"

ReconstructionSets::ReconstructionSets(vector<RepairStripe*> collection, 
                                       int k,
                                       int n,
                                       Config* conf,
                                       string scenario) {
    _collection = collection;
    _ecK = k;
    _ecN = n;
    _conf = conf;
    _scenario = scenario;

    _peer_node_num = conf->_peer_node_num;
    _hotstandby = conf->_hotStandbyNodeIPs.size();

    if (_scenario == "scatteredRepair") {
        _num_stripes_per_group = (_peer_node_num-1)/_ecK;                      
    } else{
        _num_stripes_per_group = (_peer_node_num-1)/_ecK;

        if (_num_stripes_per_group > _hotstandby-1)
            _num_stripes_per_group = _num_stripes_per_group / (_hotstandby-1) * (_hotstandby-1);
    }

    _num_rebuilt_chunks = collection.size();

    _rg_num = (int)(ceil(_num_rebuilt_chunks*1.0/_num_stripes_per_group))*_expand_ratio;

    _related_stripes=(int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    for(int i=0; i<collection.size(); i++) {
        RepairStripe* repstripe = collection[i];
        int index = repstripe->_stripe_id;
        _related_stripes[i] = index;
    }

    _mark=(int*)malloc(sizeof(int)*_peer_node_num);
    _RepairGroup=(int*)malloc(sizeof(int)*_rg_num*_num_stripes_per_group);
    _bipartite_matrix=(int*)malloc(sizeof(int)*_rg_num*_num_stripes_per_group*_peer_node_num);
    _node_belong=(int*)malloc(sizeof(int)*_rg_num*_peer_node_num);
    _ifselect=(int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    _cur_matching_stripe=(int*)malloc(sizeof(int)*_rg_num);
    _record_stripe_id=(int*)malloc(sizeof(int)*_num_stripes_per_group);
}


vector<ReconstructionSet*> ReconstructionSets::findReconstructionSets(int soon_to_fail_node) {
    int i;
    int stripe_id;
    int ret;
    int num_related_stripes = _num_rebuilt_chunks;

    // initialization
    int* select_stripe=(int*)malloc(sizeof(int)*_num_stripes_per_group);

    memset(_RepairGroup, -1, sizeof(int)*_rg_num*_num_stripes_per_group);
    memset(_ifselect, 0, sizeof(int)*num_related_stripes);
    memset(_bipartite_matrix, 0, sizeof(int)*_num_stripes_per_group*_peer_node_num*_rg_num);
    memset(_node_belong, -1, sizeof(int)*_rg_num*_peer_node_num);
    memset(select_stripe, -1, sizeof(int)*_num_stripes_per_group);
    memset(_cur_matching_stripe, 0, sizeof(int)*_rg_num);

    int rg_index=0;
    int flag;

    while(true){
        flag = 0;
        // generate an solution for a new repair group
        for(i=0; i<num_related_stripes; i++){
            if(_ifselect[i] == 1)
                continue;

            stripe_id = _related_stripes[i];
            if (_debug)
                cout << "findReconstructionSets.stripe_id = " << stripe_id << endl;
            RepairStripe* repstripe = _collection[i];

            ret = if_insert(repstripe, rg_index, _cur_matching_stripe[rg_index], soon_to_fail_node);

            if(ret){
                _RepairGroup[rg_index*_num_stripes_per_group + _cur_matching_stripe[rg_index]] = i;
                _ifselect[i]=1;
                _cur_matching_stripe[rg_index]++;
                flag=1;
                if(_cur_matching_stripe[rg_index] == _num_stripes_per_group)
                    break;
            }
        }

        if(!flag) break;

        // optimize that solution

        ret = 1;
        while(ret)
            ret = greedy_replacement(num_related_stripes, soon_to_fail_node, rg_index);

        rg_index++;
        assert(rg_index!=_rg_num);
    }

    free(select_stripe);

    vector<ReconstructionSet*> toret = formatReconstructionSets();
    return toret;
}

vector<ReconstructionSet*> ReconstructionSets::formatReconstructionSets() {

    cout << "FastPR: Repair Groups:" << std::endl;
    for(int i=0; i<_rg_num; i++){
        for(int j=0; j<_num_stripes_per_group; j++)
            cout << _RepairGroup[i*_num_stripes_per_group+j] << " ";
        cout << std::endl;                       
    }

    vector<ReconstructionSet*> toret;
    for(int i=0; i<_rg_num; i++){
        vector<RepairStripe*> cursetstripe;
        for(int j=0; j<_num_stripes_per_group; j++){
            int idx = _RepairGroup[i*_num_stripes_per_group+j];
            cout << idx << "(";
            if (idx >= 0) {
                RepairStripe* repstripe = _collection[idx];
                cout << repstripe->_stripe_id << ") ";
                cursetstripe.push_back(repstripe);
            }
        }
        cout << endl;
        if (cursetstripe.size() > 0) {
            ReconstructionSet* curset = new ReconstructionSet(_conf, cursetstripe, _ecK, _ecN);
            toret.push_back(curset);
        }
    }

    // set src for reconstruction set
    for(int i=0; i<toret.size(); i++) {
        ReconstructionSet* rset = toret[i];
        vector<RepairStripe*> rstripes = rset->_repair_stripes;
        for (int j=0; j<_peer_node_num; j++) {
            cout << _node_belong[i * _peer_node_num + j] << " ";
            int stripeid = _node_belong[i*_peer_node_num+j];

            if (stripeid == -1)
                continue;
            int nodeid = j;
            rstripes[stripeid]->setSourceNode(nodeid);
        }
        cout << endl;
    }

    return toret;
}

int ReconstructionSets::if_insert(RepairStripe* repairstripe, int rg_id, int cur_match_stripe, int soon_to_fail_node) {
    if (_debug) {
        cout << "if_insert::stripe_id = " << repairstripe->_stripe_id << endl;
        cout << "if_insert::rg_id = " << rg_id << endl;
        cout << "if_insert::cur_match_stripe = " << cur_match_stripe << endl;
        cout << "if_insert::soon_to_fail_node = " << soon_to_fail_node << endl;
    }
    int chunk_id;
    int k;
    int node_id;
    int ret;

    int* bak_node_belong=(int*)malloc(sizeof(int)*_peer_node_num);

    // 0. get out the nodeid of chunks in the repairstripe, and try to set bipartite matrix
    vector<vector<int>> chunkinfos = repairstripe->_chunks;
    if (_debug)
        cout << "nodeids: ";
    for (auto chunkinfo: chunkinfos) {
        int chunkidx = chunkinfo[0];
        node_id = chunkinfo[1];
        if (_debug)
            cout << node_id << " ";
        if (node_id == soon_to_fail_node)
            continue;
        // set bipartite matrix
        _bipartite_matrix[rg_id*_num_stripes_per_group*_peer_node_num+cur_match_stripe*_peer_node_num+node_id]=1;
    }
    if (_debug)
        cout << endl;

    // 1. backup node_belong
    for(k=0; k<_peer_node_num; k++)
        bak_node_belong[k]=_node_belong[rg_id*_peer_node_num+k];

    // 2. use hungary algorithm
    for(chunk_id=0; chunk_id<_ecK; chunk_id++){
        if (_debug)
            cout << "check chunk" << chunk_id << endl;
        memset(_mark, 0, sizeof(int)*_peer_node_num);
        ret=hungary(rg_id, cur_match_stripe, _bipartite_matrix, rg_id*_num_stripes_per_group*_peer_node_num, _node_belong);
        if(ret==0)
            break;
    }

    // if the repair stripe cannot be inserted into the repair group
    if(chunk_id<=_ecK-1){
        // reset the bipartite matrix
        for(node_id=0; node_id<_peer_node_num; node_id++)
            _bipartite_matrix[rg_id*_num_stripes_per_group*_peer_node_num+cur_match_stripe*_peer_node_num+node_id]=0;
        // reset the node belong
        for(k=0; k<_peer_node_num; k++)
            _node_belong[rg_id*_peer_node_num+k]=bak_node_belong[k];
    }

    free(bak_node_belong);
    if(chunk_id<=_ecK-1)
        return 0;
    else
        return 1;
}

int ReconstructionSets::hungary(int rg_id, int cur_match_stripe, int* matrix, int matrix_start_addr, int* node_selection) {
    int i;
    int ret;

    for(i=0; i<_peer_node_num; i++){
        if((matrix[matrix_start_addr + cur_match_stripe*_peer_node_num + i]==1) && (_mark[i]==0)){
            if (_debug)
                cout << "hungary:checknode" << i << endl;
            _mark[i]=1;

            if (_debug)
                cout << "node_selection " << i << " = " << node_selection[rg_id*_peer_node_num+i] << endl;
            if(node_selection[rg_id*_peer_node_num+i]==-1){
                node_selection[rg_id*_peer_node_num+i]=cur_match_stripe;
                if (_debug)
                    cout << "hungary ret1.1: rg_id " << rg_id << ", mark (" << rg_id << ", " << i << ") = " << cur_match_stripe << endl;
                return 1;
            } else if ((ret=hungary(rg_id, node_selection[rg_id*_peer_node_num+i], matrix, matrix_start_addr, node_selection))==1){
                node_selection[rg_id*_peer_node_num+i] = cur_match_stripe;
                if (_debug)
                    cout << "hungary ret1.2: rg_id " << rg_id << ", mark (" << rg_id << ", " << i << ") = " << cur_match_stripe << endl;
                return 1;
            }
        }
    }
    if (_debug)
        cout << "hungary: rg_id " << rg_id << ", cur_match_stripe " << cur_match_stripe << ", return 0"<< endl;
    return 0;
}

int ReconstructionSets::greedy_replacement(int num_related_stripes, int soon_to_fail_node, int rg_id) {
    if (_debug)
        cout << "greedy_replacement.numstripes: " << num_related_stripes << ", stfnode: " << soon_to_fail_node << ", rg_id : " << rg_id << endl;
    int i;
    int best_src_id, best_des_id;
    int ret;
    int src_id;
    int des_id;
    int max_benefit;
    int if_benefit;

    int* addi_id=(int*)malloc(sizeof(int)*_num_stripes_per_group);
    best_src_id=-1;
    best_des_id=-1;
    if_benefit = 1;
    max_benefit=-1;

    memset(_record_stripe_id, -1, sizeof(int)*_num_stripes_per_group);

    if(_cur_matching_stripe[rg_id]==_num_stripes_per_group)
        return 0;
    for(src_id=0; src_id<num_related_stripes; src_id++){
        if(_ifselect[src_id]==1)
            continue;
        for(i=0; i<_cur_matching_stripe[rg_id]; i++){
            memset(addi_id, -1, sizeof(int)*_num_stripes_per_group);
            des_id=_RepairGroup[rg_id*_num_stripes_per_group+i];

            string flag = "test_replace";
            ret = replace(src_id, des_id, rg_id, addi_id, num_related_stripes, flag, soon_to_fail_node);
            if(ret == 0)
                continue;
            if(ret > 0){
                best_src_id=src_id;
                best_des_id=des_id;
                max_benefit=ret;
                memcpy(_record_stripe_id, addi_id, sizeof(int)*_num_stripes_per_group);
                break;
            }
            if(max_benefit == _num_stripes_per_group - _cur_matching_stripe[rg_id])
                break;
        }
        if(max_benefit == _num_stripes_per_group - _cur_matching_stripe[rg_id])
            break;
    }

    if (_debug)
        cout << "max_benefit = " << max_benefit << endl;

    // perform replacement
    if(max_benefit!=-1) {
        ret=replace(best_src_id, best_des_id, rg_id, addi_id, num_related_stripes, "perform_replace", soon_to_fail_node);
        if (_debug)
            cout << "replace res = " << ret << endl;
    } else
        if_benefit = 0;

    if (_debug) {
        cout << "before exit greedy_replacement" << endl;
        for(i=0; i<_rg_num; i++){
            for(int j=0; j<_num_stripes_per_group; j++)
                cout << _RepairGroup[i*_num_stripes_per_group+j] << " ";
            cout << std::endl;
            
        }
        cout << "if_benefit = " << if_benefit << endl;
    }

    free(addi_id);
    return if_benefit;
}

int ReconstructionSets::replace(int src_id, int des_id, int rg_id, int* addi_id, int num_related_stripes, string flag, int soon_to_fail_node) {
    if (_debug)
        cout << "replace src_id = " << src_id << ", des_id = " << des_id << ", rg_id = " << rg_id << endl;

    int src_stripe_id, des_stripe_id;
    int stripe_id;
    int i;
    int j;
    int index;
    int benefit_cnt;
    int* bakp_node_belong=NULL;

    // establish the index of des_id in the _RepairGroup
    for(i=0; i<_cur_matching_stripe[rg_id]; i++)
        if(_RepairGroup[rg_id*_num_stripes_per_group+i] == des_id)
            break;

    // delete the information of the des_id-th stripe in _bipartite_matrix
    index=i;
    des_stripe_id=_related_stripes[des_id];
    update_bipartite_for_replace(des_id, des_stripe_id, rg_id, index, "delete", soon_to_fail_node);

    if(flag=="test_replace"){
        bakp_node_belong=(int*)malloc(sizeof(int)*_peer_node_num);
        memcpy(bakp_node_belong, _node_belong+rg_id*_peer_node_num, sizeof(int)*_peer_node_num);                       
    }

    // update the _node_belong information
    for (i=0; i<_peer_node_num; i++)
        if (_node_belong[rg_id*_peer_node_num+i]==index)
            _node_belong[rg_id*_peer_node_num+i]=-1;

    // add the information of the src_id-th stripes in the _bipartite_matrix
    src_stripe_id = _related_stripes[src_id];
    RepairStripe* src_repair_stripe = _collection[src_id];
    
    // check if the stripe can be inserted into the stripe
    int ret=if_insert(src_repair_stripe, rg_id, index, soon_to_fail_node);

    if (flag=="test_replace") {
        if(ret == 0 ){

            update_bipartite_for_replace(des_id, des_stripe_id, rg_id, index, "add", soon_to_fail_node);
            memcpy(_node_belong+rg_id*_peer_node_num, bakp_node_belong, sizeof(int)*_peer_node_num);

            free(bakp_node_belong);
            return 0;
        }

        // calculate the benefit of the replacement
        benefit_cnt=0;
        int cur_stripe_num;
        cur_stripe_num = _cur_matching_stripe[rg_id];

        // try if other stripes that are not selected can be inserted into the RG
        for(i=src_id; i<num_related_stripes; i++){

            if(_ifselect[i]==1) continue;
            if(i==src_id) continue;

            stripe_id = _related_stripes[i];
            RepairStripe* cur_repair_stripe = _collection[i];
            ret=if_insert(cur_repair_stripe, rg_id, cur_stripe_num, soon_to_fail_node);
            if(ret == 1){

                benefit_cnt++;
                cur_stripe_num++;
                // record the additional stripe id that can be inserted
                j=0;
                while(addi_id[j]!=-1) j++;
                addi_id[j]=i;

            }

            if(cur_stripe_num==_num_stripes_per_group){


                break;
            }
        }

        // reset the _bipartite_matrix and _node_belong
        for(i=rg_id*_num_stripes_per_group*_peer_node_num; i<(rg_id+1)*_num_stripes_per_group*_peer_node_num; i++)
            _bipartite_matrix[i]=0;

        for(i=0; i<_cur_matching_stripe[rg_id]; i++){
            stripe_id=_RepairGroup[rg_id*_num_stripes_per_group+i];
            update_bipartite_for_replace(stripe_id, _related_stripes[stripe_id], rg_id, i, "add", soon_to_fail_node);
        }

        memcpy(_node_belong+rg_id*_peer_node_num, bakp_node_belong, sizeof(int)*_peer_node_num);
        free(bakp_node_belong);
        return benefit_cnt;
    } else if(flag=="perform_replace"){

        _ifselect[src_id]=1;
        _ifselect[des_id]=0;

        // update _RepairGroup
        i=0;
        while(_RepairGroup[rg_id*_num_stripes_per_group+i]!=des_id) i++;
        _RepairGroup[rg_id*_num_stripes_per_group+i]=src_id;

        i=0;
        while(_record_stripe_id[i]!=-1 && i<_num_stripes_per_group){
            RepairStripe* repstripe = _collection[_record_stripe_id[i]];
            ret=if_insert(repstripe, rg_id, _cur_matching_stripe[rg_id], soon_to_fail_node);
             
            if(ret==0){                
                printf("ERR-2: if_insert\n");
                exit(1);                                                                                      
            }
            // perform update
            _RepairGroup[rg_id*_num_stripes_per_group + _cur_matching_stripe[rg_id]] = _record_stripe_id[i];
            _cur_matching_stripe[rg_id]++;
            _ifselect[_record_stripe_id[i]]=1;
            i++;
        }
    }

    return 1;
}

void ReconstructionSets::update_bipartite_for_replace(int des_id, int stripe_id, int rg_id, int index_in_rg, string flag, int soon_to_fail_node) {
    // des_id is the index in _collection
    // stripe_id is the index in the overall stripes
    int k;
    int node_id;
    int bi_value;

    if(flag=="delete")
        bi_value=0;
    else
        bi_value=1;


    RepairStripe* repstripe = _collection[des_id];
    vector<vector<int>> chunkinfos = repstripe->_chunks;
    for (auto chunkinfo: chunkinfos) {
        int chunkidx = chunkinfo[0];
        node_id = chunkinfo[1];
        if (node_id == soon_to_fail_node)
            continue;
        // set bipartite matrix
        _bipartite_matrix[rg_id*_num_stripes_per_group*_peer_node_num+index_in_rg*_peer_node_num+node_id]=bi_value;
    }
}

