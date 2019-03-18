#include "Coordinator.hh"

// read the config file
Coordinator::Coordinator(Config* conf){
    _conf = conf;
    init();
}

void Coordinator::display(int len, int width, int* array){
    for(int i=0; i<width; i++){
        for(int j=0; j<len; j++)
            cout<< array[i*len+j] << " ";
         cout << std::endl;
     }
     cout << std::endl;
}

// initialization based on the configurations in the config file
void Coordinator::init(){

    _ecK = _conf->_ecK;
    _ecN = _conf->_ecN;
    _ecM = _ecN - _ecK;
    _peer_node_num = _conf->_peer_node_num;
    _hotstandby_node_num = _conf->_hotstandby_node_num;
    _chunk_size = _conf->_chunk_size;
    _packet_size = _conf->_packet_size;
    _repair_scenario = _conf->_repair_scenario;

    // read the placement
    _placement = (int*)malloc(sizeof(int)*MAX_STRIPE_NUM*_ecN);
    memset(_placement, -1, sizeof(int)*MAX_STRIPE_NUM*_ecN);

    // extract the stripe information and establish the placement information 
    parseLog(); 

    for(int i=0; i<_stripe_num; i++){
        for(int j=0; j<_ecN; j++)
            cout << _placement[i*_ecN+j] << " ";
        cout << endl;          
    }

#if DEBUG_COORD 
    map<size_t, size_t>::const_iterator it;
    for(it=_chunkid2addr.begin(); it!=_chunkid2addr.end(); it++)
        cout << it->first << "==>" << it->second << std::endl;
#endif

}

string Coordinator::ip2Str(unsigned int ip) const {

    string retVal;

    retVal += to_string(ip & 0xff);
    retVal += '.';
    
    retVal += to_string((ip >> 8) & 0xff);
    retVal +='.';

    retVal += to_string((ip >> 16) & 0xff);
    retVal +='.';

    retVal += to_string((ip >> 24) & 0xff);
    return retVal;
}

// hungary matching algorithm 
int Coordinator::hungary(int rg_id, int cur_match_stripe, int* matrix, int matrix_start_addr, int* node_selection){

    int i; 
    int ret;
    for(i=0; i<_peer_node_num; i++){
        if((matrix[matrix_start_addr + cur_match_stripe*_peer_node_num + i]==1) && (_mark[i]==0)){
 
            _mark[i]=1; //this node is now under checking

            if(node_selection[rg_id*_peer_node_num+i]==-1){             
                node_selection[rg_id*_peer_node_num+i]=cur_match_stripe;    
                return 1;
            }
            else if ((ret=hungary(rg_id, node_selection[rg_id*_peer_node_num+i], matrix, matrix_start_addr, node_selection))==1){
                node_selection[rg_id*_peer_node_num+i] = cur_match_stripe;
                return 1;
            }
        }
    }
    return 0;
}

// check if the stripe can be inserted into a given repair group 
int Coordinator::if_insert(int stripe_id, int rg_id, int cur_match_stripe, int soon_to_fail_node){

    int chunk_id;
    int k;
    int node_id;
    int ret;

    int* bak_node_belong=(int*)malloc(sizeof(int)*_peer_node_num);
    // update the bipartite matrix, we first update the k retrieval chunks 
    for(k=0; k<_ecK+_ecM; k++){

        node_id=_placement[stripe_id*(_ecK+_ecM)+k];
        if(node_id==soon_to_fail_node)
            continue;

        _bipartite_matrix[rg_id*_num_stripes_per_group*_peer_node_num+cur_match_stripe*_peer_node_num+node_id]=1;
    }

    // copy the _node_belong
    for(k=0; k<_peer_node_num; k++)
        bak_node_belong[k]=_node_belong[rg_id*_peer_node_num+k];

    // use hungary algorithm
    for(chunk_id=0; chunk_id<_ecK; chunk_id++){ // we do not consider the conflict of storage I/O
        memset(_mark, 0, sizeof(int)*_peer_node_num);   
        ret=hungary(rg_id, cur_match_stripe, _bipartite_matrix, rg_id*_num_stripes_per_group*_peer_node_num, _node_belong);

        // if we cannot find the matching
        if(ret==0)
            break;
    }
    
    // if the stripe cannot be inserted into the repair group
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

// calculate the number of stripes in a repair group
int Coordinator::cal_stripes_in_rg(int len, int* array, int value){

    int cnt=0;
    for(int i=0; i<len; i++)
        if(*(array+i)!=value)
            cnt++;
    return cnt;
}


// update the bipartite graph 
void Coordinator::update_bipartite_for_replace(int stripe_id, int rg_id, int index_in_rg, int flag, int soon_to_fail_node){
 
    int k; 
    int node_id;
    int bi_value;

    if(flag==DELETE)
        bi_value=0;
    else 
        bi_value=1;
    
    for(k=0; k<_ecK+_ecM; k++){

        node_id = _placement[stripe_id*(_ecK+_ecM)+k];
        if(node_id == soon_to_fail_node)
           continue;

        _bipartite_matrix[rg_id*_num_stripes_per_group*_peer_node_num+index_in_rg*_peer_node_num+node_id]=bi_value;
 
    }
}

// return benefit 
int Coordinator::replace(int src_id, int des_id, int rg_id, int* addi_id, int num_related_stripes, int flag, int soon_to_fail_node){

    int src_stripe_id, des_stripe_id;
    int stripe_id;
    int i;
    int j;
    int index;
    int benefit_cnt;
    int* bakp_node_belong=NULL; 

    // establish the index of des_id in the _RepairGroup
    for(i=0; i<_cur_matching_stripe[rg_id]; i++)
        if(_RepairGroup[rg_id*_num_stripes_per_group+i]==des_id)
            break;

    // delete the information of the des_id-th stripe in _bipartite_matrix
    index=i;
    des_stripe_id=_related_stripes[des_id];
    update_bipartite_for_replace(des_stripe_id, rg_id, index, DELETE, soon_to_fail_node);

    if(flag==TEST_REPLACE){
        bakp_node_belong=(int*)malloc(sizeof(int)*_peer_node_num);
        memcpy(bakp_node_belong, _node_belong+rg_id*_peer_node_num, sizeof(int)*_peer_node_num);
    }

    // update the _node_belong information
    for(i=0; i<_peer_node_num; i++)
        if(_node_belong[rg_id*_peer_node_num+i]==index)
            _node_belong[rg_id*_peer_node_num+i]=-1;

    // add the information of the src_id-th stripes in the _bipartite_matrix 
    src_stripe_id = _related_stripes[src_id];

    // check if the stripe can be inserted into the stripe 
    int ret=if_insert(src_stripe_id, rg_id, index, soon_to_fail_node);

    if(flag==TEST_REPLACE){
        if(ret == 0 ){
            
            update_bipartite_for_replace(des_stripe_id, rg_id, index, ADD, soon_to_fail_node);
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

            ret=if_insert(stripe_id, rg_id, cur_stripe_num, soon_to_fail_node);
            if(ret == 1){
                
                benefit_cnt++;
                cur_stripe_num++;
                // record the additional stripe id that can be inserted 
                j=0;
                while(addi_id[j]!=-1) j++;
                addi_id[j]=i;
                
                }

            if(cur_stripe_num==_num_stripes_per_group){

                //printf("rg_id = %d, RG FULL\n", rg_id);
                break;

                }
            }

        // reset the _bipartite_matrix and _node_belong
        for(i=rg_id*_num_stripes_per_group*_peer_node_num; i<(rg_id+1)*_num_stripes_per_group*_peer_node_num; i++)
            _bipartite_matrix[i]=0;
        
        for(i=0; i<_cur_matching_stripe[rg_id]; i++){

            stripe_id=_RepairGroup[rg_id*_num_stripes_per_group+i];
            update_bipartite_for_replace(_related_stripes[stripe_id], rg_id, i, ADD, soon_to_fail_node);

        }

        memcpy(_node_belong+rg_id*_peer_node_num, bakp_node_belong, sizeof(int)*_peer_node_num);

        free(bakp_node_belong);
        return benefit_cnt;
    }

    else if(flag==PERFORM_REPLACE){

        _ifselect[src_id]=1;
        _ifselect[des_id]=0;

        // update _RepairGroup 
        i=0;
        while(_RepairGroup[rg_id*_num_stripes_per_group+i]!=des_id) i++;
        _RepairGroup[rg_id*_num_stripes_per_group+i]=src_id;

        i=0;
        while(_record_stripe_id[i]!=-1 && i<_num_stripes_per_group){

            ret=if_insert(_related_stripes[_record_stripe_id[i]], rg_id, _cur_matching_stripe[rg_id], soon_to_fail_node);

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

int Coordinator::greedy_replacement(int num_related_stripes, int soon_to_fail_node, int rg_id){

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

            ret = replace(src_id, des_id, rg_id, addi_id, num_related_stripes, TEST_REPLACE, soon_to_fail_node);
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

    // perform replacement
    if(max_benefit!=-1)
        ret=replace(best_src_id, best_des_id, rg_id, addi_id, num_related_stripes, PERFORM_REPLACE, soon_to_fail_node);

    else 
        if_benefit = 0;

    free(addi_id);

#if DEBUG_COORD
    for(i=0; i<_rg_num; i++){
        for(int j=0; j<_num_stripes_per_group; j++)
             cout << _RepairGroup[i*_num_stripes_per_group+j] << " ";
         cout << std::endl;
    }
#endif
     return if_benefit;
}

// get an initial solution of repair groups
int Coordinator::fastpr_establish_rg(int num_related_stripes, int soon_to_fail_node){

    int i;
    int stripe_id;
    int ret;
   
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
            ret = if_insert(stripe_id, rg_index, _cur_matching_stripe[rg_index], soon_to_fail_node);

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

        //cout << "Init Stripe Solution:" <<  endl;
        //for(i=0; i<_num_stripes_per_group; i++)
        //     cout << _RepairGroup[rg_index*_num_stripes_per_group+i] << " ";
        // cout << endl;    

        // optimize that solution
        ret = 1;
        while(ret)
            ret = greedy_replacement(num_related_stripes, soon_to_fail_node, rg_index);

        //cout << "Optimized Stripe Solution:" << endl;
        //for(i=0; i<_num_stripes_per_group; i++)
        //     cout << _RepairGroup[rg_index*_num_stripes_per_group+i] << " ";
        // cout << std::endl;    

        rg_index++;

        assert(rg_index!=_rg_num);
    }

    // cout << "Initial Repair Groups:" << std::endl;
    //for(i=0; i<_rg_num; i++){
    //    for(j=0; j<_num_stripes_per_group; j++)
    //         cout << _RepairGroup[i*_num_stripes_per_group+j] << " ";
    //     cout << std::endl;
    //}

    printf("+++ Init: Num_Repair_Chunk = %d\n", cal_stripes_in_rg(_num_stripes_per_group*_rg_num, _RepairGroup, -1));

    free(select_stripe);
    return rg_index;
}

/* sort the array and move the corresponding index to the new position */
void Coordinator::QuickSort_index(int* data, int* index,int left, int right){
    int temp = data[left];
    int p = left;
    int temp_value=index[left];
    int i = left, j = right;

    while (i <= j){
        while (data[j] <= temp && j >= p)
            j--;
        if(j >= p) {
            data[p] = data[j];
            index[p]=index[j];
            p = j;
        }

        while (data[i] >= temp && i <= p)
            i++;
        if (i <= p){
            data[p] = data[i];
            index[p]=index[i];
            p = i;
        }
    }

    data[p] = temp;
    index[p]=temp_value;

    if(p - left > 1)
        QuickSort_index(data, index, left, p-1);
    if(right - p > 1)
        QuickSort_index(data, index, p+1, right);

}

// check if the number of chunks in the STF node is more than the number of chunks to be repaired 
void Coordinator::preprocess(int soon_to_fail_node, int specified_repair_num){

     cout << "----enter: preprocess" << std::endl;
    // determine the number of repaired chunks  
    int i,j; 
    int index;

    _num_rebuilt_chunks=0;  
    for(i=0; i<_stripe_num; i++){
        for(j=0; j<_ecN; j++)
            if(_placement[i*_ecN+j]==soon_to_fail_node){
                _num_rebuilt_chunks++;
                break;
          }
    }

    cout << "real: _num_rebuilt_chunk = " << _num_rebuilt_chunks << endl;
    // ensure that every test repairs the same number of chunks
    if(_num_rebuilt_chunks < specified_repair_num){
        cout << "ERR: not enough number of chunks to be repaired." << endl;
        cout << "specified_repair_num = " << specified_repair_num << ", real_lost_num = " << _num_rebuilt_chunks << endl;
        exit(1);
    }
    else
        _num_rebuilt_chunks = specified_repair_num;

    cout << "specified: _num_rebuilt_chunk = " << _num_rebuilt_chunks << endl;

    index=0;
    _related_stripes=(int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    for(i=0; i<_stripe_num; i++){
        for(j=0; j<_ecN; j++){  
            if(_placement[i*_ecN+j]==soon_to_fail_node){
                _related_stripes[index++]=i;
                break;
                }
            }
        if(index == _num_rebuilt_chunks)
            break;
        }
}

// use fastpr to repair the chunks. 
int Coordinator::FastPRRepair(int soon_to_fail_node){

    if(_repair_scenario == "scatteredRepair")
        _num_stripes_per_group=(int)(floor((_peer_node_num-1)*1.0/_ecK));
    else
        _num_stripes_per_group = ((_peer_node_num-1)/_ecK)/(_hotstandby_node_num-1)*(_hotstandby_node_num-1); 

    if(_num_stripes_per_group == 0)
    _num_stripes_per_group = (_peer_node_num-1)/_ecK;

    cout << "_repair_scenario = " << _repair_scenario << endl;
    cout << "_num_stripes_repaired_per_group" << _num_stripes_per_group << endl;

    if(_num_rebuilt_chunks/_num_stripes_per_group == 0)
        _rg_num = expand_ratio;
    else 
    _rg_num = (int)(ceil(_num_rebuilt_chunks/_num_stripes_per_group))*expand_ratio;

    // init pointers
    _mark=(int*)malloc(sizeof(int)*_peer_node_num);
    _RepairGroup=(int*)malloc(sizeof(int)*_rg_num*_num_stripes_per_group); // record the stripe id in each rg
    _bipartite_matrix=(int*)malloc(sizeof(int)*_rg_num*_num_stripes_per_group*_peer_node_num);
    _node_belong=(int*)malloc(sizeof(int)*_rg_num*_peer_node_num);
    _ifselect=(int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    _cur_matching_stripe=(int*)malloc(sizeof(int)*_rg_num); // recording the number of matching stripes in each rg
    _record_stripe_id=(int*)malloc(sizeof(int)*_num_stripes_per_group);

    int ret = fastpr_establish_rg(_num_rebuilt_chunks, soon_to_fail_node);
 
     cout << "after:_cur_matching_stripe" << std::endl;
    for(int i=0; i<_rg_num; i++)
         cout << _cur_matching_stripe[i] << " ";
     cout << std::endl;

    cout << "Sender_nodes: node_belong: " << endl;
    for(int i=0; i<_rg_num; i++){
        for(int j=0; j<_peer_node_num; j++)
             cout << _node_belong[i*_peer_node_num+j] << " ";
         cout << std::endl;
    }

    return ret;
}

// calculate the number of chunks to be migrated based on the mathematical model 
int Coordinator::calMigrateChunkNum(int num_repair_chunk){

    int num_repair_chunk_node;
    if(_repair_scenario == "scatteredRepair")
        num_repair_chunk_node = 1;
    else 
        num_repair_chunk_node = ceil(num_repair_chunk*1.0/(_hotstandby_node_num-1));

    cout << "num_repair_chunk_node = " << num_repair_chunk_node << endl;
    // for case of pipelined repair
    double repair_chunk_time;
    double migrate_chunk_time;
    int packet_num;

    packet_num = _chunk_size/_packet_size;
    // if transmission time > read/write time
    double ec_transmit_pkt_time = _packet_size*_ecK*num_repair_chunk_node*8/(1024*_conf->_network_bandwidth);
    double dir_transit_pkt_time = _packet_size*8/(1024*_conf->_network_bandwidth);
    double rd_pkt_time = _packet_size*1.0/_conf->_disk_bandwidth;
    double wr_pkt_time = _packet_size*1.0/_conf->_disk_bandwidth;
 
    // determine the repair time
    if(ec_transmit_pkt_time > wr_pkt_time)
        repair_chunk_time = ec_transmit_pkt_time*packet_num + wr_pkt_time*num_repair_chunk_node + rd_pkt_time;
    else 
        repair_chunk_time = wr_pkt_time*packet_num*num_repair_chunk_node + ec_transmit_pkt_time + rd_pkt_time;

    // determine the migration time
    if(dir_transit_pkt_time > wr_pkt_time)
        migrate_chunk_time = dir_transit_pkt_time*packet_num + wr_pkt_time + rd_pkt_time;
    else
        migrate_chunk_time = wr_pkt_time*packet_num + dir_transit_pkt_time + rd_pkt_time;

    cout << "repair_chunk_time = " << repair_chunk_time << endl;
    cout << "migrate_chunk_time = " << migrate_chunk_time << endl;
    
    int num_least_migrate_chunk = repair_chunk_time/migrate_chunk_time;
    double normal_aver_time = repair_chunk_time/(num_repair_chunk+num_least_migrate_chunk);
    double pref_mgrt_aver_time = (num_least_migrate_chunk+1)*migrate_chunk_time/(num_repair_chunk+num_least_migrate_chunk+1);

    if(normal_aver_time < pref_mgrt_aver_time) return num_least_migrate_chunk;
    else return num_least_migrate_chunk+1;

}

// find the receive nodes
void Coordinator::findReceiverNode(int* receiver_node, int* stripe_id, int num_repair_chunk, int num_migrate_chunk){

    int i;
    int j;
    int stripe;
    int ret;
    int* distribution = (int*)malloc(sizeof(int)*(num_repair_chunk+num_migrate_chunk)*_peer_node_num);
    int* receiver_node_dist=(int*)malloc(sizeof(int)*_peer_node_num);
    memset(receiver_node_dist, -1, sizeof(int)*_peer_node_num);
    
    for(i=0; i<(num_repair_chunk+num_migrate_chunk)*_peer_node_num; i++)
        distribution[i]=1;

    // establish the candidate nodes 
    for(i=0; i<num_repair_chunk+num_migrate_chunk; i++){
        if(stripe_id[i] == -1)
            continue;
        stripe = _related_stripes[stripe_id[i]];
        // read the distribution 
        for(j=0; j<_ecN; j++)
            distribution[i*_peer_node_num + _placement[stripe*_ecN+j]] = 0;
    }
    
    // find the receiver node distribution
    for(i=0; i<num_repair_chunk+num_migrate_chunk; i++){
        memset(_mark, 0, sizeof(int)*_peer_node_num);
        ret = hungary(0, i, distribution, 0, receiver_node_dist);
        if(ret!=1){
             cout << "ERR: receiver matching fails" << std::endl;
            exit(1);
        }
    }

    for(i=0; i<_peer_node_num; i++){
        if(receiver_node_dist[i]==-1)
            continue;
        receiver_node[receiver_node_dist[i]] = i;
    }

    //cout<< "distribution:" << std::endl;
    //display(_peer_node_num, num_repair_chunk+num_migrate_chunk, distribution);
    //cout<< "receiver_node_dist:" << std::endl;
    //display(_peer_node_num, 1, receiver_node_dist);
    //cout<< "receiver_node" << std::endl;
    //display(num_repair_chunk+num_migrate_chunk, 1, receiver_node);

    free(distribution);
    free(receiver_node_dist);
}

// get decoding coefficient 
int* Coordinator::getDecodeCoeff(int* remain_chunks, int* complete_enc_mat, int repair_chunk_id){

    int i;
    int remain_chunk_id;
    cout << "complete_matrix:" << std::endl;
    display(_ecK, _ecN, complete_enc_mat);

    //get remaining matrix
    int* remain_mat = (int*)malloc(sizeof(int)*_ecK*_ecK);
    for(i=0; i<_ecK; i++){
        remain_chunk_id = remain_chunks[i];
        memcpy((char*)remain_mat + i*_ecK*sizeof(int), (char*)complete_enc_mat + remain_chunk_id*_ecK*sizeof(int), sizeof(int)*_ecK);
    }

    cout << "remain_matrix:" << std::endl;
    display(_ecK, _ecK, remain_mat);
    
    //get inverted matrix
    int *invert_mat = (int*)malloc(sizeof(int)*_ecK*_ecK);
    jerasure_invert_matrix(remain_mat, invert_mat, _ecK, 8);
   
    //get coeff 
    return jerasure_matrix_multiply(complete_enc_mat + repair_chunk_id*_ecK, invert_mat, 1, _ecK, _ecK, _ecK, 8);
}


// the command format: 
// for PeerNode: num-of-commands (1 byte) + num*commands[role-receiver/sender (2 bytes) + blk_name (24 bytes) + stripe_name (4bytes) + decoding_coeff (4 bytes) + next_ip_addr (9 bytes)]
// for HotStandbyNode: num-of-commands (1 byte) + num*commands[role-receiver (2 bytes) + blk_name_to_repair (24 bytes) + stripe_name (4 byte) + decoding_coeff (4bytes) + next_ip_addr (9byte) + _ecK*SenderIPs (_ecK*9 bytes)]
string Coordinator::initCommand(int* stripe_id_array, int receiver_node_id, int global_chunk_id, int coeff, int flag){

    int i;
    string str; 
    // find the address of the read chunk 
    map<size_t, string>::iterator it;
    it = _chunkid2addr.find(global_chunk_id);
    string chunk_name = it->second;
   
    for(i=chunk_name.length(); i<BLK_NAME_LEN; i++)
        str += "0"; 
    str += chunk_name;

    if(chunk_name.length() > BLK_NAME_LEN){
        cout << "ERR: BLK_NAME_LEN is small" << endl;
        exit(1);
    }

    // read the stripe name 
    map<string,string>::iterator tmp_it;
    tmp_it = _blkName2stripeName.find(chunk_name);
    string stripe_name = tmp_it->second;

    for(i=stripe_name.length(); i<STRIPE_NAME_LEN; i++)
        str += "0";
    str += stripe_name;

    // cout << "+stripe_name, str = " << str << std::endl;
    // cout << "+stripe_name = " << stripe_name << std::endl;
    // cout << "+stripe_name_length = " << stripe_name.length() << std::endl;

    if(flag == RECEIVER){
        for(i=str.length()+ROLE_LEN; i<CMD_LEN; i++)
            str += "0";
        //cout << "receiver init_cmd str = " << str << endl;
        return str;
    }  
    // read the decoding coeff
    cout << "recv_node_id = " << receiver_node_id << endl;
    cout << "coeff = " << coeff << std::endl;
    // cout << "coeff-len = " << to_string(coeff).length() << std::endl;

    if(flag == STF){
        for(i=0; i<COEFF_LEN; i++)
            str += "0";
    }
    
    else{
        assert(to_string(coeff).length()<4);
        for(i=0; i<COEFF_LEN-(int)to_string(coeff).length(); i++)
            str += '0';
        str += to_string(coeff);
    }

    if(receiver_node_id >= _peer_node_num)
         cout << "next_ip = " << ip2Str(_conf->_hotStandbyNodeIPs[receiver_node_id-_peer_node_num]) << std::endl;
    else
         cout << "next_ip = " << ip2Str(_conf->_peerNodeIPs[receiver_node_id]) << std::endl;

    // read the next ip
    string next_ip_str;
    if(receiver_node_id < _peer_node_num)
        next_ip_str = to_string(_conf->_peerNodeIPs[receiver_node_id]);
    else
        next_ip_str = to_string(_conf->_hotStandbyNodeIPs[receiver_node_id-_peer_node_num]);
    
    //cout << "next_ip_str = " << next_ip_str << endl;
    if(next_ip_str.length() > NEXT_IP_LEN){
        cout << "ERR: NEXT_IP_LEN is small" << endl;
        exit(1);
    }

    for(i=next_ip_str.length(); i<NEXT_IP_LEN; i++)
        str += "0";
    str += next_ip_str;

    return str;
}

// send command to peernode and receive commit
void Coordinator::sendCommandRecvCommit(int* stripe_id_rg, int* node_belong_info, int* receiver_node, int* complete_matrix, int num_repair_chunk, int num_migrate_chunk, int repair_rg_id, int soon_to_fail_node, int flag){

    cout << "---init commands---" << endl;
    // initial commands and send them to the sender nodes (multi-thread)
    int i,j,k;
    int local_stripe_id;
    int global_stripe_id;
    int lost_global_chunk_id;
    int chunk_cnt;
    int recv_node_id, send_node_id;

    int total_node_num;
    if(_repair_scenario == "scatteredRepair")
        total_node_num = _peer_node_num;
    else 
        total_node_num = _peer_node_num + _hotstandby_node_num;

    string node_cmd[total_node_num];

    int mark_if_join[total_node_num];
    memset(mark_if_join, 0, sizeof(int)*total_node_num);

    int* select_k_chunk = (int*)malloc(sizeof(int)*_ecK);
    int* select_k_nodes = (int*)malloc(sizeof(int)*_ecK*num_repair_chunk);
    int* decode_coeff;
 
    int recv_node_num;
    map<int, string> stripe2senderIP;

    // fill the receiver info
    // MR: migration receiver in scatterRepair/hotStandbyRepair
    // MS: migration sender in scatteredRepair/hotStandbyRepair
    // RR: repair repairer in scatteredRepair only
    // RS: repair sender in scatteredRepair/hotStandbyRepair
    // TS: metadata sender in scatteredRepair/hotStandbyRepair
    // TR: metadata receiver in scatteredRepair/hotStandbyRepair
    // HR: hotstandby repairer only 
    for(i=0; i<num_repair_chunk+num_migrate_chunk; i++){
        int repair_stripe = stripe_id_rg[i];
        if(repair_stripe == -1)
            continue;
        recv_node_id = receiver_node[i];
        mark_if_join[recv_node_id]++;
    }

    recv_node_num=0;
    for(i=0; i<total_node_num; i++)
        if(mark_if_join[i])
            recv_node_num++;

    for(i=0; i<num_repair_chunk+num_migrate_chunk; i++){
        recv_node_id = receiver_node[i];
        // add the number of commands received by a receiver node
        if(node_cmd[recv_node_id].length() == 0 && mark_if_join[recv_node_id]>0){
            char tmp = '0';
            tmp += mark_if_join[recv_node_id];
            node_cmd[recv_node_id].push_back(tmp);
        } 
    }

    // fill the sender info
    for(i=0; i<num_repair_chunk + num_migrate_chunk; i++){

        // for the sender nodes 
        local_stripe_id = stripe_id_rg[i];
        if(local_stripe_id == -1)
            continue;

        global_stripe_id = _related_stripes[local_stripe_id];

        // init the migration commands for metadata to STF
        if(flag == META_COMMANDS){
           
            j=0; 
            while(_placement[global_stripe_id*_ecN+j]!=soon_to_fail_node) j++;
            
            mark_if_join[soon_to_fail_node]++;
             
            int cnt = 0;
            if(node_cmd[soon_to_fail_node].length() == 0){
                for(int p=0; p<num_repair_chunk+num_migrate_chunk;p++)
                    if(stripe_id_rg[p]!=-1)
                        cnt++;
                char tmp = '0';
                tmp += cnt;
                node_cmd[soon_to_fail_node].push_back(tmp);
            }
            node_cmd[soon_to_fail_node] += "TS";
            string temp_str = initCommand(stripe_id_rg, receiver_node[i], global_stripe_id*_ecN+j, -1, STF);
            node_cmd[soon_to_fail_node] += temp_str;

            continue;
        }

       if(i<num_repair_chunk && flag == DATA_COMMANDS){

           // get the remain k chunk_idx in stripe for recovery
           memset(select_k_chunk, -1, sizeof(int)*_ecK); 
           chunk_cnt=0;
           
           for(j=0; j<_peer_node_num; j++){
               if(node_belong_info[repair_rg_id*_peer_node_num + j] == i){
                   for(k=0; k<_ecN; k++)
                       if(_placement[global_stripe_id*_ecN + k] == j)
                           break;
                   
                   select_k_chunk[chunk_cnt] = k;
                   select_k_nodes[i*_ecK + chunk_cnt] = j;
                   chunk_cnt ++;
               }
               if(chunk_cnt == _ecK)
                   break;
            }
            assert(chunk_cnt == _ecK);
            // get the repaired chunk i
            j=0;
            while(_placement[global_stripe_id*_ecN+j]!=soon_to_fail_node) j++;
            //cout << "num_repair_chunk = " << num_repair_chunk << endl;
            //cout << "global_stripe_id = " << global_stripe_id << endl;
            //cout << "j = " << j << endl;
            //cout << "node_belong_info = " << endl;
            //for(int p =0; p<_peer_node_num; p++)
            //    cout << node_belong_info[repair_rg_id*_peer_node_num+p] << " ";
            //cout << endl;
            cout << "select_k_chunk = " << endl;
            display(_ecK, 1, select_k_chunk);
            // get the decoding coeffi
            decode_coeff = getDecodeCoeff(select_k_chunk, complete_matrix, j);
            for(j=0; j<_ecK; j++){
                string temp_str = initCommand(stripe_id_rg, receiver_node[i], global_stripe_id*_ecN+select_k_chunk[j], decode_coeff[j], REPAIR_SENDER);
                send_node_id = select_k_nodes[i*_ecK+j];
                mark_if_join[send_node_id]=1;

                if(node_cmd[send_node_id].length() == 0)
                    node_cmd[send_node_id] += "1";
                else 
                    node_cmd[send_node_id].at(0) = '2';
                node_cmd[send_node_id] += "RS";
                node_cmd[send_node_id] += temp_str;
               
                // for hot-standby repair, we need to add the sender ip in the commands
                if(_repair_scenario == "hotStandbyRepair"){

                    string add_ip_str;
                    int cnt; 
                    if((cnt = stripe2senderIP.count(global_stripe_id)) == 0)
                        add_ip_str = "";

                    else{
                        map<int,string>::iterator it; 
                        it = stripe2senderIP.find(global_stripe_id);
                        add_ip_str = it->second;
                    }
                    string tmp_ip_str;
                    for(int p=to_string(_conf->_peerNodeIPs[send_node_id]).length(); p<NEXT_IP_LEN; p++)
                        tmp_ip_str += "0";

                    tmp_ip_str += to_string(_conf->_peerNodeIPs[send_node_id]);

                    add_ip_str += tmp_ip_str;                        
                    if(cnt == 0)
                        stripe2senderIP.insert(pair<int, string>(global_stripe_id, add_ip_str));
                    else 
                        stripe2senderIP[global_stripe_id] = add_ip_str;

                    //node_cmd[recv_node_id] += to_string(_conf->_peerNodeIPs[send_node_id]);
                }
            }
            free(decode_coeff);
        }
        // for the migrated chunks
        else if(i>=num_repair_chunk && flag == DATA_COMMANDS){

            mark_if_join[soon_to_fail_node] = 1;
            // find the chunk index for addressing the read data on the STF
            for(j=0; j<_ecN; j++)
                if(_placement[global_stripe_id*_ecN+j] == soon_to_fail_node)
                    break;

            string temp_str = initCommand(stripe_id_rg, receiver_node[i], global_stripe_id*_ecN+j, -1, STF);
          
            if(node_cmd[soon_to_fail_node].length() == 0){
                char tmp = '0';
                tmp += num_migrate_chunk;
                node_cmd[soon_to_fail_node].push_back(tmp); 
            }
            node_cmd[soon_to_fail_node] += "MS";
            node_cmd[soon_to_fail_node] += temp_str;
        }
    } 

    // for the receiver node
    for(i=0; i<num_repair_chunk+num_migrate_chunk; i++){

        int repair_stripe  = stripe_id_rg[i];
        if(repair_stripe == -1)
            continue;
        
        recv_node_id = receiver_node[i]; 
        // for scattered repair or the migration receiver node in hot-standby repair, they use the same command format
        if((_repair_scenario == "scatteredRepair") || (i>=num_repair_chunk)){

            if(flag == DATA_COMMANDS){           
                if(i<num_repair_chunk)
                    node_cmd[recv_node_id] += "RR";
                else
                    node_cmd[recv_node_id] += "MR";
            }

            else{
                node_cmd[recv_node_id] += "TR";
            }
        }

        // for the repair nodes in the hot-standby repair
        else if((_repair_scenario == "hotStandbyRepair") && (i<num_repair_chunk)){

            if(flag == DATA_COMMANDS)
                node_cmd[recv_node_id] += "HR";
            else // for metadata
                node_cmd[recv_node_id] += "TR";
        }

        // complement the cmds by adding stripe information (for both)
        local_stripe_id = stripe_id_rg[i];
        global_stripe_id = _related_stripes[local_stripe_id];

        for(j=0; j<_ecN; j++)
            if(_placement[global_stripe_id*_ecN+j] == soon_to_fail_node)
                break;

        lost_global_chunk_id = global_stripe_id*_ecN+j;
        node_cmd[recv_node_id] += initCommand(NULL, -1, lost_global_chunk_id, -1, RECEIVER);
        
        if(stripe2senderIP.count(global_stripe_id) == 0)
            continue;

        map<int, string>::iterator it;
        it = stripe2senderIP.find(global_stripe_id);
        node_cmd[recv_node_id] += it->second;  
    }

    for(i=0; i<_peer_node_num; i++){
        if(mark_if_join[i] >0 && node_cmd[i].length() < CMD_LEN){
            for(j=node_cmd[i].length(); j<=CMD_LEN; j++)
                node_cmd[i] += "0";
        }
    }
    
    cout << "node_cmd:" << std::endl;   
    for(i=0; i<total_node_num; i++)
        cout << node_cmd[i] << endl;

    cout << "mark_if_join = " << endl;
    display(total_node_num, 1, mark_if_join);

    // send the command
    for(i=0; i<total_node_num; i++){
        if(mark_if_join[i] == 0)
            continue;

        Socket* sock = new Socket();
        if(i<_peer_node_num){
            //cout << " i = " << i << endl;
            //cout << " ip = " << (char*)ip2Str(_conf->_peerNodeIPs[i]).c_str() << endl;
            sock->sendData((char*)node_cmd[i].c_str(), node_cmd[i].length(), (char*)ip2Str(_conf->_peerNodeIPs[i]).c_str(), PN_RECV_CMD_PORT);
        }
        else{  
            //cout << " i = " << i << endl;
            //cout << " ip = " << (char*)ip2Str(_conf->_hotStandbyNodeIPs[i-_peer_node_num]).c_str() << endl;
            sock->sendData((char*)node_cmd[i].c_str(), node_cmd[i].length(), (char*)ip2Str(_conf->_hotStandbyNodeIPs[i-_peer_node_num]).c_str(), PN_RECV_CMD_PORT);
        }
        delete sock; 
   }

    // collect the commits
    int num_acks;
    if(_repair_scenario == "scatteredRepair"){
        num_acks = 0;
        for(i=0; i<num_repair_chunk+num_migrate_chunk; i++)
            if(stripe_id_rg[i]!=-1)
                num_acks++;
    }
    else{
        num_acks = recv_node_num;
    }

    char* recv_ack = (char*)malloc(sizeof(char)*(ACK_LEN+1)*num_acks);
    Socket* sock_recv_cmmt = new Socket();
    sock_recv_cmmt->paraRecvData(CD_RECV_ACK_PORT, ACK_LEN+1, recv_ack, num_acks, NULL, -1, -1, ACK_INFO, "");
   
    free(select_k_nodes);
    free(select_k_chunk);
    free(recv_ack);
    delete sock_recv_cmmt; 
    cout << "---end commands--- " << endl;
}

// perform the repair
void Coordinator::doProcess(int soon_to_fail_node, int real_rg_num, char* repair_method){
    
    int i;
    int num_repair_chunk;
    int num_migrate_chunk;
    int repair_rg_id;
    int migrate_rg_id;

    int* rg_index = (int*)malloc(sizeof(int)*real_rg_num);
    int* sort_cur_matching_stripe = (int*)malloc(sizeof(int)*real_rg_num);
    for(i=0; i<real_rg_num; i++)
        sort_cur_matching_stripe[i] = _cur_matching_stripe[i];
    for(i=0; i<real_rg_num; i++) 
        rg_index[i]=i;

    if(strcmp("fastpr", repair_method) == 0)
        QuickSort_index(sort_cur_matching_stripe, rg_index, 0, real_rg_num-1);

    // get the cauchy encoding matrix
    int* complete_matrix = (int*)malloc(sizeof(int)*_ecK*_ecN);
    memset(complete_matrix, 0, sizeof(int)*_ecK*_ecN);
    for(i=0; i<_ecK; i++)
        complete_matrix[i*_ecK+i]=1;

    // get the filename of the encoding matrix 
    string enc_file_name = string("metadata/rsEncMat_") + to_string(_ecN) + string("_") + to_string(_ecK);
    // cout << "enc_file_name = " << enc_file_name << endl; 

    // read encoding matrix from the rsEncMat file
    ifstream readin(enc_file_name);
    for(int i=0; i<_ecN-_ecK; i++)
        for(int j=0; j<_ecK; j++){
            readin >> complete_matrix[(i+_ecK)*_ecK+j];
        }
    readin.close();

    cout << "complete_matrix:" << endl;
    display(_ecK, _ecN, complete_matrix);
 
    // find the repair recievers and the migration receivers
    int repair_rg_index = 0;
    int migrate_rg_index = real_rg_num-1;
    int migrate_count = 0;
    int repair_count;

    int total_repair_count = 0;
    int total_migrate_count = 0;

    num_migrate_chunk = 0;

    struct timeval bg_tm, ed_tm;
    while(true){
        
        gettimeofday(&bg_tm, NULL);

        cout << "++++++++++++++++++ round " << repair_rg_index << "++++++++++++++++++++++++++" << std::endl;
        // for each round of repair 
        num_repair_chunk = sort_cur_matching_stripe[repair_rg_index];          
        repair_rg_id = rg_index[repair_rg_index];

        if(strcmp("fastpr", repair_method) == 0)
            num_migrate_chunk = calMigrateChunkNum(num_repair_chunk); 
        else if(strcmp("random", repair_method) == 0)
            num_migrate_chunk = 0;
        else if(strcmp("migration", repair_method)==0){
            num_repair_chunk = 0;
            num_migrate_chunk = 1;
            repair_rg_index = 0;
        }
        
        int remainint_chunk = _num_rebuilt_chunks - (total_migrate_count + total_repair_count);
        if( remainint_chunk <= num_migrate_chunk){
            num_migrate_chunk = remainint_chunk;
            num_repair_chunk = 0;
        }
      
        if(remainint_chunk == 0)
            break;
        
        // cout << "network bandwidth = " << _conf->_network_bandwidth << endl; 
        // cout << "num_repair_chunk = " << num_repair_chunk << ", num_migrate_chunk = " << num_migrate_chunk << std::endl; 
        // cout << "repair_rg_id = " << repair_rg_id << std::endl; 
        // read the repaired stripe id and the migrated stripe id 
        int* stripe_id_rg = (int*)malloc(sizeof(int)*(num_repair_chunk+num_migrate_chunk));
        memset(stripe_id_rg, -1, sizeof(int)*(num_migrate_chunk+num_repair_chunk));
         
        for(i=0; i<num_repair_chunk; i++){
            if(_RepairGroup[repair_rg_id*_num_stripes_per_group+i]!=-1){
                repair_count++;
            }
            stripe_id_rg[i] = _RepairGroup[repair_rg_id*_num_stripes_per_group + i];
            _RepairGroup[repair_rg_id*_num_stripes_per_group + i] = -1;
        }
        //if((repair_count) == 0 && (strcmp(repair_method, "fastpr")==0))
        //    break;

        migrate_count=0;
        while(migrate_count<num_migrate_chunk){
            
            migrate_rg_id = rg_index[migrate_rg_index];
            for(i=0; i<sort_cur_matching_stripe[migrate_rg_index]; i++){

                if(_RepairGroup[migrate_rg_id*_num_stripes_per_group+i]==-1)
                    continue;
 
                stripe_id_rg[num_repair_chunk + migrate_count] = _RepairGroup[migrate_rg_id*_num_stripes_per_group+i];
                _RepairGroup[migrate_rg_id*_num_stripes_per_group+i]=-1;
                migrate_count++;

                if(migrate_count == num_migrate_chunk)
                    break;
            }
            if(i>=sort_cur_matching_stripe[migrate_rg_index])
                migrate_rg_index--;
           
            if(migrate_rg_index < repair_rg_index)
                break;
        }

        //cout << "migrate_count = " << migrate_count << endl;
        //cout << "repair_count = " << repair_count << endl;
        printf("stripe_id_rg:\n");
        display(num_repair_chunk+migrate_count, 1, stripe_id_rg);

        // find the receiver nodes 
        int* receiver_node = (int*)malloc(sizeof(int)*(num_repair_chunk + migrate_count));
        memset(receiver_node, -1, sizeof(int)*(num_repair_chunk + migrate_count));
        
        if(_repair_scenario == "scatteredRepair")
            findReceiverNode(receiver_node, stripe_id_rg, num_repair_chunk, migrate_count);
        else{
            // we put the migrated chunks on the first hotstandby node, and use round-robin method to select the repaired chunks 
            if(strcmp(repair_method, "random")!=0){
                for(i=0; i<num_repair_chunk; i++){
                    if(stripe_id_rg[i] == -1)
                        continue;
                    receiver_node[i] = i%(_hotstandby_node_num-1) + 1 + _peer_node_num;
                }

                for(i=num_repair_chunk; i<num_repair_chunk+migrate_count; i++)
                    receiver_node[i] = 0 + _peer_node_num;
            }

            else{
                for(i=0; i<num_repair_chunk; i++){
                    if(stripe_id_rg[i] == -1)
                        continue;
                    receiver_node[i] = i%_hotstandby_node_num + _peer_node_num;
                }
            }
        }
        cout << "receiver_node = " << endl;
        display(num_repair_chunk+migrate_count, 1, receiver_node);

        // send commands for migrating and repairing data, and receive commits
         sendCommandRecvCommit(stripe_id_rg, _node_belong, receiver_node, complete_matrix, num_repair_chunk, migrate_count, repair_rg_id, soon_to_fail_node, DATA_COMMANDS);

        // send commands for migrating metadata and receive commits
        sendCommandRecvCommit(stripe_id_rg, _node_belong, receiver_node, complete_matrix, num_repair_chunk, migrate_count, repair_rg_id, soon_to_fail_node, META_COMMANDS);

        //update 
        repair_rg_index++;
        total_repair_count += num_repair_chunk;
        total_migrate_count += migrate_count; 

#if DEBUG_COORD    
         cout << "round " << repair_rg_index << std::endl;
        for(i=0; i<num_repair_chunk + num_migrate_chunk; i++)
             cout << stripe_id_rg[i] << " ";
         cout << std::endl;
#endif       
        free(stripe_id_rg);
        free(receiver_node);

        gettimeofday(&ed_tm, NULL);
        printf("round time = %.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

        if(migrate_rg_index < repair_rg_index)
            break;
    }
    
    // free the memory 
    free(complete_matrix);  
    free(rg_index);
    free(sort_cur_matching_stripe);
    free(_placement);
    free(_related_stripes);
    free(_RepairGroup);
    free(_cur_matching_stripe);
    free(_mark);

    if((strcmp(repair_method, "fastpr") == 0 )||(strcmp(repair_method, "random")==0)){
        free(_bipartite_matrix);
    free(_record_stripe_id);
        free(_node_belong);
    free(_ifselect);
    }

    cout << "Repair chunk number = " << _num_rebuilt_chunks << endl;
}

// random repair 
int Coordinator::randomRepair(int soon_to_fail_node){

    int i,j; 

    if(_repair_scenario == "scatteredRepair")
        _num_stripes_per_group=(int)(floor((_peer_node_num-1)*1.0/_ecK));
    else
        _num_stripes_per_group = (_peer_node_num-1)/(_ecK*_hotstandby_node_num)*_hotstandby_node_num; 

    if(_num_stripes_per_group == 0)
    _num_stripes_per_group = (_peer_node_num-1)/_ecK;

    if(_num_rebuilt_chunks/_num_stripes_per_group == 0) 
        _rg_num = expand_ratio;
    else
        _rg_num = (int)(ceil(_num_rebuilt_chunks/_num_stripes_per_group))*expand_ratio;
   

    // init pointers
    _mark=(int*)malloc(sizeof(int)*_peer_node_num);
    _RepairGroup=(int*)malloc(sizeof(int)*_rg_num*_num_stripes_per_group); // record the stripe id in each rg
    _bipartite_matrix=(int*)malloc(sizeof(int)*_rg_num*_num_stripes_per_group*_peer_node_num);
    _node_belong=(int*)malloc(sizeof(int)*_rg_num*_peer_node_num);
    _ifselect=(int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    _cur_matching_stripe=(int*)malloc(sizeof(int)*_rg_num); // recording the number of matching stripes in each rg
    _record_stripe_id=(int*)malloc(sizeof(int)*_num_stripes_per_group);

    int ret = fastpr_establish_rg(_num_rebuilt_chunks, soon_to_fail_node);

    cout << "randomRepair RepairGroup: " << endl;
    for(i=0; i<ret; i++){
        for(j=0; j<_cur_matching_stripe[i]; j++)
            cout << _related_stripes[_RepairGroup[i*_num_stripes_per_group+j]] << " ";
        cout << endl;
    }

    return ret;
}

// read the stripe information from the NameNode of HDFS 
void Coordinator::parseLog(){

    cout << "---parseLog" << endl;  
    // get the metadatabase by using hdfs fsck 
    // struct timeVal tv1, tv2;
    string cmdResult;
    string cmdFsck("hdfs fsck / -files -blocks -locations");
    FILE* pipe = popen(cmdFsck.c_str(), "r");
    if(!pipe) 
        cerr << "ERROR when using hdfs fsck" << endl;
    char cmdBuffer[256];
    while(!feof(pipe))
    {
        if(fgets(cmdBuffer, 256, pipe) != NULL)
        {
            cmdResult += cmdBuffer;
        }
    }
    pclose(pipe);
    cout << "Get the Metadata successfully" << endl;
    cout << cmdResult << endl;
    //the result is stored in cmdResult as a string
    
    //start to parse cmdResult
    //length of stripeID: 29
    //length of blkName: 24
    set<string> blks;    //the set of blks
    set<string> stripeSet;
    set<string> tempBlkSet;
    map<string, set<string>> blk2Stripe;
    map<string, vector<string>> recoveree;
    map<string, set<string>> stripe2Blk;
    //map<string, unsigned int> blk2Ip;
    _blk2Stripe.clear();

    _stripe_num = 0;

    string stripeId, ipAdd, blkName, stripeName;
    size_t currentPos, endPos, ipLength;
    size_t tmp_pos;

    /* Attention: as we assume that the HDFS client will create the file named /ec_test in our test, we will use the string "/ec_test" 
       as a marker to parse the output text returned by the NameNode. If you create a folder with a different name in your test, then 
       you should replace the "/ec_test" with the name of the folder used in your test */
    size_t strpPos = cmdResult.find("/ec_test");
    size_t startPos = cmdResult.find("blk_", strpPos);
    while(true){
        if(startPos == string::npos){
            break;
        }
        stripeId = cmdResult.substr(startPos, 29);
        tmp_pos = stripeId.find("_",4);
        stripeName = stripeId.substr(tmp_pos+1, 4);

        //stripeSet.insert(stripeId);
        cout << "Find the stripe: " << stripeId << endl;
        cout << "stripeName: " << stripeName << endl;
        for (int i = 0; i < _ecN; i++){
            cout << "i = " << i << endl;
            currentPos = cmdResult.find("blk_", startPos+29);
            //get the block index
            blkName = cmdResult.substr(currentPos, 24);
            cout << "blkName: " << blkName << endl;
            currentPos = cmdResult.find("[", currentPos);
            endPos = cmdResult.find(":", currentPos);
            ipLength = endPos - currentPos - 1;
            //get the ip_address of this block
            ipAdd = cmdResult.substr(currentPos + 1, ipLength);
            cout << "ip_address: " << ipAdd << endl; 
            startPos = endPos;
            //update _placement
            vector <unsigned int>::iterator find_pos = find(_conf->_peerNodeIPs.begin(), _conf->_peerNodeIPs.end(), inet_addr(ipAdd.c_str()));
            if(distance(_conf->_peerNodeIPs.begin(), find_pos) >= _peer_node_num){
                cout << "ERR: " << ipAdd << endl;
            exit(1);
            }
            _placement[_stripe_num*_ecN+i] = distance(_conf->_peerNodeIPs.begin(), find_pos);
            //update chunkid2addr
            size_t global_chunk_id = _stripe_num*_ecN+i;
            _chunkid2addr.insert(pair<size_t, string>(global_chunk_id, blkName));
            _blkName2stripeName.insert(pair<string,string>(blkName,stripeName));
            //blk2Ip.insert(make_pair(blkName, str2Ip(ipAdd)));
            blks.insert(blkName);
            tempBlkSet.insert(blkName);                 
        }
        // /ec_test is the folder created for storing erasure coded data 
        // ------------> strpPos = cmdResult.find("/ec_test", startPos);
        strpPos = cmdResult.find("BP-", startPos);
        startPos = cmdResult.find("blk_", strpPos);
        stripe2Blk.insert(make_pair(stripeId, tempBlkSet));
        tempBlkSet.clear();
        _stripe_num++;
        assert(_stripe_num < MAX_STRIPE_NUM);
    }

    cout << "placement:" << endl;
    display(_ecN, _stripe_num, _placement);
   
    cout << "global_chunk_id -> block_name" << endl;
    map<size_t, string>::const_iterator it;
   for(it=_chunkid2addr.begin(); it!=_chunkid2addr.end(); it++)
         cout << it->first << "==>" << it->second << std::endl;
}

// using migration only 
int Coordinator::simpleMigration(int soon_to_fail_node){

    _num_stripes_per_group = 1;
    _rg_num = _num_rebuilt_chunks;
   
    // init pointers
    _RepairGroup=(int*)malloc(sizeof(int)*_rg_num*_num_stripes_per_group); // record the stripe id in each rg
    _cur_matching_stripe = (int*)malloc(sizeof(int)*_rg_num);
    _mark=(int*)malloc(sizeof(int)*_peer_node_num);

    memset(_RepairGroup, -1, sizeof(int)*_rg_num*_num_stripes_per_group);
    memset(_cur_matching_stripe, 0, sizeof(int)*_rg_num);

    // find the lost chunk
    int i;
    for(i=0; i<_rg_num; i++)
        _cur_matching_stripe[i] = 1;

    for(i=0; i<_num_rebuilt_chunks; i++)
        _RepairGroup[i] = i;

    cout << "_RepairGroup:" << endl;
    display(1, _num_rebuilt_chunks, _RepairGroup);

    return _rg_num;

}
