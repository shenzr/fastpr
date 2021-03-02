#include "ReconstructionSet.hh"

ReconstructionSet::ReconstructionSet(Config* conf, vector<RepairStripe*> repair_stripes, int k, int n) {
    _conf = conf;
    _repair_stripes = repair_stripes;
    _ecK = k;
    _ecN = n;

    _scheduled = false;
}

int ReconstructionSet::size() {
    return _repair_stripes.size();
}

// keyun hard code input params
double ReconstructionSet::weight(string scenario, double t_m) {
    return (_repair_stripes.size() + getReconstructionTime(scenario) / t_m) / getReconstructionTime(scenario);
}

void ReconstructionSet::dump() {
    for (int i=0; i<_repair_stripes.size(); i++) {
        RepairStripe* repstripe = _repair_stripes[i];
    }
}

double ReconstructionSet::getReconstructionTime(string scenario) {
    int num_repair_chunk = _repair_stripes.size();
    int num_repair_chunk_node;
    int hot_standby_node_num = _conf->_hotStandbyNodeIPs.size();

    if (scenario == "scatteredRepair")
        num_repair_chunk_node = 1;
    else
        num_repair_chunk_node = (int) ceil(num_repair_chunk*1.0/hot_standby_node_num);
               
    // for case of pipelined repair
    double repair_chunk_time;
    int packet_num;
    
    size_t _chunk_size = _conf->_chunk_size / 1048576; // MB
    size_t _packet_size = _conf->_packet_size / 1048576; // MB
    packet_num = _chunk_size/_packet_size;

    
    // if transmission time > read/write time
    double ec_transmit_pkt_time = _packet_size*_ecK*8/(1024*_conf->_network_bandwidth);
    double rd_pkt_time = _packet_size*1.0/_conf->_disk_bandwidth;
    double wr_pkt_time = _packet_size*1.0/_conf->_disk_bandwidth;
                                                         
    // determine the repair time
    if(ec_transmit_pkt_time > wr_pkt_time)
        repair_chunk_time = ec_transmit_pkt_time*num_repair_chunk_node*packet_num + wr_pkt_time*num_repair_chunk_node + rd_pkt_time;
    else
        repair_chunk_time = wr_pkt_time*packet_num*num_repair_chunk_node + ec_transmit_pkt_time*num_repair_chunk_node + rd_pkt_time;
                                                                                           
    return repair_chunk_time;                                                                                                                   
}

vector<RepairStripe*> ReconstructionSet::getRepairStripes4Reconstruction() {

    _scheduled = true;
    return _repair_stripes;
}

vector<RepairStripe*> ReconstructionSet::getRepairStripes4Migration(int num, bool del) {
    vector<RepairStripe*> toret;
    
    int size = _repair_stripes.size();

    while (num > 0 && size > 0) {
        
        RepairStripe* curstripe = _repair_stripes[size-1];
        toret.push_back(curstripe);

        num--;
        size--;

        if (del) {
            _repair_stripes.pop_back();
        }
    }

    if (_repair_stripes.size() <= 0) {
        _scheduled = true;
    }

    return toret;
}
