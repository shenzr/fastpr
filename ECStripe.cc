#include "ECStripe.hh"

ECStripe::ECStripe(Config* conf, string info, int stripeid) {
    _conf = conf;
    _stripe_id = stripeid;

    vector<string> splitinfo = FastPRUtil::split(info, ";");
    _stripe_name = splitinfo[0];

    int chunkid = 0;
    for (int i=1; i<splitinfo.size(); i++) {
        string blkinfo = splitinfo[i];
        vector<string> splitblkinfo = FastPRUtil::split(blkinfo, ":");

        string chunkname = splitblkinfo[0];
        unsigned int ip = inet_addr(splitblkinfo[1].c_str());
        int nodeid = conf->_peerIp2Idx[ip];

        _chunk_list.push_back(nodeid);
        _nodeid2chunk[nodeid] = chunkid++;
        _chunk_name_list.push_back(chunkname);
    }
}

bool ECStripe::containsNodeId(int nodeid) {
    if (_nodeid2chunk.find(nodeid) == _nodeid2chunk.end())
        return false;
    else
        return true;
}

int ECStripe::getChunkIndex(int nodeid) {
    return _nodeid2chunk[nodeid];    
}

int ECStripe::getChunkPlace(int chunkid) {
    return _chunk_list[chunkid];    
}

void ECStripe::dump() {
    cout << "stripename: " << _stripe_name << endl;
    for (int i=0; i<_chunk_list.size(); i++) {
        cout << _chunk_name_list[i] << ": " << to_string(_chunk_list[i]) << endl;
    }
}
