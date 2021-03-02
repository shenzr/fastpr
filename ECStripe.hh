#ifndef _ECSTRIPE_HH_
#define _ECSTRIPE_HH_

#include <unordered_map>

#include "include.hh"
#include "Config.hh"
#include "FastPRUtil.hh"

class ECStripe {

    public:
        Config* _conf;

        // the global stripe index of all the stripes
        int _stripe_id;
        // idx: chunk index in a stripe
        // value: nodeid 
        vector<int> _chunk_list;
        // map nodeid to chunk idx in a stripe
        unordered_map<int, int> _nodeid2chunk;


        string _stripe_name; // 4 bytes
        vector<string> _chunk_name_list;

        ECStripe(Config* conf, string info, int stripeid);

        bool containsNodeId(int nodeid);
        int getChunkIndex(int nodeid);
        int getChunkPlace(int chunkid);
        void dump();

};
#endif
