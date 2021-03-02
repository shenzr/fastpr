#ifndef _REPAIRSTRIPE_HH_
#define _REPAIRSTRIPE_HH_

#include "include.hh"
#include "Config.hh"
#include "ECBase.hh"
#include <unordered_map>

// In a repair stripe, k chunks out of n chunks can repair any chunks in a repair stripe
// For example: 
//   for RS (n = 9, k = 6), suppose a ECStripe (0,1,2,3,4,5,6,7,8), a repair stripe is (0,1,2,3,4,5,6,7,8);k=6;n=9; 
//   for Azure-LRC (n=10, k=6, r=3) (0,1,2,3,4,5, 6,7, 8,9)
//     local repair stripe1 (0,1,2,6);k=3;n=4;
//     local repair stripe2 (3,4,5,7);k=3;n=4;
//     global repair stripe (0,1,2,3,4,5,8,9)k;=6;n=8;


class RepairStripe {
    public:
        // stripe_id is the original stripe id in all the stripes, including healthy stripes
        int _stripe_id;
        // _chunks[i][0] = original chunk index in the ECStripe
        // _chunks[i][1] = nodeid for the chunk
        vector<vector<int>> _chunks;
        ECBase* _ec;
        // original chunkid 2 chunkname
        unordered_map<int, string> _chunk2name;
        string _stripe_name;

        // to repair chunk
        int _repair_chunk_idx;
        int _repair_nodeid;

        // src chunkid
        vector<int> _src_chunks;
        vector<int> _src_nodeid;

        // receiver nodeid
        int _receiver_nodeid;
        
        // original chunkid 2 nodeid
        unordered_map<int, int> _chunk2node;
        unordered_map<int, int> _node2chunk;

        RepairStripe(int stripe_id, vector<vector<int>> chunks, int rpnodeid, ECBase* ec, unordered_map<int, string> chunk2name, string stripename);
        void setSourceNode(int nodeid);
        void setRepairNodeId(int nodeid);
        
        void genReconstructionCommands(Config* conf, unordered_map<int, vector<string>>& commands, string scenario);
        void genMigrationCommands(Config* conf, unordered_map<int, vector<string>>& commands, string scenario);

        string genRepairSenderCommand(string chunkname, string stripename, int coeff, unsigned int nextip);
        string genRepairReceiverCommand(string chunkname, string stripename, vector<unsigned int> srclist, string scenario);
        string genMigrationSenderCommand(string chunkname, string stripename, unsigned int nextip);
        string genMigrationReceiverCommand(string chunkname, string stripename);

};
#endif
