#ifndef _RECONSTRUCTIONSETS_HH_
#define _RECONSTRUCTIONSETS_HH_

#include <assert.h>

#include "include.hh"
#include "Config.hh"
#include "ReconstructionSet.hh"
#include "RepairStripe.hh"

class ReconstructionSets {
    private:
        // all the repair stripes for chunks in STF node
        vector<RepairStripe*> _collection;
        Config* _conf;
        string _scenario;

        bool _debug = false;

        // copy parameters from the original implementations
        // total number of nodes in the cluster
        int _peer_node_num;

        // eck number of chunks are enough to repair lost chunk 
        int _ecK;
        // ecn number of chunks in a repair stripe
        int _ecN;


        // number of hot standby node
        int _hotstandby;

        // maximum number of matches in a repair round
        int _num_stripes_per_group;

        // number of chunks in stf node
        int _num_rebuilt_chunks;

        // prepare large enough space to store repair group information
        int _rg_num;
        int _expand_ratio = 3;

        // record the original index of repair stripes into an array
        int* _related_stripes;

        int* _mark;
        int* _RepairGroup; // record the stripe id in each rg
        int* _bipartite_matrix;
        int* _node_belong;
        int* _ifselect;
        int* _cur_matching_stripe; // recording the number of matching stripes in each rg
        int* _record_stripe_id;

        int if_insert(RepairStripe* repairstripe, int rg_id, int cur_match_stripe, int soon_to_fail_node);
        int hungary(int rg_id, int cur_match_stripe, int* matrix, int matrix_start_addr, int* node_selection);
        int greedy_replacement(int num_related_stripes, int soon_to_fail_node, int rg_id);
        int replace(int src_id, int des_id, int rg_id, int* addi_id, int num_related_stripes, string flag, int soon_to_fail_node);
        void update_bipartite_for_replace(int idx, int stripe_id, int rg_id, int index_in_rg, string flag, int soon_to_fail_node);
        vector<ReconstructionSet*> formatReconstructionSets();

    public:
        ReconstructionSets(vector<RepairStripe*> collection, 
                           int k,
                           int n,
                           Config* conf,
                           string scenario);

        vector<ReconstructionSet*> findReconstructionSets(int sftnode);
};

#endif
