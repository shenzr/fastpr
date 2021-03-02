#ifndef _REPAIRGROUP_HH_
#define _REPAIRGROUP_HH_

#include "include.hh" 
#include "Config.hh"
#include "ECStripe.hh"
#include "FastPRUtil.hh"
#include "RepairStripe.hh"
#include "Socket.hh"

class RepairGroup {
    public:
        vector<RepairStripe*> _reconstruction;
        vector<RepairStripe*> _migration;
        double _repair_time;

        double _reconstruction_time;
        double _migration_time;

        // for finding receiver node
        int _peer_node_num;
        int* _mark;

        RepairGroup(vector<RepairStripe*> reconstruction, vector<RepairStripe*> migration);
        void setRepairTime(double t);
        void setReconstructionTime(double t);
        void setMigrationTime(double t);
        double getRepairTime();
        double getReconstructionTime();

        void findReceiverNode(Config* conf, vector<ECStripe*> stripes, string scenario);
        void scatterReceiverNode(Config* conf, vector<ECStripe*> stripes, string scenario);
        void hotstandbyReceiverNode(Config* conf);
        int hungary(int rg_id, int cur_match_stripe, int* matrix, int matrix_start_addr, int* node_selection);

        void generateCommands(Config* conf, unordered_map<int, string>& commands, string scenario);
        void enforceCommands(Config* conf, unordered_map<int, string>& commands);
};

#endif
