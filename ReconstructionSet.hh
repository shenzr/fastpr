#ifndef _RECONSTRUCTIONSET_HH_
#define _RECONSTRUCITONSET_HH_

#include "include.hh"
#include "Config.hh"
#include "RepairGroup.hh"
#include "RepairStripe.hh"

class ReconstructionSet {
    public:
        Config* _conf;
        vector<RepairStripe*> _repair_stripes;
        int _ecK;
        int _ecN;
        bool _scheduled;

        int _c_m; // estimated max number of chunks that can be migrated in a repair round

        ReconstructionSet(Config* conf, vector<RepairStripe*> repair_stripes, int k, int n);
        int size();

        double weight(string scenario, double t_m);

        void dump();
        double getReconstructionTime(string scenario);
        vector<RepairStripe*> getRepairStripes4Reconstruction();
        vector<RepairStripe*> getRepairStripes4Migration(int num, bool del);
};

#endif
