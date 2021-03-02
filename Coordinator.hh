#ifndef _COORDINATOR_HH_
#define _COORDINATOR_HH_

#include "include.hh"

#include <algorithm>
#include <fstream>
#include <iostream> 
#include <map>
#include <set>
#include <thread>
#include <unordered_map>
#include <sstream>
#include <string>

#include <assert.h>
#include <arpa/inet.h>
#include <math.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "AzureLRC.hh"
#include "AzureLRCPlus.hh"
#include "Config.hh"
#include "ECBase.hh"
#include "ECStripe.hh"
#include "ReconstructionSets.hh"
#include "RepairStripe.hh"
#include "RS.hh"

class Coordinator{
  protected: 
    Config* _conf;
    ECBase* _ec;
    
    // all the stripes
    vector<ECStripe*> _stripes;

    // init placement
    void readPlacementFromFile();

    // stripe index that contain chunks in the STF node
    vector<int> _lostStripeIndices;
    int _stfnode_idx;
    unsigned int _stfnode_ip;

    // get repair stripes
    vector<RepairStripe*> getRSRepairStripes();
    vector<RepairStripe*> getAzureLRCRepairStripes(bool local);
    vector<RepairStripe*> getAzureLRCPlusRepairStripes(bool local);

    // repair scheduling
    vector<ReconstructionSet*> naiveSorting(vector<ReconstructionSet*> reconstructionsets);
    vector<RepairGroup*> naiveRepairScheduling(vector<ReconstructionSet*> reconstructionsets, string scenario);
    vector<ReconstructionSet*> weightedSorting(vector<ReconstructionSet*> reconstructionsets, string scenario);
    vector<RepairGroup*> weightedRepairScheduling(vector<ReconstructionSet*> reconstructionsets, string scenario);

    // time to migrate a chunk
    double getChunkMigrationTime();

  public:
    Coordinator(Config*);
    void initPlacement();
    void getLostInfo(int stfnode);
    int getLostNum();

    void fastprRSRepair(string scenario);
    void randomRSRepair(string scenario);
    void migrationRSRepair(string scenario);

    void fastprAzureLRCRepair(string scenario);
    void randomAzureLRCRepair(string scenario);
    void migrationAzureLRCRepair(string scenario);

    void fastprAzureLRCPlusRepair(string scenario);
    void randomAzureLRCPlusRepair(string scenario);
    void migrationAzureLRCPlusRepair(string scenario);

};

#endif
