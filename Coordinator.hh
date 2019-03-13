#ifndef _COORDINATOR_HH_
#define _COORDINATOR_HH_

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
#include <netinet/in.h>
#include <sys/socket.h>

#include "Config.hh"
#include "Socket.hh"

extern "C"{
#include "Jerasure/jerasure.h"
#include "Jerasure/galois.h"
#include "Jerasure/reed_sol.h"
#include "Jerasure/cauchy.h"
}

#define DEBUG_COORD 0
#define ADD 0
#define DELETE 1
#define TEST_REPLACE 2
#define PERFORM_REPLACE 3
#define expand_ratio 3

#define RECEIVER 0
#define REPAIR_SENDER   1
#define STF      2

#define MAX_STRIPE_NUM 1000

#define DATA_COMMANDS 0
#define META_COMMANDS 1

using namespace std;

class Coordinator{
  protected: 
    Config* _conf;
    
    int _ecK;
    int _ecM;
    int _ecN;
    int _coeffi;
    int _stripe_num;
    int _peer_node_num;
    int _hotstandby_node_num;
    size_t _chunk_size;
    size_t _packet_size;
    int _rg_num;

    int* _rsEncMat;
    int* _placement;
    int* _related_stripes;

    // for hyre
    int _num_stripes_per_group;
    string _repair_scenario;

    int* _RepairGroup;
    int* _ifselect;
    int* _bipartite_matrix;
    int* _node_belong;   
    int* _cur_matching_stripe; 
    int* _mark;
    int* _record_stripe_id;

    vector<thread> _distThrds;

    map<unsigned int, int> _ip2idx;
    map<size_t, string> _chunkid2addr; // the map between the global chunk id and the logical address on the node
    map<string, set<pair<unsigned int, string>>> _blk2Stripe;
    map<string,string> _blkName2stripeName;
    // for repair processing
    void init();
    void findReceiverNode(int*, int*, int, int);
    void sendCommandRecvCommit(int*, int*, int*, int*, int, int, int, int, int);
    string ip2Str(unsigned int) const;
   
    // for perfect matching
    void update_bipartite_for_replace(int, int, int, int, int);
    int hungary(int, int, int*, int, int*);
    int if_insert(int, int, int, int);
    int cal_stripes_in_rg(int, int*, int);
    int hyre_establish_rg(int, int);
    int replace(int, int, int, int*, int, int, int);
    int greedy_replacement(int, int, int);

    // for erasure coding 
    int* getDecodeCoeff(int*, int*, int);
    string initCommand(int*, int, int, int, int);

    // for doProcess 
    void QuickSort_index(int*, int*, int, int);
    void display(int, int, int*);
    int calMigrateChunkNum(int);    

  public:
    int _num_rebuilt_chunks;

    Coordinator(Config*);
    void doProcess(int, int, char*);
    void preprocess(int, int);
    void parseLog(void);
    int HyReRepair(int);
    int randomRepair(int);
    int simpleMigration(int);
};

#endif
