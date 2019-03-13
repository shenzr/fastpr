#ifndef _CONFIG_HH_
#define _CONFIG_HH_

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>

#include "Util/tinyxml2.h"

#define ACK_LEN 3
#define CMD_LEN 44
#define ROLE_LEN 2
#define BLK_NAME_LEN 24
#define STRIPE_NAME_LEN 4
#define COEFF_LEN 4
#define NEXT_IP_LEN 10

// data type
#define DATA_CHUNK 0 
#define META_CHUNK 1
#define ACK_INFO  2

using namespace tinyxml2; 

class Config{
  public: 
    
    // variables
    int _ecK; 
    int _ecN;
    int _peer_node_num;
    int _hotstandby_node_num;

    size_t _chunk_size;  //in MB
    size_t _meta_size;  //in MB
    size_t _packet_size; //in MB 
    size_t _stripe_num;
    double _disk_bandwidth;
    double _network_bandwidth;

    std::string _localDataPath;
    std::string _repair_scenario;

    std::vector<unsigned int> _peerNodeIPs;
    std::vector<unsigned int> _hotStandbyNodeIPs;
    unsigned int _coordinatorIP;
    unsigned int _localIP;

    // functions
    Config(std::string confFile);
    void display();
};

#endif
