#ifndef _PEERNODE_HH_
#define _PEERNODE_HH_

#include <iostream>
#include <map>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <fcntl.h>
#include <dirent.h>

#include "Socket.hh"
#include "Config.hh"

extern "C"{
#include "Jerasure/jerasure.h"
#include "Jerasure/galois.h"
#include "Jerasure/reed_sol.h"
#include "Jerasure/cauchy.h"
}

#define DEBUG_PEERNODE true

using namespace std;

class PeerNode{
  protected:
    size_t _chunk_size;
    size_t _packet_size;
    size_t _meta_size;
    int _ecK;
    unsigned int _coordinator_ip;
    string _data_path;

    string findChunkAbsPath(char*, char*);
    void partialEncodeSendData(int, char*, char*, int*);
    void readData(char*, string, int*);

  public:
    string _repair_scenario;

    PeerNode(Config*); 
    void recvData(int, int, char*, int);
    void sendData(string, int, char*, int);
    void sendData_debug(string, int, int, int);
    void paraRecvData(int, string, int);
    void parseCommand(char*, char*, char*, char*, char*, char*, char*);
    void aggrDataWriteData(string, char*, int, size_t, int*, int);
    void writeData(string, size_t, char*, size_t, int);
    string ip2Str(unsigned int);
    int getVal(string);
    void commitACK(void);
    void initChar(char**, int, int);
};
#endif
