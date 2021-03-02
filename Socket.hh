#ifndef _SOCKET_HH
#define _SOCKET_HH

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

#include <string>
#include <iostream>
#include <thread>

#define DATA_CHUNK 0
#define META_CHUNK 1
#define ACK_INFO   2

#define CD_RECV_ACK_PORT  23453
#define PN_RECV_CMD_PORT  24672
#define PN_RECV_ACK_PORT  12353
#define PN_SEND_DATA_PORT 42146
#define PN_RECV_DATA_PORT 52351

using namespace std;

class Socket{

  public:
    Socket(void);
    int initClient(int);
    int initServer(int);
    void sendData(char*, size_t, char*, int);
    void recvData(int, int, char*, int, int*, int, int);
    char* recvCommand(int, size_t, size_t);
    void paraRecvData(int, int, char*, int, int*, int, int, int, string);
    char* aggrData(char*, char*, int, int, int, int);
    void calDelta(char*, char*, char*, int); 

    char* hsbAggrData(char* total_recv_data, char* repaired_data, int num_chunks, int chunk_size, int packet_id, int packet_size);
    char* recvHSBCommand(int port_num, size_t unit_size_sr, int* hsb_cmd_len);
};

#endif
