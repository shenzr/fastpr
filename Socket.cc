#include "Socket.hh"

Socket::Socket(void){
}

// for sending data 
int Socket::initClient(int port_num){

    // set client_addr infor
    struct sockaddr_in client_addr;
    bzero(&client_addr, sizeof(client_addr));
    client_addr.sin_family = AF_INET;
    client_addr.sin_addr.s_addr = htons(INADDR_ANY);
    client_addr.sin_port = htons(port_num);

    // create client socket
    int client_socket = socket(AF_INET, SOCK_STREAM, 0);
    int on=1;
    if(client_socket < 0)
        perror("Create Client Socket Fails!");

    setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    return client_socket;

}

// init the socket information of a server 
int Socket::initServer(int port_num){
    
    int server_socket;
    int opt = 1;
    int ret;

    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htons(INADDR_ANY);
    server_addr.sin_port = htons(port_num);

    server_socket = socket(PF_INET, SOCK_STREAM, 0);
    if(server_socket < 0)
        perror("Create server socket fails!");

    ret = setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt));
    if(ret!=0)
        perror("setsockopt error!");

    if(bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)))
        perror("Server bind port fails");

    return server_socket;
}

// send data 
void Socket::sendData(char* sent_data, size_t data_size, char* des_ip, int des_port_num){

    int client_socket = initClient(0);
    int ret;
    // set server_addr info
    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(des_port_num);
    
    if(inet_aton(des_ip, &server_addr.sin_addr) == 0)
        perror("inet_aton fails");
    
    while(connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0);

    size_t sent_len = 0;
    while(sent_len < data_size){
        ret = write(client_socket, sent_data + sent_len, data_size-sent_len);
        sent_len += ret;
    }
    ret=close(client_socket);
    if(ret == -1){
        cout << "ERR: close client_socket" << endl;
        exit(1);
    }
    cout << "finish send data" << endl;
}

// receive command
char* Socket::recvCommand(int port_num, size_t unit_size_sr, size_t hsb_cmd_len){

    int max_conn = 100;
    int connfd;
    int cmd_num;
    int ret;
    size_t unit_size;
    char tag[3];
    char role[3];

    // init the server socket info
    int server_socket = initServer(port_num);
    struct sockaddr_in sender_addr;
    socklen_t length = sizeof(sender_addr);

    if(listen(server_socket, max_conn) == -1)
        perror("listen fails");

    connfd = accept(server_socket, (struct sockaddr*)&sender_addr, &length);
    cout  << "--recv_connection_from " << inet_ntoa(sender_addr.sin_addr) << endl;

    // read the first three bytes
    ret = read(connfd, &tag, sizeof(char)*3);
    if(ret!=3){
        cout << "ERR: read cmd header" << endl;
        return NULL;
    }
    cmd_num = tag[0]-'0';

    role[0] = tag[1];
    role[1] = tag[2];
    role[2] = '\0';

    if(strcmp(role,"RS")!=0 && strcmp(role,"RR")!=0 && strcmp(role, "MS")!=0 && strcmp(role, "MR")!=0 && strcmp(role, "HS")!=0 && strcmp(role, "HR")!=0 && strcmp(role,"TS")!=0 && strcmp(role, "TR")!=0)
        return NULL;

    if(strcmp(role, "HR") == 0) 
        unit_size = hsb_cmd_len;
    else 
        unit_size = unit_size_sr;

    char* recv_cmd = (char*)malloc(sizeof(char)*(unit_size*cmd_num+1));
    memcpy(recv_cmd, role, sizeof(char)*2);

    size_t recv_len=2; // have read the role (2bytes) of the first command
    while(recv_len < unit_size*cmd_num)
        recv_len += read(connfd, recv_cmd+recv_len, unit_size*cmd_num - recv_len);
    recv_cmd[unit_size*cmd_num] = '\0';

    cout << "recv_cmd = " << recv_cmd << endl;
    
    close(connfd);
    close(server_socket);

    return recv_cmd;
}

char* Socket::recvHSBCommand(int port_num, size_t unit_size_sr, int* hsb_cmd_len){

    int max_conn = 100;
    int connfd;
    int cmd_num;
    int ret;
    size_t unit_size;
    char tag[3];
    char role[3];

    // init the server socket info
    int server_socket = initServer(port_num);
    struct sockaddr_in sender_addr;
    socklen_t length = sizeof(sender_addr);

    if(listen(server_socket, max_conn) == -1)
        perror("listen fails");

    connfd = accept(server_socket, (struct sockaddr*)&sender_addr, &length);
    cout  << "--recv_connection_from " << inet_ntoa(sender_addr.sin_addr) << endl;

    // read the first three bytes
    ret = read(connfd, &tag, sizeof(char)*3);
    if(ret!=3){
        cout << "ERR: read cmd header" << endl;
        return NULL;
    }
    cmd_num = tag[0]-'0';

    role[0] = tag[1];
    role[1] = tag[2];
    role[2] = '\0';

    if(strcmp(role,"RS")!=0 && strcmp(role,"RR")!=0 && strcmp(role, "MS")!=0 && strcmp(role, "MR")!=0 && strcmp(role, "HS")!=0 && strcmp(role, "HR")!=0 && strcmp(role,"TS")!=0 && strcmp(role, "TR")!=0)
        return NULL;

    char sizestr[4];
    if (strcmp(role, "HR") == 0) {
        // read 4 more bytes
        ret = read(connfd, &sizestr, sizeof(char)*4);
        *hsb_cmd_len = atoi(sizestr) + 4 + 2;
        unit_size = *hsb_cmd_len;
    } else
        unit_size = unit_size_sr;

    char* recv_cmd = (char*)malloc(sizeof(char)*(unit_size*cmd_num+1));
    int start = 0;
    memcpy(recv_cmd + start, role, sizeof(char)*2); start += 2;
    if (strcmp(role, "HR") == 0) {
        memcpy(recv_cmd + start, sizestr, sizeof(char)*4); start += 4;
    }

    while (start < unit_size * cmd_num)
        start += read(connfd, recv_cmd+start, unit_size*cmd_num - start);

//    size_t recv_len=2; // have read the role (2bytes) of the first command
//    while(recv_len < unit_size*cmd_num)
//        recv_len += read(connfd, recv_cmd+recv_len, unit_size*cmd_num - recv_len);

    recv_cmd[unit_size*cmd_num] = '\0';

    cout << "recv_cmd = " << recv_cmd << endl;
    
    close(connfd);
    close(server_socket);

    return recv_cmd;
}

void Socket::recvData(int chunk_size, int conn, char* buff, int index, int* mark_recv, int packet_num, int packet_size){

    int recv_len=0;

    while(recv_len < chunk_size){
    
        if(mark_recv == NULL) // for metadata
            recv_len += read(conn, buff+recv_len, chunk_size);
        else
            recv_len += read(conn, buff + recv_len, packet_size);
        
        if((index!=-1) && (mark_recv!=NULL)){

            int recv_packet_num = recv_len/packet_size -1;
            
            if(recv_packet_num!=-1){
                mark_recv[index*packet_num+recv_packet_num] = 1;

            //cout << "recv: mark_recv, index = " << index << endl;
            //for(int j=0; j<packet_num; j++)
            //    printf("%d ", mark_recv[index*packet_num+j]);
            //cout << endl; 
            //cout << "++ recv_len = " << recv_len << endl;
            }
        }        
    }

    if(mark_recv == NULL)
        return;

    cout << "finish recvData " << endl; 
}

// receive data in parallel
void Socket::paraRecvData(int port_num, int chunk_size, char* total_recv_data, int num_conn, int* mark_recv, int packet_num, int packet_size, int flag, string scenario){

    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);
    int i;
    int max_conn = 100;
    int* connfd = (int*)malloc(sizeof(int)*num_conn);
    thread recv_thrds[num_conn];

    // init the server socket info
    int server_socket = initServer(port_num);
    struct sockaddr_in sender_addr;
    socklen_t length = sizeof(sender_addr);

    if(listen(server_socket, max_conn) == -1)
        perror("listen fails");

    int index = 0;
    while(1){
        connfd[index] = accept(server_socket, (struct sockaddr*)&sender_addr, &length);
        cout  << "--recv_connection_from " << inet_ntoa(sender_addr.sin_addr) << endl;

        // we use multiple threads for the repair of data chunks
        if(flag != DATA_CHUNK)
            recv_thrds[index] = thread([=]{this->recvData(chunk_size, connfd[index], total_recv_data + index*chunk_size, -1, NULL, -1, -1);});
            
        else 
            recv_thrds[index] = thread([=]{this->recvData(chunk_size, connfd[index], total_recv_data + index*chunk_size, index, mark_recv, packet_num, packet_size);});

        index++;   
        if(index == num_conn)
            break; 
    }

    for(i=0; i<num_conn; i++)
        recv_thrds[i].join();
    
    for(i=0; i<num_conn; i++)
        close(connfd[i]);
    free(connfd);
    int ret=close(server_socket);
    if(ret == -1){
        cout << "ERR: close server_socket" << endl;
        exit(1);
    }
 
    // for migrated data, we commit an ack to the STF node
    if((num_conn == 1) && (flag!=ACK_INFO) && (scenario == "hotStandbyRepair")){
        char ack[4] = "ACK";
        sendData(ack, 4, inet_ntoa(sender_addr.sin_addr), PN_RECV_ACK_PORT);
        cout<< "---- migrate commit -----" << endl;
    }

    gettimeofday(&ed_tm, NULL);
    printf("paraRecv_time = %.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);
}

// calculate the delta of two regions
void Socket::calDelta(char* result, char* srcA, char* srcB, int len){

    int i;
    int XorCount = len/sizeof(long);
    
    long* srcA64 = (long*)srcA;
    long* srcB64 = (long*)srcB;
    long* result64 = (long*)result;

    for(i=0; i<XorCount; i++)
        result64[i] = srcA64[i]^srcB64[i];
}


// aggregate the receive chunks into one chunk
char* Socket::aggrData(char* total_recv_data, char* repaired_data, int num_chunks, int chunk_size, int packet_id, int packet_size){

    int i;
    char* mid_result = total_recv_data + packet_id*packet_size;
    char* tmp;

    for(i=1; i<num_chunks; i++){
        
        calDelta(repaired_data, mid_result, total_recv_data+i*chunk_size+packet_id*packet_size, packet_size);
        tmp = mid_result;
        mid_result = repaired_data;
        repaired_data = tmp;      
    }

    return mid_result;
}

char* Socket::hsbAggrData(char* total_recv_data, char* repaired_data, int num_chunks, int chunk_size, int packet_id, int packet_size){

    int i;
    char* mid_result = total_recv_data + packet_id*packet_size;
    char* tmp;

    for(i=1; i<num_chunks; i++){
        
        calDelta(repaired_data, mid_result, total_recv_data+i*chunk_size+packet_id*packet_size, packet_size);
        mid_result = repaired_data;
    }

    return mid_result;
}

