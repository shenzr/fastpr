#include "PeerNode.hh"

// read the information from the config file
PeerNode::PeerNode(Config* conf){
    _chunk_size = conf->_chunk_size;
    _packet_size = conf->_packet_size;
    _meta_size = conf->_meta_size;
    _ecK = conf->_ecK;
    _data_path = conf->_localDataPath;
    _coordinator_ip = conf->_coordinatorIP;
    _repair_scenario = conf->_repair_scenario;
    cout << "data_path = " << _data_path << endl;

}

// init char array
void PeerNode::initChar(char** array, int len, int num){

    for(int i=0; i<num; i++)
	array[i] = (char*)malloc(sizeof(char)*len);

}

// parse and extract the information from the command
void PeerNode::parseCommand(char* command, char* role, char* blk_name, char* stripe_name, char* coeff, char* next_ip, char* sender_ips){

    // read the first two bytes to judge the role
    strncpy(role, command, sizeof(char)*ROLE_LEN);

    strncpy(blk_name, command + ROLE_LEN, sizeof(char)*BLK_NAME_LEN);
    strncpy(stripe_name, command + ROLE_LEN + BLK_NAME_LEN, sizeof(char)*STRIPE_NAME_LEN);
    strncpy(coeff, command + ROLE_LEN + BLK_NAME_LEN + STRIPE_NAME_LEN, sizeof(char)*COEFF_LEN);
    strncpy(next_ip, command + ROLE_LEN + BLK_NAME_LEN + STRIPE_NAME_LEN + COEFF_LEN, sizeof(char)*NEXT_IP_LEN); 

    role[ROLE_LEN] = '\0';
    blk_name[BLK_NAME_LEN] = '\0';
    stripe_name[STRIPE_NAME_LEN] = '\0';
    coeff[COEFF_LEN] = '\0';
    next_ip[NEXT_IP_LEN] = '\0';

    if(_repair_scenario == "hotStandbyRepair" && (strcmp(role, "HR") == 0)){
        strncpy(sender_ips, command + ROLE_LEN + BLK_NAME_LEN + STRIPE_NAME_LEN + COEFF_LEN + NEXT_IP_LEN, sizeof(char)*_ecK*NEXT_IP_LEN);
        sender_ips[_ecK*NEXT_IP_LEN] = '\0';
    }
}

// transform a ip address to string
string PeerNode::ip2Str(unsigned int ip){

    string retVal;
    retVal += to_string(ip & 0xff);
    retVal += '.';
    
    retVal += to_string((ip >> 8) & 0xff);
    retVal +='.';

    retVal += to_string((ip >> 16) & 0xff);
    retVal +='.';

    retVal += to_string((ip >> 24) & 0xff);
    return retVal;
}


// partial encode and send data 
void PeerNode::partialEncodeSendData(int coeff, char* data_buff, char* next_ip, int* mark_index){

    printf("coeff = %d\n", coeff);
    // partial encode data
    Socket* sock = new Socket();
    char* pse_buff = (char*)malloc(sizeof(char)*_packet_size);
    int num_packets = _chunk_size/_packet_size;

    // init the client socket info and connect to the server
    int client_socket = sock->initClient(0);

    // set server_addr info
    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PN_RECV_DATA_PORT);

    if(inet_aton(next_ip, &server_addr.sin_addr) == 0)
        perror("inet_aton fails");
    
    while(connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0);

    // use a socket to send data multiple times
    int sent_count = 0;
    int ret;
    while(sent_count < num_packets){

        if(mark_index[sent_count] == 0)
           sleep(0.001);

        if(mark_index[sent_count] == 1){
 
            if(coeff!=-1){
                galois_w08_region_multiply(data_buff+sent_count*_packet_size, coeff, _packet_size, pse_buff, 0);

                size_t sent_len = 0;
                while(sent_len < _packet_size){
                    ret =  write(client_socket, pse_buff + sent_len, _packet_size - sent_len);
                    sent_len += ret;
                }  
            }
            else{
                size_t sent_len = 0;

                while(sent_len < _packet_size){
                    ret =  write(client_socket, data_buff + sent_len, _packet_size - sent_len);
                    sent_len += ret;
                }  
            }

           sent_count++;
           //cout << "sent_paket_num = " << sent_count << endl;          
           //cout << "send: mark_index:" << endl;
           //for(int i=0; i<num_packets; i++)
           //    cout << mark_index[i] << " ";
           //cout << endl;

           //gettimeofday(&ed_tm, NULL);
           //printf("encode_send_data_time = %.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);
            
        }
    }
    ret = close(client_socket);
    if(ret == -1){
        cout << "ERR: close socket" << endl;
        exit(1);
    }
    delete sock;
    free(pse_buff);
}

// read data from the data path 
void PeerNode::readData(char* buff, string blk_abs_path, int* mark_index){

    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    int fd = open((char*)blk_abs_path.c_str(), O_RDONLY | O_DIRECT);
    if(fd<0)
        perror("open_file_fails");
    
    size_t read_size = 0;
    size_t ret;
    int read_count = 0;
    size_t rd_pkt_len;
    while(read_size < _chunk_size){

        rd_pkt_len = 0;
        while(rd_pkt_len < _packet_size){
            ret = read(fd, buff + read_count*_packet_size + rd_pkt_len, _packet_size - rd_pkt_len);
            rd_pkt_len += ret;
        }
        assert(rd_pkt_len == _packet_size);

        read_size += _packet_size;
        lseek(fd, read_size, SEEK_SET);
        mark_index[read_count++] = 1;
       
        //cout << "packet_size = " << _packet_size << endl;  
        //cout << "chunk_size = " << _chunk_size << endl;  
        //cout << "read_count = " << read_size/_packet_size << endl; 
        //cout << "read_size = " << read_size << ", chunk_size = " << _chunk_size << endl;
        //cout << "read: mark_index:" << endl;
        //for(int i=0; i<_chunk_size/_packet_size; i++)
        //    cout << mark_index[i] << " ";
        //cout << endl;
    }

    close(fd);

    gettimeofday(&ed_tm, NULL);
    printf("read_data_time = %.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

}


// send data 
void PeerNode::sendData(string blk_name, int coeff, char* next_ip, int flag){
   
    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    // get absolute path
    string blk_abs_path;
    string parent_dir;

    size_t sent_size;
    if(flag == DATA_CHUNK)
        sent_size = _chunk_size;
    else 
        sent_size = _meta_size;

    parent_dir = findChunkAbsPath((char*)blk_name.c_str(), (char*)_data_path.c_str());
    blk_abs_path = parent_dir + string("/") + blk_name;

    // multi-thread: 1) read data; 2) send data 
    // read data
    int num_packets;
    num_packets = sent_size/_packet_size;

    char* buff;
    int ret = posix_memalign((void**)&buff, getpagesize(), sizeof(char)*sent_size);
    if(ret){
        cout << "ERR: posix_memalign" << endl;
        exit(1);
    }

    // if there are more packets, then we use multiple threads
    if(flag == DATA_CHUNK){

        int* mark_read = (int*)malloc(sizeof(int)*num_packets);
        memset(mark_read, 0, sizeof(int)*num_packets);

        thread dothrds[2];
        dothrds[0] = thread(&PeerNode::readData, this, buff, blk_abs_path, mark_read);
        dothrds[1] = thread(&PeerNode::partialEncodeSendData, this, coeff, buff, next_ip, mark_read);
                                             
        dothrds[0].join();
        dothrds[1].join();
       
	//cout << "debug: thread ends" << endl; 
        free(mark_read); 
    }

    //if it is the metadata chunk 
    else {
        //read the data
        int fd = open((char*)blk_abs_path.c_str(), O_RDWR);
        size_t ret;
        
        ret = read(fd, buff, sent_size);
        if(ret!=sent_size){
            cout << "ret = " << ret << endl;
            exit(1);
        } 
        // sent the data
        Socket* sock = new Socket();
        sock->sendData(buff, sent_size, next_ip, PN_RECV_DATA_PORT);
        close(fd);
        delete sock;
    }

    // if it is a migrated data, then receive commit
    if(coeff == -1 && _repair_scenario == "hotStandbyRepair"){
        cout << "waiting commit..." << endl;
        char recv_ack[ACK_LEN+1];
        Socket* recv_ack_sock = new Socket();
        recv_ack_sock->paraRecvData(PN_RECV_ACK_PORT, ACK_LEN+1, recv_ack, 1, NULL, -1, -1, ACK_INFO, _repair_scenario);
        delete recv_ack_sock;

    }

    cout << "<--------sendData " << endl;
    free(buff);
    
    gettimeofday(&ed_tm, NULL);
    printf("sendData time = %.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

}

// write the data 
void PeerNode::writeData(string chunk_name, size_t offset, char* write_buff, size_t write_size, int flag){

    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    // append the data, we put the repaired data on the subdir0
    size_t ret;
    string abs_lost_chunk_path; 
    //You can choose to place the repaired chunk at the default data path of HDFS 
    abs_lost_chunk_path = _data_path + string("/subdir0/") + chunk_name;
    // abs_lost_chunk_path = "/home/ncsgroup/zrshen/HyRe/" + chunk_name;

    int fd;
    if(flag == DATA_CHUNK)
        fd = open((char*)abs_lost_chunk_path.c_str(), O_RDWR | O_CREAT | O_SYNC, 0755);
    else 
        fd = open((char*)abs_lost_chunk_path.c_str(), O_RDWR | O_CREAT, 0755);

    lseek(fd, offset, SEEK_SET);
    
    ret = write(fd, write_buff, write_size);
    if(ret!=write_size)
        perror("write error");

    close(fd);

    //remove the write data
    //if(remove((char*)abs_lost_chunk_path.c_str())!=0)
        //perror("remove file");

    gettimeofday(&ed_tm, NULL);
    printf("writeData time = %.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);
}

// aggregate the data and write the data
void PeerNode::aggrDataWriteData(string chunk_name, char* recv_buff, int recv_chunk_num, size_t recv_size, int* mark_recv, int flag){

    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    // we have to differentiate data chunk and metadata chunk
    if((recv_chunk_num >= 1) && (recv_size == _chunk_size)){

        size_t ret;
        string abs_lost_chunk_path = _data_path + string("/subdir0/") + chunk_name;
        // string abs_lost_chunk_path = string("/home/ncsgroup/zrshen/HyRe/") + chunk_name;

        int fd = open((char*)abs_lost_chunk_path.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0755);

        char* repair_buff;
        int tmp_ret = posix_memalign((void**)&repair_buff, getpagesize(), sizeof(char)*_packet_size);
        if(tmp_ret){
            cout << "ERR: posix_memalign" << endl;
            exit(1);         
        }
        char* tmp_pos;
        Socket* sock = new Socket();

        int packet_num = _chunk_size/_packet_size;
        int sum = 0;
    
        for(int i=0; i<packet_num; i++){
            sum = 0;
            for(int j=0; j<recv_chunk_num; j++)
                if(mark_recv[j*packet_num+i])
                    sum++;
            
            // if the first packet of all the chunks is not received
            if(sum!=recv_chunk_num){
                i--;
                sleep(0.001);
            }
            // aggregate the packets and write the packet            
            else {
                tmp_pos = sock->aggrData(recv_buff, repair_buff, recv_chunk_num, recv_size, i, _packet_size);
                //writeData(chunk_name, i*_packet_size, tmp_pos, _packet_size, flag);               

                lseek(fd, i*_packet_size, SEEK_SET);
                ret = write(fd, tmp_pos, _packet_size);
                if(ret!=_packet_size)
                    perror("write error");
                
                //cout << "aggregate: mark_recv, check packet_id = " << i << endl;
                //for(int p=0; p<recv_chunk_num; p++){
                //    for(int q=0; q<packet_num; q++)
                //        printf("%d ", mark_recv[p*packet_num+q]);
                //printf("\n");
                //}    
            }
        }
        
        free(repair_buff);
        close(fd);
        delete sock;

        // remove the file after write completes 
        //if(remove((char*)abs_lost_chunk_path.c_str()) != 0) --------> mark at Mar. 06
            //perror("remove file"); --------------> mark at Mar.06
    }

    // we have to further consider the writes for multiple metadata chunks
    else if(recv_size!=_chunk_size){

        writeData(chunk_name, 0, recv_buff, recv_size, flag);
 
    }
   gettimeofday(&ed_tm, NULL);
   printf("write_data_time = %.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);
}

// parallel receive data 
void PeerNode::paraRecvData(int recv_chunk_num, string lost_chunk_name, int flag){

    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    cout << "--------> paraRecvData, recv_chunk_num =  " << recv_chunk_num << endl;
    size_t recv_size;
    char* recv_buff;
    int ret;
    if(flag == DATA_CHUNK){
        recv_size = _chunk_size;
        ret = posix_memalign((void**)&recv_buff, getpagesize(), sizeof(char)*recv_chunk_num*recv_size);
        if(ret){
            cout << "ERR: posix_memalign" << endl;
            exit(1);
        }    
    }
    else{ 
        recv_size = _meta_size;
        recv_buff = (char*)malloc(sizeof(char)*recv_size*recv_chunk_num);
    }
    
    // mark the recv data 
    int packet_num = recv_size/_packet_size;
    int* mark_recv = NULL;

    if(flag == DATA_CHUNK){
        mark_recv = (int*)malloc(sizeof(int)*recv_chunk_num*packet_num);
        memset(mark_recv, 0, sizeof(int)*recv_chunk_num*packet_num);
    }

    // recv the data    
    Socket* sock = new Socket();
    thread dothrds[2];

    // multiple threads: recv data and write data
    dothrds[0] = thread(&Socket::paraRecvData, sock, PN_RECV_DATA_PORT, recv_size, recv_buff, recv_chunk_num, mark_recv, packet_num, _packet_size, flag, _repair_scenario);
    dothrds[1] = thread(&PeerNode::aggrDataWriteData, this, lost_chunk_name, recv_buff, recv_chunk_num, recv_size, mark_recv, flag);

    dothrds[0].join();
    dothrds[1].join();
    
    free(recv_buff);
    delete(sock);

    if(flag == DATA_CHUNK)
        free(mark_recv);

    cout << "<-------- paraRecvData" << endl;

    gettimeofday(&ed_tm, NULL);
    printf("recvData time = %.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);
}

// commit ack
void PeerNode::commitACK(){

    // commit the ack
    char ack[ACK_LEN+1]="ACK";
    Socket* sock_ack = new Socket();
    cout << "coordinator ip = " << (char*)ip2Str(_coordinator_ip).c_str() << endl; 
    cout << "CD_RECV_ACK_PORT = " << CD_RECV_ACK_PORT << endl; 
    sock_ack->sendData(ack, ACK_LEN+1, (char*)ip2Str(_coordinator_ip).c_str(), CD_RECV_ACK_PORT);
    delete sock_ack;
    cout << "--------commit finishes--------" << endl;

}

// find the absolute path of a given data chunk
string PeerNode::findChunkAbsPath(char* chunk_name, char* parent_dir){

    DIR* dir = opendir(parent_dir);
    assert(dir!=NULL);

    string want_dir;
    string child_dir;
    struct dirent *d_ent;
	
    while((d_ent = readdir(dir))!=NULL){

        if((strcmp(d_ent->d_name,".")==0) || (strcmp(d_ent->d_name,"..")==0))
            continue;

        if((d_ent->d_type == 8) && strcmp(d_ent->d_name, chunk_name)==0){
            return parent_dir;
        }

        else if(d_ent->d_type == 4){
            child_dir = parent_dir + string("/") + string(d_ent->d_name);
            want_dir = findChunkAbsPath(chunk_name, (char*)child_dir.c_str());
            if(want_dir != ""){
         		closedir(dir);
                return want_dir;
	        }
        }
     }

     closedir(dir);
     return "";
}

// get values
int PeerNode::getVal(string req){

    if(req == "_ecK")
        return _ecK;

    else if(req == "_chunk_size")
        return _chunk_size;

    else if(req == "_packet_size")
        return _packet_size;

    else {
        cout << "ERR: input " << endl;
        exit(1);
    }
}

