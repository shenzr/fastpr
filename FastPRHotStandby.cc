#include <iostream>
#include <thread>

#include "PeerNode.hh"

int main(int argc, char**argv){
    
    Config* conf = new Config("metadata/config.xml");
    Socket* sock = new Socket();
    PeerNode* pn = new PeerNode(conf);


    int index;
    int ecK = pn->getVal("_ecK");
    int chunk_size = pn->getVal("_chunk_size");
    int packet_size = pn->getVal("_packet_size");
    int packet_num = chunk_size/packet_size;

    int hsb_cmd_len = CMD_LEN + ecK*NEXT_IP_LEN;
    cout << "hsb_cmd_len = " << hsb_cmd_len << endl;

    char* recv_cmd;
    char* one_cmd = (char*)malloc(sizeof(char)*hsb_cmd_len);

    char role[ROLE_LEN+1];
    char chunk_name[BLK_NAME_LEN+1];
    char stripe_name[STRIPE_NAME_LEN+1];
    char coeff[COEFF_LEN+1];
    char next_ip[NEXT_IP_LEN+1];
    char sender_ip[NEXT_IP_LEN*ecK+1];
    // ecK threads for receiving data and one thread for writing data
    thread dothrds[ecK+1];

    int num_repair_chunk;
    int conn_num = -1;
    int recv_cmd_len = -1;

    // some freq use memory
    int* cur_conn = (int*)malloc(sizeof(int)*(ecK+1));
    int* mark_recv = (int*)malloc(sizeof(int)*ecK*packet_num);
    char* total_recv_data = (char*)malloc(sizeof(char)*ecK*chunk_size);
    char* cur_sender_ip = (char*)malloc(sizeof(char)*NEXT_IP_LEN);
    int* connfd = NULL;
    int server_socket = -1; 

    map<string, int> senderIP2Conn;
    while(1){

        cout << "hsb: listen command ..." << endl;
        recv_cmd = sock->recvCommand(PN_RECV_CMD_PORT, CMD_LEN, hsb_cmd_len);
	if(recv_cmd == NULL)
            continue;

        char tmp_role[ROLE_LEN+1];
        strncpy(tmp_role, recv_cmd, sizeof(char)*ROLE_LEN);
        tmp_role[ROLE_LEN] = '\0';

        if(strcmp(tmp_role, "HR") == 0){

            // clean the info in map
            senderIP2Conn.erase(senderIP2Conn.begin(), senderIP2Conn.end());

            recv_cmd_len = hsb_cmd_len;
            num_repair_chunk = string(recv_cmd).length()/hsb_cmd_len;
            conn_num = num_repair_chunk*ecK;

            connfd = (int*)malloc(sizeof(int)*conn_num);
            server_socket = sock->initServer(PN_RECV_DATA_PORT);
            struct sockaddr_in sender_addr;
            socklen_t length = sizeof(sender_addr);

            if(listen(server_socket, 100) == -1)
                perror("listen fails");

            // collect the connfds
            int index = 0;
            while(1){
                connfd[index] = accept(server_socket, (struct sockaddr*)&sender_addr, &length);
                cout  << "--recv_connection_from " << inet_ntoa(sender_addr.sin_addr) << endl;
                string ipstr = to_string(sender_addr.sin_addr.s_addr);
                if(ipstr.length() < NEXT_IP_LEN)
                    ipstr.insert(0, "0");
                cout << "ipstr = " << ipstr << endl;
                printf("connfd[%d] = %d \n", index, connfd[index]);
                senderIP2Conn.insert(pair<string, int>(ipstr, connfd[index]));

                index++;
                if(index == conn_num)
                    break;
            }
        }

        else recv_cmd_len = CMD_LEN;

	cout << "sockets in map:" << endl;
	map<string, int>::iterator it;
	for(it=senderIP2Conn.begin(); it!=senderIP2Conn.end(); it++){
		cout<< it->first << " " << it->second << endl;
	}
        // process the cmds
        index = 0;
        while(recv_cmd[index]!=0){

            memcpy(one_cmd, recv_cmd+index, recv_cmd_len);
            index += recv_cmd_len;
        
            pn->parseCommand(one_cmd, role, chunk_name, stripe_name, coeff, next_ip, sender_ip);
            cout << "recv cmd ..." << endl;
            cout << "role = " << role << endl;
            cout << "chunk_name = " << chunk_name << endl;
            cout << "stripe_name = " << stripe_name << endl;
            cout << "coeff = " << coeff << endl;
            cout << "next_ip = " << next_ip << endl;
            
            if(strcmp(role, "HR") == 0){
         
                memset(mark_recv, 0, sizeof(int)*ecK*packet_num);
       
                for(int i=0; i<ecK; i++){
                    // get the sender ip
                    strncpy(cur_sender_ip, sender_ip+i*NEXT_IP_LEN, NEXT_IP_LEN);
                    string ipstr = string(cur_sender_ip);
                    string ip_substr = ipstr.substr(0,NEXT_IP_LEN);
                    cur_conn[i] = senderIP2Conn[ip_substr];
                    // cout << "ip_substr = " << ip_substr << endl; 
                    // printf("connfd[%d] = %d \n", i, cur_conn[i]);
                    // use a thread to read the data 
                    dothrds[i] = thread(&Socket::recvData, sock, chunk_size, cur_conn[i], total_recv_data+i*chunk_size, i, mark_recv, packet_num, packet_size);

                }

                // use a thread to aggregate and write the data 
                dothrds[ecK] = thread(&PeerNode::aggrDataWriteData, pn, chunk_name, total_recv_data, ecK, chunk_size, mark_recv, DATA_CHUNK);

                // join the threads
                for(int i=0; i<ecK+1; i++)
                    dothrds[i].join();
            }
            // if it is a command to write the metadata
            else if(strcmp(tmp_role, "TR") == 0){
  
                string chunk_meta_name;
                chunk_meta_name = string(chunk_name) + string("_") + string(stripe_name) + string(".meta");
                // sequential write the metadata
                pn->paraRecvData(1, chunk_meta_name, META_CHUNK); 
            }
        }

        // free the socket info for the HR
        if(strcmp(tmp_role, "HR") == 0){

            close(server_socket);
            for(int i=0; i<conn_num; i++)
                close(connfd[i]);
            free(connfd);        
        }
        free(recv_cmd);
        pn->commitACK();
    }

    free(mark_recv);
    free(total_recv_data);
    free(cur_conn);
    free(total_recv_data);
    free(one_cmd);
    delete conf;
    delete sock;
    delete pn;
}
