#include <iostream>
#include <thread>

#include "PeerNode.hh"

int main(int argc, char**argv){
   
    Config* conf = new Config("conf/config.xml");
    PeerNode* pn = new PeerNode(conf);
    Socket* sock = new Socket();
    thread doThreads[2];
    
    int send_flag;
    int recv_flag;
    int index;
    int num_cmds;
    int cmd_id; 
    string next_ip_str;

    char* recv_cmd;
    char* one_cmd = (char*)malloc(sizeof(char)*CMD_LEN);

    while(1){
        cout << "listen command ...." << endl;
        recv_cmd = sock->recvCommand(PN_RECV_CMD_PORT, CMD_LEN, -1);
        if(recv_cmd == NULL)
            continue;

        index = 0; 
        send_flag = 0;
        recv_flag = 0;
        cmd_id = 0;

        num_cmds = string(recv_cmd).length()/CMD_LEN;     
        string chunk_name_str[num_cmds];
        string stripe_name_str[num_cmds];

        char** role = (char**)malloc(sizeof(char*)*num_cmds);
        char** chunk_name = (char**)malloc(sizeof(char*)*num_cmds);
        char** stripe_name = (char**)malloc(sizeof(char*)*num_cmds);
        char** coeff = (char**)malloc(sizeof(char*)*num_cmds);
        char** next_ip = (char**)malloc(sizeof(char*)*num_cmds);
                                  
        pn->initChar(role, ROLE_LEN+1, num_cmds);
        pn->initChar(chunk_name, BLK_NAME_LEN+1, num_cmds);
        pn->initChar(stripe_name, STRIPE_NAME_LEN+1, num_cmds);
        pn->initChar(coeff, COEFF_LEN+1, num_cmds);
        pn->initChar(next_ip, NEXT_IP_LEN+1, num_cmds);

        struct timeval bg_tm, ed_tm;
        gettimeofday(&bg_tm, NULL);

        while(recv_cmd[index]!='\0'){
    
            memcpy(one_cmd, recv_cmd+index, CMD_LEN);
            index += CMD_LEN;

            cout << "parse cmd ...." << endl;
            pn->parseCommand(one_cmd, role[cmd_id], chunk_name[cmd_id], stripe_name[cmd_id], coeff[cmd_id], next_ip[cmd_id], NULL);    
        
            cout << "====== recv cmd ======" << endl; 
            cout << "cmd_id = " << cmd_id << endl;
            cout << role[cmd_id] << endl;
            cout << chunk_name[cmd_id] << endl;
            cout << stripe_name[cmd_id] << endl;
            cout << coeff[cmd_id] << endl;
            cout << next_ip[cmd_id] << endl;
            cout << "=======================" << endl;

            chunk_name_str[cmd_id] = string(chunk_name[cmd_id]);
            stripe_name_str[cmd_id] = string(stripe_name[cmd_id]);
            // if it is a receiver, then collect the k data
            if((strcmp(role[cmd_id], "MR") == 0)){
                
                if(pn->_repair_scenario == "scatteredRepair"){
                    recv_flag = 1;
                    doThreads[0] = thread(&PeerNode::paraRecvData, pn, 1, chunk_name_str[cmd_id], DATA_CHUNK);
                }
                // we do not use multiple threads in hotStandbyRepair
                else if(pn->_repair_scenario == "hotStandbyRepair"){
                    pn->paraRecvData(1, chunk_name_str[cmd_id], DATA_CHUNK);
                }
            }
            // if it is the receiver of the reconstruction
            else if(strcmp(role[cmd_id], "RR") == 0){
                recv_flag = 1;
                int recv_chunk_num = atoi(coeff[cmd_id]);
                //doThreads[0] = thread(&PeerNode::paraRecvData, pn, conf->_ecK, chunk_name_str[cmd_id], DATA_CHUNK);
                doThreads[0] = thread(&PeerNode::paraRecvData, pn, recv_chunk_num, chunk_name_str[cmd_id], DATA_CHUNK);
            }

            // if it is a sender of the reconstruction, then send the data 
            else if(strcmp(role[cmd_id], "RS") == 0){
                send_flag = 1;
                next_ip_str = pn->ip2Str(atoi(next_ip[cmd_id])); 
                doThreads[1] = thread(&PeerNode::sendData, pn, chunk_name_str[cmd_id], atoi(coeff[cmd_id]), (char*)next_ip_str.c_str(), DATA_CHUNK);
            }

            // if it is the sender of the migrated data (i.e., the STF node)
            else if(strcmp(role[cmd_id], "MS") == 0){
                pn->sendData(chunk_name_str[cmd_id], -1, (char*)(pn->ip2Str(atoi(next_ip[cmd_id])).c_str()), DATA_CHUNK); 
            }

            // if it is the sender of the metadata (i.e., the STF node)
            else if(strcmp(role[cmd_id], "TS") == 0){
                string chunk_meta_name;
                chunk_meta_name = chunk_name_str[cmd_id] + string("_") + stripe_name_str[cmd_id] + string(".meta");
                pn->sendData(chunk_meta_name, -1, (char*)(pn->ip2Str(atoi(next_ip[cmd_id]))).c_str(), META_CHUNK);
            }

            // if it is the receiver of the metadata
            else if(strcmp(role[cmd_id], "TR") == 0){
                string chunk_meta_name;
                chunk_meta_name = chunk_name_str[cmd_id] + string("_") + stripe_name_str[cmd_id] + string(".meta");
                
                if(pn->_repair_scenario == "scatteredRepair"){
                    recv_flag = 1;
                    doThreads[0] = thread(&PeerNode::paraRecvData, pn, 1, chunk_meta_name, META_CHUNK);
                }
                else if(pn->_repair_scenario == "hotStandbyRepair"){
                    pn->paraRecvData(1, chunk_meta_name, META_CHUNK);
                }
            }
            cmd_id ++;
        }   

        // execute the multiple threads
        if(recv_flag)
            doThreads[0].join();
        if(send_flag)
            doThreads[1].join(); 
        
        if((strcmp(role[0], "TR") == 0) || (strcmp(role[0], "MR") == 0) || (strcmp(role[0], "RR") == 0) || (recv_flag == 1))
            pn->commitACK();
        
        free(recv_cmd);
        
        for(int i=0; i<num_cmds; i++){
            free(role[i]);
            free(stripe_name[i]);
            free(chunk_name[i]);
            free(next_ip[i]);
            free(coeff[i]);
        }
        gettimeofday(&ed_tm, NULL);
        cout << "process one round time = " << ed_tm.tv_sec - bg_tm.tv_sec + (ed_tm.tv_usec - bg_tm.tv_usec)*1.0/1000000 << endl;
        
        free(role);
        free(stripe_name);
        free(chunk_name);
        free(coeff);
        free(next_ip);
    }

    free(one_cmd);
    delete conf;
    delete pn;
    delete sock;
}

