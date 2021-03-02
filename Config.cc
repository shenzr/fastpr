#include "Config.hh"

// read the config.xml file and get the configuration
Config::Config(std::string confFile){
    XMLDocument doc; 
    doc.LoadFile(confFile.c_str());
    XMLElement* element;

    for(element = doc.FirstChildElement("setting")->FirstChildElement("attribute"); element!=NULL; element = element->NextSiblingElement("attribute")){
        
        XMLElement* ele = element->FirstChildElement("name");
        std::string attName = ele->GetText();
        if (attName == "code_type")
            _code_type = std::string(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "fs_type")
            _fs_type = std::string(ele-> NextSiblingElement("value")-> GetText());

        else if(attName == "erasure_code_k")
            _ecK = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "erasure_code_n") 
            _ecN = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "erasure_code_r")
            _ecL = std::stoi(ele-> NextSiblingElement("value")-> GetText());
        
        else if (attName == "peer_node_num") 
            _peer_node_num = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "hotstandby_node_num") 
            _hotstandby_node_num = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "chunk_size")
            _chunk_size = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "meta_size")
            _meta_size = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "packet_size")
            _packet_size = std::stoi(ele-> NextSiblingElement("value")-> GetText());
        
        else if (attName == "disk_bandwidth")
            _disk_bandwidth = std::stoi(ele-> NextSiblingElement("value")-> GetText());
        
        else if (attName == "network_bandwidth")
            _network_bandwidth = std::stod(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "coordinator_ip")
            _coordinatorIP = inet_addr(ele-> NextSiblingElement("value")-> GetText());
        
        else if (attName == "local_data_path")
            _localDataPath = ele->NextSiblingElement("value")->GetText();
        
        else if (attName == "repair_scenario")
            _repair_scenario = ele->NextSiblingElement("value")->GetText();

        else if (attName == "peer_node_ips"){
            for(ele = ele->NextSiblingElement("value"); ele!=NULL; ele=ele->NextSiblingElement("value")){

                std::string tempstr = ele->GetText();
                int pos = tempstr.find("/");
                int len = tempstr.length();
                std::string ip = tempstr.substr(pos+1, len-pos-1);
                //std::cout << "Read IP: "<< ip << std::endl;
                //std::cout << "pos = " << pos << " len = "<< len << std::endl;
                //std::cout << "- %d" << inet_addr(ip.c_str()) << std::endl;
                _peerNodeIPs.push_back(inet_addr(ip.c_str()));
               
            }
            // sort(_peerNodeIPs.begin(), _peerNodeIPs.end()); 
        }
        else if (attName == "hotstandby_node_ips"){
            for(ele = ele->NextSiblingElement("value"); ele!=NULL; ele=ele->NextSiblingElement("value")){

                std::string tempstr = ele->GetText();
                int pos = tempstr.find("/");
                int len = tempstr.length();
                std::string ip = tempstr.substr(pos+1, len-pos-1);
                //std::cout << "HotStandby IP: "<< ip << std::endl;
                //std::cout << "pos = " << pos << " len = "<< len << std::endl;
                //std::cout << "- %d" << inet_addr(ip.c_str()) << std::endl;
                _hotStandbyNodeIPs.push_back(inet_addr(ip.c_str()));
               
            }
            // sort(_peerNodeIPs.begin(), _peerNodeIPs.end()); 
            
            for (int i=0; i<_peerNodeIPs.size(); i++) {
                unsigned int ip = _peerNodeIPs[i];
                _peerIp2Idx[ip] = i;
            }
        }
    }

    _ecL = _ecK / _ecR;
    _ecG = _ecN - _ecK - _ecL;
}

void Config::display(){
    
    std::cout << "Global info:"<< std::endl;
    std::cout << "_ecK = "<< _ecK << std::endl;
    std::cout << "_ecN = "<< _ecN << std::endl;
    std::cout << "_chunk_size = "<< _chunk_size << std::endl;
    std::cout << "_packet_size = "<< _packet_size << std::endl;
    std::cout << "_repair_scenario = "<< _repair_scenario << std::endl;

    std::cout << "Coordinator info:"<< std::endl;
    std::cout << "_coordinator_ip = "<< _coordinatorIP << std::endl;
    std::cout << "_localIP = " << _localIP << std::endl;
    std::cout << "Peer node info:" << std::endl;
    for(auto it: _peerNodeIPs)
        std::cout << it << std::endl;

    std::cout << "Hot standby node info:" << std::endl;
    for(auto it: _hotStandbyNodeIPs)
        std::cout << it << std::endl;
}
