#include <iostream>
#include <thread>
#include <vector>

#include "Coordinator.hh"
#include "FastPRUtil.hh"

using namespace std; 

int main(int argc, char** argv){

    if(argc != 3){
        cout << "Usage: ./AzureLRCPlusTest" << endl;
        cout << "    1. repair method [fastpr|recon|mig]" << endl;
        cout << "    2. fastpr method [sct|hsb]" << endl;
        exit(1);
    }

    string repairmethod = argv[1];
    string fastprmethod = argv[2];

    string scenario;
    if (fastprmethod == "sct") {
        scenario = "scatteredRepair";
    } else {
        scenario = "hotStandbyRepair";
    }

    int stfnode = 0;
    
    Config* conf = new Config("conf/config.xml");
   
    Coordinator *coord = new Coordinator(conf);
    coord->getLostInfo(stfnode);

    struct timeval begin_tm, end_tm;    
    gettimeofday(&begin_tm, NULL); 
    if (repairmethod == "fastpr") {
        coord->fastprAzureLRCPlusRepair(scenario);
    } else if (repairmethod == "recon") {
        coord->randomAzureLRCPlusRepair(scenario);
    } else if (repairmethod == "mig") {
        coord->migrationAzureLRCPlusRepair(scenario);
    }
    gettimeofday(&end_tm, NULL);                                                                                                                                                                                                                                                  
    double time = FastPRUtil::duration(begin_tm, end_tm);                                                                                                                                                                                                                         
    cout << "Repair Time: " << time / 1000 << "s" << endl;                                                                                                                                                                                                                        
    cout << "Repair Num: " << coord->getLostNum() << endl;                                                                                                                                                                                                                        
    
    delete coord;                                       

    return 0;

}
