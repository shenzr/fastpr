#include <iostream>
#include <thread>
#include <vector>

#include "Coordinator.hh"
#include "FastPRUtil.hh"


using namespace std; 

int main(int argc, char** argv){

    if(argc != 3){
        cout << "Usage: ./AzureLRCTest" << endl;
        cout << "    1. repair method [fastpr|recon|mig]" << endl;
        cout << "    2. fastpr method [scatteredRepair|hotStandbyRepair]" << endl;
        exit(1);
    }

    string repairmethod = argv[1];
    string fastprmethod = argv[2];

    string scenario = fastprmethod;
    //if (fastprmethod == "sct") {
    //    scenario = "scatteredRepair";
    //} else {
    //    scenario = "hotStandbyRepair";
    //}

    int stfnode = 0;
    
    Config* conf = new Config("metadata/config.xml");
   
    Coordinator *coord = new Coordinator(conf);
    coord->getLostInfo(stfnode);

    struct timeval begin_tm, end_tm;                                                                                                                                                                                                                                              
    gettimeofday(&begin_tm, NULL);
    if (repairmethod == "fastpr") {
        coord->fastprAzureLRCRepair(scenario);
    } else if (repairmethod == "recon") {
        coord->randomAzureLRCRepair(scenario);
    } else if (repairmethod == "mig") {
        coord->migrationAzureLRCRepair(scenario);
    }
    gettimeofday(&end_tm, NULL);                                                                                                                                                                                                                                                  
    double time = FastPRUtil::duration(begin_tm, end_tm);                                                                                                                                                                                                                         
    cout << "Repair Time: " << time / 1000 << "s" << endl;                                                                                                                                                                                                                        
    cout << "Repair Num: " << coord->getLostNum() << endl;                                                                                                                                                                                                                        
    
    delete coord; 

    return 0;

}
