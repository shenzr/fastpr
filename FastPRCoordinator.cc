#include <iostream>
#include <thread>
#include <vector>

#include "Coordinator.hh"
#include "FastPRUtil.hh"

using namespace std; 

int main(int argc, char** argv){

    if(argc != 4){
        cout << "Usage: ./FastPRCoordinator" << endl;
        cout << "    1. id of stf node" << endl;
        cout << "    2. repair method" << endl;
        cout << "    3. number of repaired chunks" << endl;
        exit(1);
    }

    int stfnode = atoi(argv[1]);
    string repairmethod = argv[2];
    int chunknum = atoi(argv[3]);

    Config* conf = new Config("metadata/config.xml");
    string scenario = conf->_repair_scenario;
   
    Coordinator *coord = new Coordinator(conf);
    coord->getLostInfo(stfnode);

    if (coord->getLostNum() < chunknum) {
        cout << "ERROR: The number of chunks in the STF node is less than " << chunknum << endl;
        return -1;
    }

    struct timeval begin_tm, end_tm;
    gettimeofday(&begin_tm, NULL);
    if (repairmethod == "fastpr") {
        coord->fastprRSRepair(scenario);
    } else if (repairmethod == "recon") {
        coord->randomRSRepair(scenario);
    } else if (repairmethod == "mig") {
        coord->migrationRSRepair(scenario);
    }
    gettimeofday(&end_tm, NULL);
    double time = FastPRUtil::duration(begin_tm, end_tm);
    cout << "Repair Time: " << time / 1000 << "s" << endl;
    cout << "Repair Num: " << coord->getLostNum() << endl;

    delete coord;
    return 0;

}
