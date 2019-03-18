#include <iostream>
#include <thread>
#include <vector>

#include "Coordinator.hh"

using namespace std; 

int main(int argc, char** argv){

    if(argc != 4){
        std::cout << "Err: ./FastPRCoordinator #id_of_STF RepairMethod(fastpr or random or migration) #num_of_repair_chunks!" << std::endl;
        exit(1);
    }
    
    Config* conf = new Config("metadata/config.xml");
    conf -> display();
   
    Coordinator *coord = new Coordinator(conf);
    coord->preprocess(atoi(argv[1]), atoi(argv[3]));

    struct timeval begin_tm, end_tm;
    gettimeofday(&begin_tm, NULL);

    int real_rg_num = 0;
    if(strcmp(argv[2], "fastpr") == 0){    
        real_rg_num = coord->FastPRRepair(atoi(argv[1]));
    }
    else if(strcmp(argv[2], "random") == 0){
        real_rg_num = coord->randomRepair(atoi(argv[1]));
    }
    else if(strcmp(argv[2], "migration") == 0){
        real_rg_num = coord->simpleMigration(atoi(argv[1]));
    }
    else{
        cout << "Input should be: fastpr, random, migration" << endl;
        exit(1);
    }

    coord-> doProcess(atoi(argv[1]), real_rg_num, argv[2]);

    gettimeofday(&end_tm, NULL);
    double time = end_tm.tv_sec - begin_tm.tv_sec + (end_tm.tv_usec - begin_tm.tv_usec)*1.0/1000000;
    printf("%s_time: %.2lf\n", argv[2], time);
    delete coord;
    return 0;

}
