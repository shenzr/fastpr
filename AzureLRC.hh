#ifndef _AZURELRC_HH_
#define _AZURELRC_HH_

#include <unordered_map>

#include "ECBase.hh"

class AzureLRC : public ECBase {
    public:
        int _l;
        int _g;

        int _local_group_size;

        int* _complete_matrix;
        unordered_map<int, vector<int>> _id2group;
        
        AzureLRC(vector<int> param);

        vector<int> getDecodeCoef(vector<int> from, int to);
};

#endif
