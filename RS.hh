#ifndef _RS_HH_
#define _RS_HH_

#include "ECBase.hh"

class RS : public ECBase {
    public:
        int* _complete_matrix;
        
        RS(vector<int> param);

        vector<int> getDecodeCoef(vector<int> from, int to);
};

#endif
