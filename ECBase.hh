#ifndef _ECBASE_HH_
#define _ECBASE_HH_

#include "include.hh"

extern "C"{
#include "Util/jerasure.h"                                                                                                                                                                                                                                                    
#include "Util/galois.h"                                                                                                                                                                                                                                                      
}  

class ECBase {
    public:
        int _n, _k;

        ECBase();
        ECBase(vector<int> param);

        virtual vector<int> getDecodeCoef(vector<int> from, int to) = 0;
};

#endif
