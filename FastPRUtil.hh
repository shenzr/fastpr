#ifndef _FASTPRUTIL_HH_
#define _FASTPRUTIL_HH_

#include "include.hh"

class FastPRUtil {
    public:
        static vector<string> split(string str, string delim);
        static string ip2Str(unsigned int);
        static double duration(struct timeval t1, struct timeval t2);
};

#endif
