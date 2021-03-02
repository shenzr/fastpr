#include "FastPRUtil.hh"

vector<string> FastPRUtil::split(string str, string delim) {
    vector<string> tokens;
    size_t prev = 0, pos = 0;
    do
    {
        pos = str.find(delim, prev);
        if (pos == string::npos) pos = str.length();
        string token = str.substr(prev, pos-prev);
        if (!token.empty()) tokens.push_back(token);
        prev = pos + delim.length();
        
    }
    while (pos < str.length() && prev < str.length());
    return tokens;
}

string FastPRUtil::ip2Str(unsigned int ip) {
	string retVal;

    retVal += to_string(ip & 0xff);
    retVal += '.';

    retVal += to_string((ip >> 8) & 0xff);
    retVal +='.';

    retVal += to_string((ip >> 16) & 0xff);
    retVal +='.';

    retVal += to_string((ip >> 24) & 0xff);
    return retVal;
}

double FastPRUtil::duration(struct timeval t1, struct timeval t2) {
    return (t2.tv_sec-t1.tv_sec) * 1000.0 + (t2.tv_usec-t1.tv_usec) / 1000.0;  
}

