#include "AzureLRCPlus.hh"

AzureLRCPlus::AzureLRCPlus(vector<int> param) {
    _n = param[0];
    _k = param[1];
    _l = param[2];
    _g = param[3];

    _complete_matrix = (int*)malloc(sizeof(int)*_k*_n);
    memset(_complete_matrix, 0, sizeof(int)*_k*_n);
    
    for(int i=0; i<_k; i++) 
        _complete_matrix[i*_k+i]=1;

    string enc_file_name = string("conf/azurelrcplusEncMat_") + to_string(_n) + string("_") + to_string(_k) + "_" + to_string(_l) + "_" + to_string(_g); 
    ifstream readin(enc_file_name);
    for(int i=0; i<_n-_k; i++)                                                                                                                                                                                                                                                
        for(int j=0; j<_k; j++)
            readin >> _complete_matrix[(i+_k)*_k+j];                                                                                                                                                                                                                           
    readin.close();

    for (int i=0; i<_n; i++) {
        for (int j=0; j<_k; j++) {
            cout << _complete_matrix[i*_k+j] << " ";
        }
        cout << endl;
    }

    // local group
    _local_group_size = _k / _l;

    for (int i=0; i<_l; i++) {
        vector<int> curgroup;
        for (int j=0; j<_local_group_size; j++) {
            int chunkid = i * _local_group_size + j;
            curgroup.push_back(chunkid);
        }
        int pid = _k + i;
        curgroup.push_back(pid);

        for (auto idx: curgroup) {
            _id2group[idx] = curgroup;
        }
    }

    // global group
    vector<int> curgroup;
    for (int i=_k+_l; i<_n; i++)
        curgroup.push_back(i);
    for (int i=_k+_l; i<_n; i++) {
        _id2group[i] = curgroup;
    }
}

vector<int> AzureLRCPlus::getDecodeCoef(vector<int> from, int to) {
    cout << "from: ";
    for (int i=0; i<from.size(); i++)
        cout << from[i] << " ";
    cout << endl;

    cout << "to: " << to << endl;

    bool local;
    if (to < _k+_l)
        local = true;
    else
        local = false;

    cout << "local: " << local << endl;

    vector<int> toret;

    vector<int> group = _id2group[to];
    // map original chunk index in a stripe to idx in the subgroup;
    unordered_map<int, int> idxmap;
    for (int i=0; i<group.size(); i++) {
        int chunkidx = group[i];
        idxmap[chunkidx] = i;
    }

    if (local) {
        // prepare encoding matrix for the subgroup
        int* enc_matrix = (int*)malloc(sizeof(int)*group.size()*_local_group_size);
        memset(enc_matrix, 0, sizeof(int)*group.size()*_local_group_size);

        for (int i=0; i<_local_group_size; i++) {
            enc_matrix[i*_local_group_size+i] = 1;
            enc_matrix[_local_group_size*_local_group_size+i] = 1;
        }

        int _select_matrix[_local_group_size * _local_group_size];
        vector<int> data;
        for (int i=0; i<_local_group_size; i++) {
            int chunkidx = from[i];
            int sidx = idxmap[chunkidx];
            memcpy(_select_matrix + i * _local_group_size,
                   enc_matrix + sidx * _local_group_size,
                   sizeof(int) * _local_group_size);
        }

        int _invert_matrix[_local_group_size * _local_group_size];
        jerasure_invert_matrix(_select_matrix, _invert_matrix, _local_group_size, 8);

        int toidx = idxmap[to];
        int _select_vector[_local_group_size];
        memcpy(_select_vector,
               enc_matrix + toidx * _local_group_size,
               _local_group_size * sizeof(int));

        int* _coef_vector = jerasure_matrix_multiply(
                _select_vector, _invert_matrix, 1, _local_group_size, 
                _local_group_size, _local_group_size, 8);
        for (int i=0; i<_local_group_size; i++) toret.push_back(_coef_vector[i]);
    } else {
        for (int i=0; i<from.size(); i++)
            toret.push_back(1);
    }

    return toret;
}
