#include "RS.hh"

RS::RS(vector<int> param) {
    _n = param[0];
    _k = param[1];

    _complete_matrix = (int*)malloc(sizeof(int)*_k*_n);
    memset(_complete_matrix, 0, sizeof(int)*_k*_n);
    
    for(int i=0; i<_k; i++) 
        _complete_matrix[i*_k+i]=1;

    string enc_file_name = string("metadata/rsEncMat_") + to_string(_n) + string("_") + to_string(_k); 
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
}

vector<int> RS::getDecodeCoef(vector<int> from, int to) {
    vector<int> data;
    int _select_matrix[_k*_k];
    for (int i=0; i<_k; i++) {
        data.push_back(from[i]);
        int sidx = from[i];
        memcpy(_select_matrix + i * _k,
                _complete_matrix + sidx * _k,
                sizeof(int) * _k);
    }

    int _invert_matrix[_k*_k];
    jerasure_invert_matrix(_select_matrix, _invert_matrix, _k, 8); 

    vector<int> toret;

    int ridx = to;
    int _select_vector[_k];
    memcpy(_select_vector,
            _complete_matrix + ridx * _k,
            _k * sizeof(int));
    int* _coef_vector = jerasure_matrix_multiply(
            _select_vector, _invert_matrix, 1, _k, _k, _k, 8
            );
    for (int i=0; i<_k; i++) toret.push_back(_coef_vector[i]);

    return toret;
}
