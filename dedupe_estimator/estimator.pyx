# estimator.pyx
from libcpp.vector cimport vector
from libcpp.string cimport string


# Import the function declared in the .pxd file
cdef extern from "src/estimator.h":
    cdef struct CDedupeResult "DedupeResult":        
        size_t total_len;
        size_t chunk_bytes;
        size_t compressed_chunk_bytes;

    CDedupeResult c_estimate "estimate"(vector[string] paths)


# Python wrapper function
def estimate(paths):
    """
    Python wrapper for the C++ estimate function.
    Converts a Python list of strings to a C++ std::vector<std::string>.
    """
    cdef vector[string] cpp_paths
    for path in paths:
        cpp_paths.push_back(str(path).encode('utf-8'))
    
    return c_estimate(cpp_paths)
    