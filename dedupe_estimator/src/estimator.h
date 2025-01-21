#pragma once

#include <vector>
#include <tuple>
#include <string>


struct DedupeResult {
  size_t total_len;
  size_t chunk_bytes;
  size_t compressed_chunk_bytes;
};
 
DedupeResult estimate(std::vector<std::string> paths);