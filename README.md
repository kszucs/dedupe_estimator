# Chunk Dedupe Estimator
This estimates the amount of chunk level dedupe available in a
collection of files. All files will be chunked together
so if there are multiple versions of the file, all versions
should be provided together to see how much can be saved.
using Hugging Face's dedupped storage architecture.

The chunking algorithm used here is **not** the same
as the one being deployed. Notably, for simplicity this does not
even use a secure hash method for chunk hashes and just use
std::hash. This means that collisions are plausible and 
exact numbers may actually vary from run to run. 
But it should provide a reasonable estimate.

# Compile and Run
Compile:
```bash
make
```

Run:
```
./dedupe_estimator <file1> <file2> ...
```
