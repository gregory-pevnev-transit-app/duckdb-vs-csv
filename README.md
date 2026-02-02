Importing: Comparing CSV implementation against the most optimal DuckDB implementation (buffering in the in-memory DB with compression before copying to the disk in one operation).

Notes: Simulating optimistic conditions
- Using 2 CPUs (Being optimistic / trying to see the best possible performance)
- No disk-contention (HDD, but without any other traffic)
- No memory-limits (Checking how much memory is taken without restrictions, as the best possible limits cannot be accurately guessed)

Stats: 
- WMATA_P: 
  - CSV:
    - Time: 10s/15s
    - Memory: 1.8GB (~600MB for the records)
    - Size: 161MB
  - DuckDB: 
    - Time: 3s/4s
    - Memory: 1.2GB (~600MB for the records)
    - Size: 20MB
- VICMBAU:
  - CSV:
    - Time: 18s/25s
    - Memory: 3GB (~900MB for the records)
    - Size: 270MB
  - DuckDB: 
    - Time: 9s/10s
    - Memory: 1.8GB (~900MB for the records)
    - Size: 42MB
- CARRISMPT:
  - CSV:
    - Time: 45s/90s
    - Memory: 4.5GB (~2GB for the records)
    - Size: 627MB
  - DuckDB: 
    - Time: 22s/25s
    - Memory: 3GB (~2GB for the records)
    - Size: 58MB
