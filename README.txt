# On ETL DEVELOPEMENT 

- Need to think about version of injested data 
- All injested raw data (.json) files must be stored in a postgres table
- database will include when the file was injested, a hash of the file, and the data itself. 
- goal is to avoid storing .json files loosely in a directory (cool right?) 

