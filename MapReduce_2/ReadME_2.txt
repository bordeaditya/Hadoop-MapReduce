Name : Aditya Borde

Please check the following commands before running the jar files.

********

1) To run the Q.1 - use "ReduceSideJoin.jar" file and driver class name is "ReduceSideJoin"

Command to run :
hadoop jar ReduceSideJoin.jar ReduceSideJoin <path_users.dat> <path_ratings.dat> <path_movies.dat> <HadoopOutputClusterPath>


Example :
hadoop jar MapJoin.jar MapJoin /user/hue/Input/users.dat /user/hue/Input/ratings.dat /user/hue/Input/movies.dat <OUTPUT>

Input : contains users.dat, ratings.dat,movies.dat files


********


2) To run the Q.2 - use "MapJoin.jar" file and driver class name is "MapJoin"

Command to run :
hadoop jar MapJoin.jar MapJoin <path_users.dat> <path_ratings.dat> <HadoopOutputClusterPath> <movieID>


Example :
hadoop jar MapJoin.jar MapJoin /user/hue/Input/users.dat /user/hue/Input/ratings.dat <OUTPUT> 3235

<movieId> : 3235
Input : contains users.dat, ratings.dat files
********
