Name : Aditya Borde

Please check the following commands before running the jar files.

********

1) To run the Q.1 - use "MovieData.jar" file and driver class name is "MovieData"

Command to run :
hadoop jar MovieData.jar MovieData <HadoopInputClusterPath> <HadoopOutputClusterPath>

here <InputClusterPath> is folder path of user.dat file

Example :
hadoop jar MovieData.jar MovieData /user/hue/Input /user/hue/Output
Input : is containing users.dat file

********

2) To run the Q.2 - use "MovieDataQ2.jar" file and driver class name is "MovieDataQ2"

Command to run :
hadoop jar MovieDataQ2.jar MovieDataQ2 <HadoopInputClusterPath> <HadoopOutputClusterPath>

<InputClusterPath> : this path contains folder path of users.dat on Hadoop cluster.

Example :
hadoop jar MovieDataQ2.jar MovieDataQ2 /user/hue/Input /user/hue/Output
Input : is containing users.dat file
********

3) To run the Q.3 - use "MovieDataQ3.jar" file and driver class name is "MovieDataQ3"

Command to run :
hadoop jar MovieDataQ3.jar MovieDataQ3 <HadoopInputClusterPath> <HadoopOutputClusterPath> <GenreValue>
<InputClusterPath> : this path contains folder path of movies.dat on Hadoop cluster.

Example for genre = "fantasy" use below:
hadoop jar MovieDataQ3.jar MovieDataQ3 /user/hue/Input /user/hue/Output fantasy

********