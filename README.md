1.首先在hdfs上创建以下目录：/user/hadoop/Kmeans/input、/user/hadoop/Kmeans/out

2.把oldCenters.data上传到hdfs的/user/hadoop/Kmeans/目录下

3.将wine.data上传到hdfs的/user/hadoop/Kmeans/input目录下

运行命令及参数：
hadoop jar ~/CodeProject/IdeaProjects/K-means/kmeans/target/kmeans-1.0-SNAPSHOT.jar com.hadoop.mapreduce.Kmeans hdfs://localhost:9000/user/hadoop/Kmeans/input hdfs://localhost:9000/user/hadoop/Kmeans/out /home/hadoop/CodeProject/IdeaProjects/K-means/kmeans/oldCenters.data hdfs://localhost:9000/user/hadoop/Kmeans/oldCenters.data hdfs://localhost:9000/user/hadoop/Kmeans/out/part-r-0000 1 3
