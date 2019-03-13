In this file, we will briefly introduce how to deploy and run the FastPR prototype. More design details can be referred to our paper "Fast Predictive Repair in Erasure-Coded Storage" which appears at the 49th IEEE/IFIP International Conference on Dependable Systems and Networks (IEEE/IFIP DSN'19). If you have any question about the deployment, please feel free to contact me at zhirong.shen2601@gmail.com 



## Preparation  

1. [All nodes] Install necessary packages, including make and g++ 

   ```shell
   $ sudo apt-get install make g++
   ```

2. [All nodes] Install HDFS 3.1.1 and construct the HDFS cluster. 

   ```shell
   $ tar xvf hadoop-3.1.1-src.tar.gz 
   ```

3. Configure the configuration files under the folder hadoop-3.1.1/etc/hadoop/, including core-site.xml, hadoop-env.sh, hdfs-site.xml, and workers. 

4. [All nodes] set the environment variables for HDFS and JAVA in ~/.bashrc. The following is an sample used in our testbed 

   ```shell
   export JAVA_HOME=/home/ncsgroup/java
   export HADOOP_HOME=/home/ncsgroup/zrshen/hadoop-3.1.1 
   export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
   ```

5. [Client] Create a file named "testfile", whose size should be multiple times of the data size of a stripe. 

   ```shell
   $ dd if=/dev/urandom of=testfile bs=1M count=30720    # create a random file (30GB)
   ```

6. [Client] Select and enable an erasure coding scheme and write data to the HDFS

   ```shell
   $ hadoop fs -mkdir /ec_test                      # create a folder named /ec_test 
   $ hdfs ec -listPolicies                          # list the policies supported by HDFS
   $ hdfs ec -enablePlicy -policy RS-3-2-1024k      # enable an erasure coding policy 
   $ hdfs ec -setPolicy -path /ec_test -policy RS-3-2-1024k  # set ec policy to /ec_test
   $ hdfs ec -getPolicy -path /ec_test              # confirm the ec policy of /ec_test 
   $ hadoop fs -put testfile /ec_test              # write testfile to /ec_test
   ```

   

## Configuration 

The configuration of FastPR is realized by a XML file named config.xml, which is stored under the folder "metadata". The config.xml specifies the following parameters and their physical meanings.

| Parameters          | Physical meanings                                            |
| ------------------- | ------------------------------------------------------------ |
| erasure_code_k      | Number of data chunks in a stripe                            |
| erasure_code_n      | Number of data and parity chunks in a stripe                 |
| peer_node_num       | Number of nodes in a system                                  |
| packet_size         | Size of packet in read, transmission, and write (to enable pipelining) |
| chunk_size          | Size of a chunks (also called blocks) in HDFS                |
| meta_size           | Size of metadata of a chunk in HDFS                          |
| stripe_num          | Number of stripes to be repaired                             |
| disk_bandwidth      | Measured disk bandwidth capacity (in unit of  MB/s)          |
| network_bandwidth   | Measured network bandwidth capacity (in unit of Gb/s)        |
| Coordinator_ip      | IP address of the NameNode                                   |
| repair_scenario     | "scatteredRepair" OR “hotStandbyRepair”                      |
| hotstandby_node_num | Number of hot-standby nodes                                  |
| peer_node_ips       | IP addresses of peer nodes                                   |
| hotstandby_node_ips | IP addresses of hot-standby nodes                            |
| local_ip            | IP address of this node                                      |
| local_data_path     | Absolute path that stores the HDFS data chunks (also called blocks) |

## Deployment 

After filling the information in the metadata/config.xml, we can deploy the FastPR as follows: 

1. Extract the files of FastPR, compile the source code of Jerasure and FastPR. 

   ```shell
   $ tar xvf fastpr-v1.0tar.gz
   $ cd fastpr-v1.0/Jerasure
   $ make                          # compile Jerasure library
   $ cd .. && make                 # compile FastPR
   ```

   This command will generate three executable files, named "**HyReCoordinator**", "**HyRePeerNode**", and "**HyReHotStandby**". Note that "HyRe" here means the hybrid usage of migration and reconstruction, i.e., the core idea of FastPR. The roles of these three files are as follows. 

   | Executable files | Roles                                                        |
   | ---------------- | ------------------------------------------------------------ |
   | HyReCoordinator  | I.e., Coordinator in our paper. It will determine which chunks to be reconstructed and migrated, and issue commands to guide the repair. |
   | HyRePeerNode     | It runs as agents in our paper. It receives commands from the Coordinator, parses the commands to understand its role (i.e., sender or receiver), and does the jobs (e.g., read which data chunk and send it to which DataNode, or receive how many data chunks and the name of the repaired chunk) specified in the commands. |
   | HyReHotStandby   | It also runs as agents in our paper. It will dedicatedly receive data for data repair. |

2. Fill in the information (including system information, and the information of erasure coding) in the metadata/config.xml 

   

3. Start the agents at the DataNode. For scattered repair, start "HyRePeerNode" at the DataNodes. 

   ```shell
   $ ./HyRePeerNode 
   ```

   For hot-standby repair, start "HyRePeerNode" at the DataNode of the original system and start "HyReHotStandby" at the DataNode that serves as the hot-standby node 

   ```shell
   $ ./HyRePeerNode            # for DataNode of the original system 
   $ ./HyReHotStandby          # for DataNode to serve as the hot-standby node
   ```



4. Run the HyReCoordinator at the NameNode. The command format is 

   ```shell
   $ ./HyReCoordinator (id_of_STF_node) (repair_method) (num_of_repair_chunks) 
   ```

   

   For example, the following command is to repair the 50 chunks of the STF node (suppose node id is 0) using migration 

   ```shell
   $ ./HyReCoordinator 0 migration 50   # for migration 
   ```

   The following command is to repair the 50 chunks of the STF node (suppose node id is 0) using random repair

   ```shell
   $ ./HyReCoordinator 0 random 50    # for random repair 
   ```

   The following command is to repair the 50 chunks of the STF node (suppose node id is 0) using HyRe (i.e., FastPR) 

   ```shell
   $ ./HyReCoordinator 0 hyre 50      # for FastPR 
   ```

   







