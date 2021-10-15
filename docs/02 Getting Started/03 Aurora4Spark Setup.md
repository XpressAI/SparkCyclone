# Aurora4Spark Installation Guide


Before proceeding with the installation, ensure that you have completed the Hadoop and Spark setup for the Vector Engine.

## 1. Update Hadoop YARN VE Resource Allocation

Depending on the number of VE and RAM available, please adjust the numbers in yarn-site.xml accordingly. The following configurations was for 2 VEs.

	$ vi /opt/hadoop/etc/hadoop/yarn-site.xml

	<property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>104448</value>
    </property> 
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>13056</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>24</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-vcores</name>
        <value>24</value>
    </property>

## 2. Check Hadoop Status

If you are running the job from another user such as root, ensure that the user has been added from user hadoop.

    # cd /opt/hadoop/
	$ bin/hdfs dfs -mkdir /user/<otheruser>
	$ bin/hdfs dfs -chown <otheruser> /user/<otheruser>

Start Hadoop

    $ sbin/start-dfs.sh
    $ sbin/start-yarn.sh

Open Hadoop YARN Web UI to verify that the settings are updated. 

    # if you are SSHing into the server from a remote device, don't forget to forward your port.
    $ ssh root@serveraddress -L 8088:localhost:8088 

As seen from the ```Cluster Nodes``` tab, the Memory Total, VCores Total, as well as Maximum Allocation is updated.

![image](https://user-images.githubusercontent.com/68586800/137414646-4ce66a4e-2f4f-4817-a5a1-686ab349a2a3.png)


## 3. Build Tools

Ensure that you have both java and javac installed. You also need sbt and java-devel.

	$ yum install sbt
	$ yum install java-devel

## 3. Clone Aurora4Spark repo and build

	$ git clone https://github.com/XpressAI/aurora4spark
	$ cd aurora4spark
	$ sbt deploy local

Now that Aurora4Spark has been setup, Examples: Try out the Runnning TPCH Benchmarks in the Examples Section.