# Hadoop and Spark Installation Guide

1\. Download hadoop 3.3.0 and Spark 3.1.0.

In this guide We have used the Spark distribution without hadoop, however you should be able to use the one bundled with Hadoop.

	# wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz
	# wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-without-hadoop.tgz


2\. Create the hadoop user

	# useradd hadoop

3\. Set hadoop user to be able to ssh to localhost without password

	# su hadoop
	$ ssh-keygen
	
**4\. < press enter to accept all defaults>**

	$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
	$ chmod 600 ~/.ssh/authorized_keys
	$ ssh localhost

	The authenticity of host 'localhost (::1)' can't be established.
	ECDSA key fingerprint is SHA256:my/1wiWdA5gz/3agIXPNk4iINUUzbuFSaLXuTectG8M.
	ECDSA key fingerprint is MD5:1b:b7:a3:c7:12:28:e0:98:a6:50:4b:2b:9f:8d:67:2d.
	Are you sure you want to continue connecting (yes/no)? yes
	<yes><enter>
	$ exit
	$ exit
	#
	
### Installing Hadoop

  **5\. Ensure Java is installed**
	
	# java -version
	openjdk version "1.8.0_282"

6\. Create profile.d script to set hadoop variables for all users. Ensure that the JAVA_HOME points to your distribution of JDK. 

	# vi /etc/profile.d/hadoop.sh
  
	#!/usr/bin/sh

	 
	JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.282.b08-1.el7_9.x86_64/
	export JAVA_HOME

	HADOOP_HOME=/opt/hadoop
	export HADOOP_HOME

	HADOOP_MAPRED_HOME=/opt/hadoop
	export HADOOP_MAPRED_HOME

	HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
	export HADOOP_CONF_DIR

	SPARK_HOME=/opt/spark
	export SPARK_HOME

	# chmod 755 /etc/profile.d/hadoop.sh


	# exit

If you are using Spark without Hadoop bundled, consider adding

	export SPARK_DIST_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)

7\. **Login again and verify variables set.**

	  # echo $HADOOP_HOME

8\. **Extract hadoop and spark downloads into /opt/hadoop and /opt/spark**

Ensure that they are extracted such that the path is ```/opt/hadoop/etc``` instead of ```/opt/hadoop/hadoop3.3/etc```

9\. Add getVEsResources.sh

# vi /opt/spark/getVEsResources.sh

	#!/usr/bin/env bash

	ve_addrs=`ls /dev/veslot[0-9] | sed 's/\/dev\/veslot\([0-9]\)/\1/'`
	addr_list=`echo $ve_addrs | sed 's/\([0-9]\)/"\1"/g' | tr ' ' ,`

	echo {\"name\": \"ve\", \"addresses\":[$addr_list]}
  

10\. Add ve-spark-shell.sh

	# vi /opt/spark/ve-spark-shell.sh

	#!/usr/bin/env bash

	$SPARK_HOME/bin/spark-shell --master yarn \
		--conf spark.driver.resource.ve.amount=1 \
		--conf spark.executor.resource.ve.amount=1 \
		--conf spark.task.resource.ve.amount=1 \
		--conf spark.driver.resource.ve.discoveryScript=$SPARK_HOME/getVEsResources.sh \
		--conf spark.executor.resource.ve.discoveryScript=$SPARK_HOME/getVEsResources.sh \
		--files $SPARK_HOME/getVEsResources.sh

11\. Change ownership of /opt/hadoop /opt/spark to hadoop user.

	# chown -R hadoop /opt/hadoop
	# chown -R hadoop /opt/spark
	# chgrp -R hadoop /opt/hadoop
	# chgrp -R hadoop /opt/spark

12\. Install pdsh

	# yum install pdsh


13\. Set hadoop configuration. Take note that if you have no GPUs installed in your system, exclude the GPU related configurations in yarn-site.xml, container-executor.cfg, and resource-types.xml.

	# su hadoop
	$ vi /opt/hadoop/etc/hadoop/core-site.xml
	 
	<configuration>
	    <property>
	        <name>fs.defaultFS</name>
	        <value>hdfs://localhost:9000</value>
	    </property>
	</configuration>
	 
	$ vi /opt/hadoop/etc/hadoop/hdfs-site.xml
	 
	<configuration>
	    <property>
	        <name>dfs.replication</name>
	        <value>1</value>
	    </property>
	</configuration>
	 
	$ vi /opt/hadoop/etc/hadoop/mapred-site.xml
	 
	<configuration>
	    <property>
	        <name>mapreduce.framework.name</name>
	        <value>yarn</value>
	    </property>
	    <property>
	        <name>mapreduce.application.classpath</name>
	        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
	    </property>
	</configuration>
	 
	$ vi /opt/hadoop/etc/hadoop/yarn-site.xml
	 
	<configuration>
	    <property>
	        <name>yarn.nodemanager.aux-services</name>
	        <value>mapreduce_shuffle</value>
	    </property>
	    <property>
	        <name>yarn.nodemanager.env-whitelist</name>
	        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
	    </property>
	    <property>
	        <name>yarn.nodemanager.pluggable-device-framework.enabled</name>
	        <value>true</value>
	    </property>
	    <property>
	        <name>yarn.nodemanager.pluggable-device-framework.device-classes</name>
	        <value>org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nvidia.NvidiaGPUPluginForRuntimeV2,org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nec.NECVEPlugin</value>
	    </property>
	</configuration>
	 
	$ vi /opt/hadoop/etc/hadoop/container-executor.cfg
	 
	[gpu]
	module.enabled=true 
	[devices]
	module.enabled=true
	 
	$ vi /opt/hadoop/etc/hadoop/resource-types.xml
	 
	<configuration>
	    <property>
	        <name>yarn.resource-types</name>
	        <value>nvidia.com/gpu,nec.com/ve</value>
	    </property>
	</configuration>


**14\. Edit the following section of capacity-scheduler.xml and change the DefaultResourceCalculator to the DominantResourceCalculator setting below:**

	$ vi /opt/hadoop/etc/hadoop/capacity-scheduler.xml
	 
	<property>
	    <name>yarn.scheduler.capacity.resource-calculator</name>
	    <value>org.apache.hadoop.yarn.util.resource.DominantResourceCalculator</value>
	    <description>
	      The ResourceCalculator implementation to be used to compare
	      Resources in the scheduler.
	      The default i.e. DefaultResourceCalculator only uses Memory while
	      DominantResourceCalculator uses dominant-resource to compare
	      multi-dimensional resources such as Memory, CPU etc.
	    </description>
	  </property>
  

15\. Set up HDFS and YARN

	$ cd /opt/hadoop
	$ bin/hdfs namenode -format
	$ sbin/start-dfs.sh
	$ bin/hdfs dfs -mkdir /user
	$ bin/hdfs dfs -mkdir /user/hadoop
	$ bin/hdfs dfs -mkdir /user/<otheruser>
	$ bin/hdfs dfs -chown <otheruser> /user/<otheruser>

**Repeat the mkdir and chown for <otheruser> for any other users on the system.**

16\. Setup GPU and Vector engine settings and scripts

	$ mkdir /opt/hadoop/sbin/DevicePluginScript
	$ vi /opt/hadoop/sbin/DevicePluginScript/nec-ve-get.py
	 
	#!/usr/bin/env python
	import os
	from subprocess import Popen, PIPE
	 
	vecmd = Popen('/opt/nec/ve/bin/vecmd info', shell=True, stdout=PIPE)
	 
	lines = []
	for line in vecmd.stdout:
	    lines.append(line.decode('utf-8').strip())
	 
	ve_count = 0
	ves = []
	current_ve = None
	ve_id = 0
	 
	for line in lines:
	    if line.startswith('Attached VEs'):
	        ve_count = int(line.split()[-1])
	 
	    if line.startswith('[VE'):
	        if current_ve != None:
	            ves.append(current_ve)
	        current_ve = {}
	        ve_id += 1
	        current_ve['id'] = ve_id
	        current_ve['dev'] = '/dev/ve' + str(ve_id - 1)
	        dev = os.lstat(current_ve['dev'])
	        current_ve['major'] = os.major(dev.st_rdev)
	        current_ve['minor'] = os.minor(dev.st_rdev)
	 
	    if line.startswith('VE State'):
	        current_ve['state'] = line.split()[-1]
	 
	    if line.startswith('Bus ID'):
	        current_ve['busId'] = line.split()[-1]
	 
	ves.append(current_ve)
	 
	for ve in ves:
	    print("id={id}, dev={dev}, state={state}, busId={busId}, major={major}, minor={minor}".format(**ve))

17\. Start YARN services

	$ sbin/start-yarn.sh

18\. Verify spark shell sees VE resources

	$ /opt/spark/ve-spark-shell.sh
	2021-05-14 13:38:47,468 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
	Setting default log level to "WARN".
	To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
	2021-05-14 13:38:54,129 WARN yarn.Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
	Spark context Web UI available at http://aurora06:4040
	Spark context available as 'sc' (master = yarn, app id = application_1620964947383_0002).
	Spark session available as 'spark'.
	Welcome to
	      ____              __
	     / __/__  ___ _____/ /__
	    _\ \/ _ \/ _ `/ __/  '_/
	   /___/ .__/\_,_/_/ /_/\_\   version 3.1.1
	      /_/
	         
	Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 1.8.0_282)
	Type in expressions to have them evaluated.
	Type :help for more information.
	 
	scala> sc.resources
	res0: scala.collection.Map[String,org.apache.spark.resource.ResourceInformation] = Map(ve -> [name: ve, addresses: 0,1])
