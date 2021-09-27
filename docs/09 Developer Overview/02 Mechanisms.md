# Mechanism

## Entry Point Mechanism
The aurora4spark-sql-plugin implements the main device extension method available in the Spark API.  It implements a SparkPlugin that offers a driver and executor plugin as follows:

![image](https://user-images.githubusercontent.com/68586800/130805169-71796510-6a1a-449c-b0cd-9bab31e520f4.png)

## Internal Mechanism 

The Aurora4SparkDriver and Aurora4SparkExecutorPlugin classes are the entry points to the application.  The Executor plugin will run on all the Spark Executors and the Driver runs only on the Spark Driver.  The driver is responsible for telling spark which extensions to use.  The LocalVeoExtension class is then used to inject a new planning optimizer strategy that replaces query plans with our own plan implementations that run on the VE.

![image](https://user-images.githubusercontent.com/68586800/130805660-5deeaee4-6e8a-454d-a260-2677f3f99539.png)

The plan classes such as CEvaluationPlan are the classes that are used to actually execute the query when during the evaluation of the rewritten plans.  These implementations essentially receive a special RDD with the data to process.  It is up to the plan implementation to execute the code on the VE and return the results.  A sample of the ArrowSummingPlan is shown to illustrate this:

![image](https://user-images.githubusercontent.com/68586800/130805765-a086a9b9-e40f-4852-9d5e-13a78f80f523.png)

Inside of these implementations you will typically see a runOn method that calls functions using the ArrowNativeInterface.  The implementations of this trait handle the conversion of arguments into Arrow formats and calling the VE or CPU function either by calling JNA (in the case of the CPU) or AVEO (for VE). 

![image](https://user-images.githubusercontent.com/68586800/130805920-b9408ea5-0795-44c0-aa61-48154031448a.png)

The source for the simple C functions can be found inside the resources directory in the project. Inside this code it is possible and necessary to use all the normal features of NCC to vectorize the code for running on the VE.

![image](https://user-images.githubusercontent.com/68586800/130806050-51974d24-e0fb-412e-8a01-060d1e064c01.png)

However most of the C code for queries are generated on the fly using information from the current query.

![image](https://user-images.githubusercontent.com/68586800/130806094-a1364bd8-99b9-4e1d-b172-a1ebf987fe0c.png)

## CEvaluation Plan as WholeStageCodeGen
