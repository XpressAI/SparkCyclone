# Repository Organization

The following document details the repository organization of the Aurora4Spark project for developers.

The top level of the aurora4spark project has the following layout.  It contains folders for both documentation and code.
* The aveo4j folder contains the JavaCPP presets for the Alternative VE Offloading library. The build of the aveo4j repo and available as a maven repository to simplify the build of aurora4spark. 
* "docs" will contain markdown formatted text files which documents the plugin usage
* "examples" has example python scripts that show users how to use the library.  
* The "src" folder contains the main source code is for the Spark plug-in.

Inside the src folder we have both the Scala source code and the C++ source code that is used in the plugin.

The C++ source files are used in some of the simple plans but in general mostly serve as examples as the plugin generates C++ code on the fly for most queries.

In the Scala code you will see folders for:
* "arrow" Contains the low level interface for transfering data and executing functions on the VE.
* "cmake" Contains logic for running queries on CPU which is used to ease development.
* "native" contains the code for running the NCC compiler to generate the VE and/or CPU code.
* "spark" contains the implementation of the plugin.  Of which the CEvaluationPlan and CExpressionEvaluation classes are the most important as they contain the C++ code generation logic.
