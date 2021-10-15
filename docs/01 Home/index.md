# Overview

The Aurora4Spark plugin showcases the performance of the Aurora Vector Engine (VE) for Big Data Analytics use cases. The plugin we have created enables developers to accelerate Spark jobs with VE hardware. We believe that the architecture of the VE makes it better suited for these Big Data workloads as it has high-performance support for the typical data types used in Spark jobs and has much larger memory capacity and higher memory bandwidth than all but the recently released A100 GPU.

This documentation outlines the design of a Spark plugin to enable the acceleration of Spark SQL and show benchmark results of the Aurora4Spark system. Several of our optimized functions showcases promising results for the VE. 

We provide a quick guide on how to start using Spark on the VE and sample commands, as well as how to install Aurora4Spark plugin from scratch. We also introduce the concepts related to the implementation of Spark and Kubernetes, instructions to install and finalizes with the benchmark results of the VE compared to CPU-only Spark and GPU-accelerated execution with the Nvidia Rapids4Spark plugin.