# Databricks - Table Dependency and Workflow Task Optimizer (Transitive Reduction)
*Your databricks python data transform script dependency scanner with workflow task optimizer via NetworkX*

# What is it ??
This repository is designed and developed to support complex data transformation and modeling tasks by maximizing parallelized task execution and simplifying workflow task dependencies. 

![Overview](img/overview.png "Overview")

From the above, if all the nodes are representative of notebook tasks in the databricks workflow. <br/>
Then from the left hand side it can be observed that *collated_data_src_b* is not required to have hard depdendencies to tables being built and ready *bronze_tbl_a...i* if we simplify and hook up all the 
dependencies of *collated_data_src_b* to *collated_data_src_a* and optimize the workflow. 

This is also called as [**Transitive Reduction**](https://en.wikipedia.org/wiki/Transitive_reduction).

![Transitive Reduction](img/transitive_reduction.png "Transitive Reduction")

The python notebook in this repository performs 3 key task as follows:

1. Code Block for reading the python notebook source code in workspace
2. Analyzing the code base, and evaluating the upstream table/volume dependency for the notebook (Regex extraction and variable substitutions, recusrsivly)
3. Creating a NetworkX DiGraph to plot the depedency graph and using the transitive reduction (pre-built) algo, to optimize the workflow task dependency for optimal execution of the notebook



