# Databricks - Table Dependency and Workflow Task Optimizer (Transitive Reduction)
*Your databricks python data transform script dependency scanner with workflow task optimizer via NetworkX*

# What is it ??
This repository is designed and developed to support complex data transformation and modeling tasks by maximizing parallelized task execution and simplifying workflow task dependencies. 

The python notebook in this repository performs 3 key task as follows:

1. Code Block for reading the python notebook source code in workspace
2. Analyzing the code base, and evaluating the upstream table/volume dependency for the notebook (Regex extraction and variable substitutions, recusrsivly)
3. Creating a NetworkX DiGraph to plot the depedency graph and using the transitive reduction (pre-built) algo, to optimize the workflow task dependency for optimal execution of the notebook



