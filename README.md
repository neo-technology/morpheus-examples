# Morpheus Examples project
This project contains examples of how to use Morpheus for Apache Spark. 

## Examples

These are fully runnable programs with example usage of the Morpheus APIs, with sample datasets.
The examples tend to focus on a certain use case, showcasing a small solution for it.  

## Templates

These are abstract dataflow programs with imagined datasets, and aim to showcase more advanced usage of federated Cypher queries.

## Branches and compatibility

There are two main branches on this repository: `master` and `stable`.

- `stable` will always be up to date with the latest stable Morpheus release, and should be runnable if you have setup your Maven `settings.xml` file to contain credentials for the `morpheus-release` repository (see `pom.xml` for details).
- `master` is ahead of `stable` containing changes that match the latest development of Morpheus.
It has a `1.0.0-SNAPSHOT` dependency on Morpheus that is assumed to contain the latest state.
Use this branch if you want to test Morpheus features that are still in development and have not yet been delivered in a stable release.

# Contact

This repository hosts example content and documentation for a product named Morpheus owned and developed by [Neo4j](https://neo4j.com).
If you are interested in the product and would like to know more, please reach out to us at `alastair.green@neo4j.com` or `mats@neo4j.org`. 

# License and copyright

All resources in this repository are copyright (c) Neo4j Sweden, AB.
The content is available for use under the terms specified in the Neo4j Pre-Release Software Agreement; please see LICENSE for details. 

