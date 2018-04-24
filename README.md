Team and repository tags
========================

[![Team and repository tags](https://governance.openstack.org/badges/monasca-transform.svg)](https://governance.openstack.org/reference/tags/index.html)

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Monasca Transform](#monasca-transform)
  - [Use Cases handled by Monasca Transform](#use-cases-handled-by-monasca-transform)
  - [Operation](#operation)
  - [Architecture](#architecture)
  - [To set up the development environment](#to-set-up-the-development-environment)
  - [Generic aggregation components](#generic-aggregation-components)
  - [Create a new aggregation pipeline example](#create-a-new-aggregation-pipeline-example)
  - [Original proposal and blueprint](#original-proposal-and-blueprint)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Monasca Transform

monasca-transform is a data driven aggregation engine which collects, groups and aggregates existing
individual Monasca metrics according to business requirements and publishes new transformed
(derived) metrics to the Monasca Kafka queue.

  * Since the new transformed metrics are published as any other metric in Monasca, alarms can be
    set and triggered on the transformed metric.

  * Monasca Transform uses [Apache Spark](http://spark.apache.org) to aggregate data. [Apache
    Spark](http://spark.apache.org) is a highly scalable, fast, in-memory, fault tolerant and
    parallel data processing framework. All monasca-transform components are implemented in Python
    and use Spark's [PySpark Python API](http://spark.apache.org/docs/latest/api/python/index.html)
    to interact with Spark.

  * Monasca Transform does transformation and aggregation of incoming metrics in two phases.

    * In the first phase spark streaming application is set to retrieve in data from kafka at a
      configurable *stream interval* (default *stream_inteval* is 10 minutes) and write the data
      aggregated for *stream interval* to *pre_hourly_metrics* topic in kafka.

    * In the second phase, which is kicked off every hour, all metrics in *metrics_pre_hourly* topic
      in Kafka are aggregated again, this time over a larger interval of an hour. These hourly
      aggregated metrics published to *metrics* topic in kafka.

## Use Cases handled by Monasca Transform ##
Please refer to **Problem Description** section on the [Monasca/Transform
wiki](https://wiki.openstack.org/wiki/Monasca/Transform)

## Operation ##
Please refer to **How Monasca Transform Operates** section on the [Monasca/Transform
wiki](https://wiki.openstack.org/wiki/Monasca/Transform)

## Architecture ##
Please refer to **Architecture** and **Logical processing data flow** sections on the
[Monasca/Transform wiki](https://wiki.openstack.org/wiki/Monasca/Transform)

## To set up the development environment ##
The monasca-transform uses [DevStack](https://docs.openstack.org/devstack/latest/) as a common dev
environment. See the [README.md](devstack/README.md) in the devstack directory for details on how
to include monasca-transform in a DevStack deployment.

## Generic aggregation components ##

Monasca Transform uses a set of generic aggregation components which can be assembled in to an
aggregation pipeline.

Please refer to [generic aggregation components](docs/generic-aggregation-components.md) document for
information on list of generic aggregation components available.

## Create a new aggregation pipeline example ##

Generic aggregation components make it easy to build new aggregation pipelines for different Monasca
metrics.

This create a [new aggregation pipeline](docs/create-new-aggregation-pipeline.md) example shows how to
create *pre_transform_specs* and *transform_specs* to create an aggregation pipeline for a new set
of Monasca metrics, while leveraging existing set of generic aggregation components.


## Original proposal and blueprint ##

Original proposal:
[Monasca/Transform-proposal](https://wiki.openstack.org/wiki/Monasca/Transform-proposal)

Blueprint: [monasca-transform
blueprint](https://blueprints.launchpad.net/monasca/+spec/monasca-transform)
