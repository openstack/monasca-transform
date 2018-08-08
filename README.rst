Team and repository tags
========================

.. image:: https://governance.openstack.org/badges/monasca-analytics.svg
    :target: https://governance.openstack.org/reference/tags/index.html

-  `Monasca Transform`_

   -  `Use Cases handled by Monasca Transform`_
   -  `Operation`_
   -  `Architecture`_
   -  `To set up the development environment`_
   -  `Generic aggregation components`_
   -  `Create a new aggregation pipeline example`_
   -  `Original proposal and blueprint`_

Monasca Transform
=================

monasca-transform is a data driven aggregation engine which collects,
groups and aggregates existing individual Monasca metrics according to
business requirements and publishes new transformed (derived) metrics to
the Monasca Kafka queue.

-  Since the new transformed metrics are published as any other metric
   in Monasca, alarms can be set and triggered on the transformed
   metric.

-  Monasca Transform uses `Apache Spark`_ to aggregate data. `Apache
   Spark`_ is a highly scalable, fast, in-memory, fault tolerant and
   parallel data processing framework. All monasca-transform components
   are implemented in Python and use Spark’s `PySpark Python API`_ to
   interact with Spark.

-  Monasca Transform does transformation and aggregation of incoming
   metrics in two phases.

   -  In the first phase spark streaming application is set to retrieve
      in data from kafka at a configurable *stream interval* (default
      *stream_inteval* is 10 minutes) and write the data aggregated for
      *stream interval* to *pre_hourly_metrics* topic in kafka.

   -  In the second phase, which is kicked off every hour, all metrics
      in *metrics_pre_hourly* topic in Kafka are aggregated again, this
      time over a larger interval of an hour. These hourly aggregated
      metrics published to *metrics* topic in kafka.

Use Cases handled by Monasca Transform
--------------------------------------

Please refer to **Problem Description** section on the
`Monasca/Transform wiki`_

Operation
---------

Please refer to **How Monasca Transform Operates** section on the
`Monasca/Transform wiki`_

Architecture
------------

Please refer to **Architecture** and **Logical processing data flow**
sections on the `Monasca/Transform wiki`_

To set up the development environment
-------------------------------------

The monasca-transform uses `DevStack`_ as a common dev environment. See
the `README.md`_ in the devstack directory for details on how to include
monasca-transform in a DevStack deployment.

Generic aggregation components
------------------------------

Monasca Transform uses a set of generic aggregation components which can
be assembled in to an aggregation pipeline.

Please refer to the
`generic-aggregation-components`_
document for information on list of generic aggregation components
available.

Create a new aggregation pipeline example
-----------------------------------------

Generic aggregation components make it easy to build new aggregation
pipelines for different Monasca metrics.

This create a `new aggregation pipeline`_ example shows how to create
*pre_transform_specs* and *transform_specs* to create an aggregation
pipeline for a new set of Monasca metrics, while leveraging existing set
of generic aggregation components.

Original proposal and blueprint
-------------------------------

Original proposal: `Monasca/Transform-proposal`_

Blueprint: `monasca-transform blueprint`_

.. _Apache Spark: http://spark.apache.org
.. _generic-aggregation-components: docs/generic-aggregation-components.md
.. _PySpark Python API: http://spark.apache.org/docs/latest/api/python/index.html
.. _Monasca/Transform wiki: https://wiki.openstack.org/wiki/Monasca/Transform
.. _DevStack: https://docs.openstack.org/devstack/latest/
.. _README.md: devstack/README.md
.. _new aggregation pipeline: docs/create-new-aggregation-pipeline.md
.. _Monasca/Transform-proposal: https://wiki.openstack.org/wiki/Monasca/Transform-proposal
.. _monasca-transform blueprint: https://blueprints.launchpad.net/monasca/+spec/monasca-transform
