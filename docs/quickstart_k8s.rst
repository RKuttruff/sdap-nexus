.. _quickstart:

*****************
Quickstart Guide - Kubernetes
*****************

This quickstart will take approximately xx minutes to complete.

Introduction
=============

NEXUS is a collection of software that enables the analysis of scientific data. In order to achieve fast analysis, NEXUS takes the approach of breaking apart, or "tiling", the original data into smaller tiles for storage. Metadata about each tile is stored in a fast searchable index with a pointer to the original data array. When an analysis is requested, the necessary tiles are looked up in the index and then the data for only those tiles is loaded for processing.

This quickstart guide will walk you through how to install and run NEXUS on your laptop. By the end of this quickstart, you should be able to run a time series analysis for one month of sea surface temperature data and plot the result.

.. _quickstart-sys-requirements:

System Requirements
--------------------

This guide was developed and tested using a MacBook Pro with a dual-core CPU (2.5 GHz) and 16 GBs of RAM. The Helm chart used in this guide was designed to accommodate these performance limitations.

For server deployments (on hardware with more memory and processor power available), there is a separate Helm chart in the ``/helm`` directory of this repository.

.. _quickstart-prerequisites:

Prerequisites
==============

* Docker Desktop (for Kubernetes cluster & to run the test container) (tested on v4.10.1)
* Internet Connection
* bash or zsh
* cURL
* `Spark Operator <https://github.com/helm/charts/tree/master/incubator/sparkoperator>`_
* 8.5 GB of disk space

Setup
======

.. _quickstart-spark-operator:

Install Spark Operator
-------------------

NEXUS needs `Spark Operator <https://github.com/helm/charts/tree/master/incubator/sparkoperator>`_ in order to run. To install, run:

.. code-block:: bash

  kubectl create namespace spark-operator
  helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
  helm install my-release spark-operator/spark-operator --namespace spark-operator --set image.tag=v1beta2-1.3.3-3.1.1

.. _quickstart-volumes:

Persistent Volumes
------------------

The RabbitMQ, Solr, Zookeeper, Cassandra, and Collection Manager (ingestion) components of SDAP need to be able to store data. In order to have persistent storage, you need to have a Storage Class defined and have Persistent Volumes provisioned either manually or dynamically. See `Persistent Volumes <https://kubernetes.io/docs/concepts/storage/persistent-volumes/>`_.

.. tip::

  If you are using an NFS server as storage, you can use `nfs-client-provisioner <https://github.com/helm/charts/tree/master/stable/nfs-client-provisioner>`_ to dynamically provision persistent volumes on your NFS server.

.. note::

  If you are using Docker Desktop's bundled Kubernetes cluster, this will already be taken care of for you with the provided Helm chart.

.. _quickstart-chart-install:

Installing the Chart
=====================

First clone the NEXUS Git repository if you haven't already.

.. code-block:: bash

  git clone https://github.com/apache/incubator-sdap-nexus.git

Then install the Helm chart.

.. _quickstart-chart-install-note:

Before You Install
------------------

Make sure to familiarize yourself with the parameters as described in the `Helm chart readme <https://github.com/apache/incubator-sdap-nexus/tree/master/helm#parameters>`_.

Also, you should familiarize yourself with the collection configuration documentation in the `Helm chart readme <https://github.com/apache/incubator-sdap-nexus/tree/master/helm#the-collections-config>`_.

.. _quickstart-chart-install-personal:

Deployment for Personal Machines
----------------------------------

Deployments on machines with limited resources (ie, desktops and laptops) should use the chart associated with this guide, which was derived from the main chart with these performance constraints in mind.

Note that you will need to set the ``ingestion.granules.path`` in the file ``<repository-root>/docs/helm/values.yml`` to the path of the directory in which you are storing data granules if you're storing the granules on your local filesystem.

.. code-block:: bash

  cd incubator-sdap-nexus/docs
  helm create namespace sdap
  kubectl create configmap collections-config --from-file=collections.yml -n sdap
  helm install nexus helm --namespace=sdap --dependency-update

.. _quickstart-chart-install-hp:

Deployment for Higher Performance Machines
-----------------------

For deploying on a higher performance machine. You should use the helm chart in the root directory of the NEXUS repository.

Be aware that you will have to configure your own collections configuration file or use one in Git as described in the Helm chart documentation linked above.

.. code-block:: bash

  helm create namespace sdap
  # If you have a local collection config YAML file, add it to the cluster here using kubectl create configmap collections-config --from-file=<your-cfc-yml-file> -n sdap
  helm install nexus helm --namespace=sdap --dependency-update

.. note::

  It may take a few minutes for the ``nexus-webapp-driver`` pod to start up because this depends on Solr and Cassandra being accessible.

.. _quickstart-chart-verify:

Verifying Successful Installation
==================================

Check that all the pods are up by running ``kubectl get pods -n sdap``, and make sure all pods have status Running. If any pods have not started within a few minutes, you can look at its status with ``kubectl describe pod -n sdap <pod-name>``.

Option 1: Local Deployment With Ingress Enabled
------------------------------------------------
If you have installed the Helm chart locally with ``ingressEnabled`` set to ``true`` (see ``ingressEnabled`` under `Configuration <https://github.com/apache/incubator-sdap-nexus/tree/master/helm#configuration>`_), you can verify the installation by requesting the ``list`` endpoint. If this returns an HTTP 200 response, NEXUS is healthy.

.. code-block:: bash

  curl localhost/nexus/list

Option 2: No Ingress Enabled
------------------------------

If you have installed the Helm chart on a cloud provider, and/or have not enabled a load balancer with ``ingressEnabled=true``, you can temporarily port-forward the ``nexus-webapp`` port to see if the webapp responds.

First, on the Kubernetes cluster or jump host, create a port-forward to the ``nexus-webapp`` service:

.. code-block:: bash

  kubectl port-forward service/nexus-webapp -n sdap 8083:8083

Then open another shell on the same host and request the list endpoint through the forwarded port:

.. code-block:: bash

  curl localhost:8083/list

.. note::

  In this case the list endpoint is ``/list`` instead of ``/nexus/list`` because we are connecting to the ``nexus-webapp`` service directly, instead of through an ingress rule.

If the request returns an HTTP 200 response, NEXUS is healthy. You can now close the first shell to disable the port-forward.

If one of the pods or deployment is not started, you can look at its status with:

.. code-block:: bash

  kubectl describe pod <pod-name> -n sdap

.. _quickstart-chart-uninstall:

Uninstalling the Chart
========================

To uninstall/delete the ``nexus`` deployment:

.. code-block:: bash

  helm delete nexus -n sdap

The command removes all the Kubernetes components associated with the chart and deletes the release.