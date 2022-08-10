.. _quickstart:

*****************
Quickstart Guide - Kubernetes
*****************

This quickstart will take approximately xx minutes to complete.

Introduction
=============

NEXUS is a collection of software that enables the analysis of scientific data. In order to achieve fast analysis, NEXUS takes the approach of breaking apart, or "tiling", the original data into smaller tiles for storage. Metadata about each tile is stored in a fast searchable index with a pointer to the original data array. When an analysis is requested, the necessary tiles are looked up in the index and then the data for only those tiles is loaded for processing.

This quickstart guide will walk you through how to install and run NEXUS on your laptop. By the end of this quickstart, you should be able to run a time series analysis for one month of sea surface temperature data and plot the result.

System Requirements
--------------------

This guide was developed and tested using a MacBook Pro with a dual-core CPU (2.5 GHz) and 16 GBs of RAM. The Helm chart used in this guide was designed to accommodate these performance limitations.

For server deployments (on hardware with more memory and processor power available), there is a separate Helm chart in the ``/helm`` directory of this repository.

.. _quickstart-prerequisites:

Prerequisites
==============

* Docker Desktop (for Kubernetes cluster & to run the test container) (tested on v????)
* Internet Connection
* bash or zsh
* cURL
* 8.5 GB of disk space