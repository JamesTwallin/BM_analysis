---
title: How it works
author: James Twallin
date: 2023-12-01
category: windfarm
layout: post
---

The code has been written by James Twallin can be found on [GitHub][1]. 

Ingredients
-------------

## Weather data

Data comes from ECMWF. The data has been made available via an AWS S3 bucket. The data is stored in a NetCDF format [here][2] thanks to the [AWS Open Data Program][2].


## Wind farm data

In order to access data from the wind farms, we need to use the [BMRS API](https://www.elexon.co.uk/guidance-note/bmrs-api-data-push-user-guide/). This is a REST API that allows us to access data from the Balancing Mechanism Reporting System (BMRS). The BMRS is a reporting service for historic and near real-time data about the electricity transmission system in Great Britain.

The 2 endpoints we are interested in are:

B1610 (This is the Actual Generation Output Per Generation Unit) and the Derived Data (This is the data which show the actions taken by the National Grid to balance the system). If we combine the 2 datasets, we can see how much each wind farm generated (B1610) and if they were curtailed at all (Derived Data).


[1]: https://github.com/JamesTwallin/BM_analysis
[2]: https://registry.opendata.aws/ecmwf-era5/