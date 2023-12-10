---
title: How it works
author: James Twallin
date: 2023-12-01
category: windfarm
layout: post
---

The code has been written by James Twallin can be found on [GitHub][1]. 

## Prerequisite: Data

### Weather data

Data comes from ECMWF. The data has been made available via an AWS S3 bucket. The data is stored in a NetCDF format [here][2] thanks to the [AWS Open Data Program][2].


### Wind farm data

In order to access data from the wind farms, we need to use the [BMRS API](https://www.elexon.co.uk/guidance-note/bmrs-api-data-push-user-guide/). This is a REST API that allows us to access data from the Balancing Mechanism Reporting System (BMRS). The BMRS is a reporting service for historic and near real-time data about the electricity transmission system in Great Britain.

The 2 endpoints we are interested in are:

B1610 (This is the Actual Generation Output Per Generation Unit) and the Derived Data (This is the data which show the actions taken by the National Grid to balance the system). If we combine the 2 datasets, we can see how much each wind farm generated (B1610) and if they were curtailed at all (Derived Data).


### Energy Yield Calculations

By using known data (generation, curtailment and weather data), we can create a model which can estimate the expected generation at a wind farm given the wind speed and direction. In this work, the approach taken is to use instance based machine learning. The basis of instance based modelling is the premise that the model is the data. The most well known instance based model is the KNN Regressor, which is like a machine learning version of a VLOOKUP in excel. It's conceptually not too difficult to understand. If we make the assertion:

`power_out = f(wind_speed, direction)`

Then this means that we can just use historic data to predict what the wind farm's output would be given a set of already known inputs.

### Method

1. Get the weather data and wind farm data
2. Train an ML model to understand how weather data influences wind farm output
3. Use the ML model to make predictions for generation going back many years (to 1990), by making predictions using ERA5 reanalysis datasets
4. Use both the actual and predicted generation data to understand what a typical year's energy production is for each wind farm


[1]: https://github.com/JamesTwallin/BM_analysis
[2]: https://registry.opendata.aws/ecmwf-era5/