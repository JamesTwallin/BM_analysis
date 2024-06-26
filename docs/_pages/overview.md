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


## Energy Yield Calculations

By using known data (generation, curtailment and weather data), we can create a model which can estimate the expected generation at a wind farm given the wind speed and direction. In this work, the approach taken is to use instance based machine learning. The basis of instance based modelling is the premise that **the model is the data**. The most well known instance based model is the KNN Regressor, which is like a machine learning version of a VLOOKUP in excel. It's conceptually not too difficult to understand. If we make the assertion:

`power_out = f(wind_speed, direction)`

Then this means that we can just use historic data to predict what the wind farm's output would be given a set of already known inputs.

This results in something which looks like this:

![]({{ site.baseurl }}/assets/1_SHRSO-1_unseen.png)

### Method

1. Get the weather data and wind farm data
2. Use the B1610 data to get the actual generation for each wind farm, add the curtailment data back into the generation data to get the corrected generation data
3. Train an ML model to understand how weather data influences wind farm output
4. Use the ML model to make predictions for generation going back many years (to 1990), by making predictions using ERA5 reanalysis datasets*
5. Use both the actual and predicted generation data to understand what a typical year's energy production is for each wind farm

*in order to get a full picture, we need to consider that we're talking about a typical year (i.e. not only for the years we have data for). In order to do this, we use ERA5 reanalysis data to create a hindcast.

[1]: https://github.com/JamesTwallin/BM_analysis
[2]: https://registry.opendata.aws/ecmwf-era5/