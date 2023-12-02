# BM Analysis


Use miniconda and the environment.yml file to create a conda environment with all the dependencies:

```conda env create -f environment.yml```

Activate the environment:

```conda activate bm_analysis```

To download the data, you'll need a `config.ini` file in the root directory with the following format:

```
[bmrs]
api_key = <your api key>
```

## How it works

This repo has been created so that anyone can use their API key to download data from the BMRS API and use this data to analyse the performance of wind farms in the UK.

In order to access data from the wind farms, we need to use the [BMRS API](https://www.elexon.co.uk/guidance-note/bmrs-api-data-push-user-guide/). This is a REST API that allows us to access data from the Balancing Mechanism Reporting System (BMRS). The BMRS is a centralised repository for historic and near real-time data about the electricity transmission system in Great Britain.

The data comes from ECMWF. The data has been made available via an AWS S3 bucket. The data is stored in a NetCDF format [here][2] thanks to the [AWS Open Data Program][2]. Unfortunately as of 2023-12-01, an announcement to deprecate the data has been made. I'll be looking for alternative sources of data in the future.

# Why?

The aim of the project is to get people engaged with how different parts of the country have the best performing wind farms (in terms of capacity factor) and how this might be changing over time.

The code has been written by James Twallin and has been made public so that anyone can add to it or make suggestions. If you wish to do so, please fork the repo and make a pull request.