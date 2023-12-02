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

The aim of the project is to get people engaged with how different parts of the country have the best performing wind farms (in terms of capacity factor) and how this might be changing over time.

The code has been written by James Twallin and has been made public so that anyone can add to it or make suggestions. If you wish to do so, please fork the repo and make a pull request.