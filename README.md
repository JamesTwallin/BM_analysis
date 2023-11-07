# BMPowerCurves

![alt text](/plots/BEATO-2_power_curve.jpg "Power curve for BEATO-2")

Use mininconda and the environment.yml file to create a conda environment with all the dependencies:

```conda env create -f environment.yml```

Activate the environment:

```conda activate bmpowercurves```

To download the data, you'll need a `config.ini` file in the root directory with the following format:

```
[bmrs]
api_key = <your api key>
```

## How it works

The `main.py` function is the one you want to run. There are 2 datasets it uses:
- weather data
- generation data

The main code joins the 2 together and fits a logistic curve to the data. A logistic curve takes the format:
    
    ```y = a / (1 + exp(-b(x - c)))```

where `a`, `b` and `c` are parameters to be fitted. The curve is fitted using the `scipy.optimize.curve_fit` function.


##### Why parquet?
'cos it's small and compressed and it remembers the data types. It's also easy to read in with pandas.

#### n.b.

The data in this repo exists only as an example. If you want more data, please run the python files: `windfarm_generation.py` and `weather.py`. These will download the data from the BMRS API and the ERA5 open data s3 bucket save it to the `gen_data`  and  `weather_data` directories respectively.


