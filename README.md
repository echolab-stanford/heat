# `heat` : Harmonized Environmental Exposure Aggregation Tools

[![](https://zenodo.org/badge/1113902927.svg)](https://doi.org/10.5281/zenodo.17882617)
[![R Tests](https://github.com/echolab-stanford/heat/actions/workflows/r-tests.yml/badge.svg)](https://github.com/echolab-stanford/heat/actions/workflows/r-tests.yml)

The `heat` R package provides a comprehensive set of tools to compute environmental exposures for administrative boundaries or point locations. Its main aggregation function, `r2e2`, supports various nonlinear transformations (e.g., polynomial, splines, binning), temporal aggregations (e.g., daily, monthly, yearly), and scales efficiently to large raster datasets spanning multiple decades.

## Installation

``` r

# Install remotes if needed
if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes")
}

# Install heat
devtools::install_github("echolab-stanford/heat")
```

## Quick Start

### Example 1: Polynomial Transformation and Aggregation to Monthly 
``` r
library(heat)
library(sf)
library(terra)

# Load dataframe containing an sf geometry (polygons or points)
regions <- st_read("regions.shp")

# Load environmental raster
temperature <- rast("temperature.tif")

# Polynomial transformation and aggregation to monthly 
exposures <- r2e2(
  env_rast = temperature,            # Environmental raster
  geometry = regions,                # Spatial geometries
  trans_type = "polynomial",         # Transformation type
  degree = 4,                        # Polynomial degree
  out_temp_res = "monthly"           # Aggregate to monthly
)
```
```r
# Access results
head(exposures$monthly_long)

#>   geom_id  trans_type    date  year month   degree_1 degree_2   degree_3 degree_4
#>    <char>      <char>  <fctr> <int> <int>      <num>    <num>      <num>    <num> 
#>  region_1  polynomial 1999-12  1999    12  0.3703647 35.44290  42.662160 2277.396 
#>  region_1  polynomial 2000-01  2000     1 -0.9477844 37.63980 -50.418337 2270.166
#>  region_1  polynomial 2000-02  2000     2 -1.2895124 36.79065 -75.434199 2400.315
#>  region_2  polynomial 1999-12  1999    12  0.1551976 31.36412  33.632878 1814.176
#>  region_2  polynomial 2000-01  2000     1  0.4783851 28.49742   7.187916 1652.574
```

### Example 2: Binning with Secondary Weights

``` r
# Population-weighted temperature aggregation
exposures <- r2e2(
  env_rast = "path/to/temperature/",           # A directory name works too
  sec_weight_rast = "path/to/population/",     # ... also for secondary weights
  geometry = "path/to/regions.shp",            # ... and spatial geometries
  geom_id_col = "region_id",                   # Optional: keep only ID column
  start_date = "2000-01-01",                   # Optional: Filter raster date range
  end_date = "2020-12-31",                     # Default: full date range
  out_format = "all",                          # Both 'long' and 'wide' output               
  trans_type = "bin",                          # Bin transformation
  breaks = c(-10, 0, 10, 20, 30),              # Bin breaks
  out_temp_res = "daily",                      # Keep daily resolution
  save_path = "path/to/output",                # Save outputs locally
  verbose = 2                                  # Show detailed messages
)
```

## `r2e2()`: Raster to Environmental Exposures

The `r2e2` function of the `heat` package aggregates environmental raster data to environmental exposures following these three steps:

1. **Transformation**: Applies the specified nonlinear transformation to each raster cell's time series.
2. **Spatial Aggregation**: Averages the transformed raster values over each geometry using `exactextractr`, optionally using secondary weights.
3. **Temporal Aggregation**: Aggregates the spatially averaged values to the desired temporal resolution (e.g., monthly, yearly).

For a great explanation of these operations and why their order matters, please refer to [Lidell et al. (2025)](https://www.sciencedirect.com/science/article/pii/S1364815224002639). The `r2e2` part of the `heat` package closely aligns in purpose with their `stagg` package but aims to provide a faster, more robust approach. `heat` also extends the `stagg` functionality with these additional features:



-  Batch processing: Enables fast processing across multiple decades on regular laptops and high-performance computing clusters
-  Smart restart: Automatically resumes from last successful batch when re-running after interruption (see performance tips below)
-  Built-in plots to validate the quality of the output and visualize the exposure distributions
-  Integrated support for time-varying secondary weights (e.g., yearly population counts)
-  Raster interpolations: Mean daily temperature, or sinusoidal hourly temperature, interpolated from daily min and max temperature
-  Long and wide output formats
-  User provided custom transformation functions on top of standard built-in transformations (Polynomial, Splines, Binning)
-  Custom arguments can be passed to the transformation functions and the spatial aggregation via `exact_extract()` to customize the processing

We are currently finishing up additional functions downstream of `r2e2` such as calculating unit specific lags. 

### Transformation Types

| Type               | Description              | Function        | Required Arguments    |
|--------------------|--------------------------|------------------|-----------------------|
| `"none"`           | No transformation        | -                | -                     |
| `"polynomial"`     | Polynomial     | `stats::poly()`  | `degree = <integer>`  |
| `"natural_spline"` | Natural cubic spline     | `splines::ns()`  | `knots = c(...)`      |
| `"b_spline"`       | B-spline           | `splines::bs()`  | `knots = c(...)`      |
| `"bin"`            | Binning   | `heat::create_bins()`           | `breaks = c(...)`     |





### Input Data Requirements

#### Environmental Raster

A raster (`SpatRaster` object), or a directory name of either GeoTIFF (`.tif`) or NetCDF (`.nc`) raster files meeting the following requirements:

-   Longitude Format: Must use -180 to 180 degrees (not 0 to 360). Use `terra::rotate()` to convert if needed.
-   Layer Names: Must be dates in format `YYYY-MM-DD HH:MM`, `YYYY-MM-DD`, `YYYY-MM`, or `YYYY` (e.g., `2020-01-15 12:00`, `2020-01-15`, `2020-01`, `2020`). It also works if the time dimension (`terra::time(raster)`) instead of the layernames contains the time steps.
-   Coverage: Will be cropped to the geometry's extent automatically but must overlap with it.
-   Subdaily Aggregation: Sub-daily data (e.g. hourly) can be aggregated to daily using `daily_agg_fun = "mean"` or `"sum"`. This step is done before the transformation and speeds up the processing without introducing bias. 

These requirements are automatically checked within `r2e2()`, but can be checked separately:

``` r
# Validate raster structure
env_rast <- rast("path/to/climate/data/")
check_raster(env_rast)                     # Checks longitude, layer names, temporal resolution
```

#### Secondary Weight Rasters (Optional)

Used to weight spatial aggregation (e.g., population-weighted averages). Same format and requirements as environmental raster data, except:

-   Values: Should be positive (e.g., population counts, crop areas)
-   CRS: Ideally matches environmental raster CRS. If not, will be reprojected to match environmental raster CRS using bilinear projection.
-   Resolution: Will be resampled (using `terra::resample(.., method = "average")`) to match environmental raster resolution.

**Note**: Secondary weight layers split up the environmental raster into weighting periods. Each weight layer creates a separate period with the layers of the environmental raster that are closest to it in time. 


#### Spatial Geometries

A `sf` object (polygons or points) or path to spatial file (`.gpkg`, `.shp`, `.geojson`, `.json`, `.fgb`, `.rds`, `.parquet`) meeting the following requirements:

-   CRS: Any valid CRS is accepted, will be reprojected to match environmental raster CRS automatically.
-   ID Column: Ideally a unique identifier column (specified via `geom_id_col`). If no ID column is provided, the row index will be used and all columns retained.
-   Geometry Type: POLYGON, MULTIPOLYGON, POINT, or MULTIPOINT

Validation is executed automatically within `r2e2()`, but can be run separately:
``` r
# Validate and clean spatial geometries
regions <- st_read("regions.gpkg")
validated_regions <- check_spatial_file(regions)  # Checks CRS, validates geometries
```



### Output Formats

Depending on the input resolution and chosen temporal aggregation, results include:

-   `daily_long`: Long format (rows: geometry-date pairs) in daily resolution.
-   `daily_wide`: Wide format (rows: geometry, columns: dates) in daily resolution. 
-   `monthly_long`: Long format in monthly resolution. See Example 1 above.
-    ...
-   `yearly_wide`: Wide format in yearly resolution
-   `area_weights`: Area weights for each geometry-cell pair

**Example:** 

- Input: daily data, `out_temp_res = "monthly"` → outputs: `daily_wide`, `daily_long`, `monthly_wide`, `monthly_long`.
- Choose output format with `out_format = "wide"`, `"long"`, or `"all"`. 
- Saved files use the same naming: `daily_long.parquet`, `monthly_wide.parquet`, etc.


## Validation

### Built-in Validation

Enable built-in validation to visualize and verify your results during processing:

``` r
exposures <- r2e2(
  env_rast = temperature,
  geometry = regions,
  geom_id_col = "region_id",
  trans_type = "polynomial",
  degree = 4,
  out_temp_res = "monthly",
  validation = TRUE,                         # Generate validation plots
  validation_var = "degree_1",               # Variable to visualize
  validation_var_name = "Temperature (°C)",  # Variable name for plots
  verbose = 1
)
```

### Validation after processing

Validation can also be run separately on existing results:

``` r
# Validate after processing
validate_r2e2(
  results = exposures,                       # Output from r2e2()
  geometry = regions,
  geom_id_col = "region_id",
  validation_var = "degree_1",               # Variable to visualize
  validation_var_name = "Temperature (°C)"   # Variable name for plots
)
```

Validation produces plots showing: 

- Maps of spatial averages and missing values
- Time series visualizing trends over the years
- Summary statistics and seasonal distributions 
- Grid cell alignment verification 
- Cell count diagnostics


## Interpolation

For raster datasets with only daily minimum and maximum values (e.g., `tmin` and `tmax`), `heat` provides interpolation functions to estimate hourly or mean daily values:

- **`mean_interpol()`**: Simple average of min and max for daily mean values
- **`sinusoidal_interpol()`**: Sinusoidal curve fitting for hourly estimates

### Example: Interpolating to Hourly Temperature

``` r
# Interpolate hourly temperature from tmin and tmax
hourly_temp <- interpol_min_max(
  min_rast = "path/to/tmin/",
  max_rast = "path/to/tmax/",
  geometry = regions,
  start_date = "2020-01-01",
  end_date = "2020-12-31",
  interpol_fun = sinusoidal_interpol   # Use sinusoidal interpolation
)

# Get binned distribution within a day from the hourly data
exposures <- r2e2(
  env_rast = hourly_temp,
  geometry = regions,
  trans_type = "bin",
  breaks = c(-10, 0, 10, 20, 30),
  out_temp_res = "daily"                 # Aggregate back to daily
)
```
## Performance Tips

-   Use `max_cells` to control memory usage. Default are 30 million (`3e7`) cells processed at once. Lower values will decrease memory usage.
-   Set `verbose = 2` for detailed progress information and debugging.
-   For points, the package automatically uses an optimized extraction method
-   Enable smart restart, which automatically resumes from the last successful batch when re-running after an interruption. This is enabled by default when a `save_path` is provided, `save_batch_output = TRUE`, and `overwrite_batch_output = FALSE`.
-   If the polygons are far apart, e.g. spanning multiple countries or continents, the soon to be released `r2e2_country()` function will accelerate processing by splitting the raster by country.


## Shocks
For wide datasets produced by the r2e2 function, and given a dataframe with geometry IDs and dates (e.g., the starting date of a program), the `heat` package provides functions to estimate measures of climate shocks or deviations with respect to a historical baseline. The main of this functions, shocks_wrapper, receives inputs to create a sequence of dates foe which the mean and standard deviation are estimated for a given climate variable, and then compared to a baseline. The user can specify the size of the window, the number of time-steps into which it is divided, the nature of the historical window (whether dynamic or fixed), and many other configurations by defining the following parameters: 


-   **`window`**: the size in time units (i.e., days when the resolution is daily, months when it is monthly) of the dates window
-   **`start`**: the offset in days indicating the starting date of the window with respect to the date associated with a geometry.
-   **`hist_lags`**: whenever the historical baseline is dynamic, the number of past years to include to create the baseline (e.g., hist_lags = 10 means that the historical baseline will be created with the climate measures for the 10 years before theyear of the geometry date). 
-   **`align`**: whether the date sequence is left- or right-aligned with respect to the geometry date. Left-alignment means that the sequence contains dates AFTER the polygon date. Right-alingment means that the sequence includes dates BEFORE the polygon date
-   **`time_step_size`**: the number of days (months) in each temporal bin that divides the date sequence (e.g., `time_step_size` = 30 divides a daily date sequence of length `window` into groups or bins of 30 days).
-   **`disjoint`**: whether the groups into which the date sequence is divide shall be considered disjoint or overlapping (default option). Disjoint groups never overlap. For example, for a date sequence of 30 days with 3 groups of 10 days (`time_step_size` = 10), disjoint groups have the form: **group 1**: day 1 - day 10 | **group 2**: day 11 - day 20 | **group 3**: day 21 - day 30. On the other hand, overlapping groups have the form: **group 1**: day 1- day 10 | **group 2**: day 1 - day 20 | **group 3**: day 1- day 30. 
-   **`window_spec`**: whether the historical window is dynamic, fixed or both. When it is dynamic, the starting and ending dates of the historical window for a given geometry depend on the specific date associated to that geometry and the number of historic lags. The window sequence corresponding to a given geometry date will be replicated for a number of past years (as specified by the `hist_lags` argument) to form the dynamic historical window. Therefore, the period to build the dynamic historical window will depend on the year of each geometry date and will vary across geometries. On the contrary, when the historical window is fixed, the historical baseline is created by replicating the window sequence of a given geometry date within a fixed range of time, which is defined by the `start_date` and `end_date` parameters. All historic windows are therefore created within the same range, regardless of the year of the geometry date. 
-   **`start_date`**: whenever the historical window is fixed, the initial date of the window of time used to create the historical baseline. 
-   **`end_date`**: whenever the historical window is fixed, the end date of the window of time used to create the historical baseline. 
-   **`int_threshold`**: some date sequence might fall outside of the date range of the dataframe produced by the `r2e2` function. This parameter controls the proportion of dates of a date sequence and its corresponding historical baseline that must be present in the dataframe to be considered valid for analysis. 

### Notes on performance: 
The `shocks_warpper` function might deal with massive objects (e.g., parquet files containing daily climate measures spanning a long period). Therefore, the function includes several parameters to balance the need for computational speed and the memory limits. These parameters are:


-   **`conditions_list`**: A named list of conditions to query the parquet file with climate measures. The named conditions  should be: 

      1. data_path: A character string with the path to the parquet file.
      2. geom_id: A vector of unique geometry IDs indicating the geometries for which the shocks should be estimated.
      3. trans_type: A character vector indicating the type of transformations to be included (e.g., c("polynomial")).
      4. trans_var: A character vector indicating the degrees of the transformation to include (e.g., c("degree_2", "degree_3")).
      5. product_temp_res: A character indicating the temporal resolution of the product. Must be one of the following: "daily" or "monthly" ("yearly" not supported yet). 

-   **`date_tolerance_days`**: some geometries can be grouped based on the proximity of their dates. Doing so helps reduce the number of date columns that need to be loaded per geometry group and, as such, the memory usage of the function. Setting a lower number means that groups are smaller in size, but might increases running times as more groups are produced. 
-   **`max_group_size`**: this parameters defines the maximum number of geometries that are allowd to be in a group (i.e., after grouping geometries by date proximity). This prevents the creation of extremely large groups, which reduces memory usage, but might increase running times by creating more groups to process.
-   **`prop_cores`**: in some cases, parallelization might be desired. The function performs parallelization by using the future callr backend, using a proportion `prop_cores` of available workers. Parallelization might reduce running times, but using multiple cores might increase memory usage substantially. Keep `prop_cores` near 0 (or 0 if you want sequential processing) if shocks are being estimated for long date sequences and using long historic sequences (e.g., 20 years of historic lags). 

### Example: estimating climate shocks with different historical window schemes
``` r
# Define conditions list
conditions = list(
  data_path = file.path(...) # Path to wide-format parquet file produced by r2_e2,
  geom_id = unique(poly_date_df$poly_id)  # Vector of geometry IDs (in the example, all polygons are used)
  trans_type = c("polynomial"),           # Transformation type of interest
  trans_var = c("degree_1", "degree_2")   # Transformation degrees of interest
)

# Estimate shocks with a dynamic window
climate_shocks_dynamic <- shocks_wrapper(
  pol_date_pairs = poly_date_df,        # Dataframe with a geometry id column and a date column
  conditions_list = conditions,         # A list with condtions to filter the parquet file with climate measure
  geom_id_col_parquet = "geom_id",      # The name of the column in the parquet file that contains the geometry ids 
  geom_id_col_df = "poly_id",           # The name of the column in the geometry-dates dataframe with the geometry ids
  date_id_col_df = "int_date",          # The name of the column in the geometry-dates dataframe with the dates
  window = 10,                          # Create a window of 10 days (given a daily resolution, as specified in the conditions list)
  start = 0,                            # No offset with respect to the geometry date
  hist_lags = 10,                       # Use 10 past years to build the dynamic baseline
  align = "right",                      # The date sequence includes dates previous to the geometry date
  time_step_size = 5,                   # The date sequence is divided into N groups, each of 5 five days
  window_spec = "dynamic",              # Create a dynamic baseline
  start_date = NULL,                    # Not required if the window is dynamic
  stop_date = NULL,                             
  disjoint= FALSE,                      # Use overlapping groups
  int_threshold = 0.8,                  # Intersection threshold
  prop_cores = 0.2,                     # Use 20% of avalaible workers
  date_tolerance_days = 10,             # Group polygons for which the date is at most 10 days apart
  max_group_size = 10,                  # Polygon groups should have at most 10 polygons
  query = "temperature_shocks",           # Name to be assigned to the final output
  output_path =  file.path("..."),      # Path were the final output should be saved
  error_log_path = file.path("...")     # Output were the error logs will be saved
)

# Estimate shocks with a fixed window
climate_shocks_dynamic <- shocks_wrapper(
  pol_date_pairs = poly_date_df,               
  conditions_list = conditions,                
  geom_id_col_parquet = "geom_id",              
  geom_id_col_df = "poly_id",                  
  date_id_col_df = "int_date",                 
  window = 10,                                 
  start = 0,                                   
  hist_lags = 0,                               # Set to zero for fixed windows
  align = "right",                             
  time_step_size = 5,                          
  window_spec = "fixed",                       # Create a fixed baseline
  start_date = "1980-01-01",                   # Range for the fixed window begins on this date
  stop_date = "1990-01-01",                    # Range for the fixed window ends on this date         
  disjoint= FALSE,                             
  int_threshold = 0.8,                         
  prop_cores = 0.2,                            
  date_tolerance_days = 10,                    
  max_group_size = 10,                         
  query = "temperature_shocks",                  
  output_path =  file.path("..."),             
  error_log_path = file.path("...")            
)

# Estimate shocks with both window schemes
climate_shocks_dynamic <- shocks_wrapper(
  pol_date_pairs = poly_date_df,               
  conditions_list = conditions,                
  geom_id_col_parquet = "geom_id",              
  geom_id_col_df = "poly_id",                  
  date_id_col_df = "int_date",                 
  window = 10,                                 
  start = 0,                                   
  hist_lags = 10,                              # Use 10 past years to build the dynamic baseline
  align = "right",                             
  time_step_size = 5,                          
  window_spec = "both",                        # Create a dynamic and a fixed baseline
  start_date = "1980-01-01",                   # Initial date for the fixed baseline
  stop_date = "1990-01-01",                    # End date for the fixed baseline         
  disjoint= FALSE,                             
  int_threshold = 0.8,                         
  prop_cores = 0.2,                            
  date_tolerance_days = 10,                    
  max_group_size = 10,                         
  query = "temperature_shocks",                  
  output_path =  file.path("..."),             
  error_log_path = file.path("...")            
)

```
The `shocks_wrapper` function will create a parquet file with its results, which will be stored in the folder specified in the `output_path` argument. Similarly, if any errors are encountered, they will be stored as `.Rds` objects (lists) in the `error_log_path`. The function currently keeps track of the following errors: 1. date sequences that fall outside of the date range in dataframe produced by `r2e2`; 2. geometry dates outside of the dataframe´s range; 3. missing data in the dataframe produced by the `r2e2` function (e.g., NAs). The function will return a list with the path to the output and the errors list (if this exists).  Use `arrow::open_dataset(output_path)` to access the results. 

## Validating the shocks measures
The `heat` package also provides functions to quickly examine the output of the shocks procedure. The three core functions included are: 
-   **`validate_global_summaries()`**: given the results datafarame as input, prints the summaries for each shocks measure, as well as NA counts.
-   **`validate_measures_distributions()`**: given the results dataframe as input, plots the distribution for each measure. 


**Installation Note for Positron Users**: If you encounter progress bar issues in the Positron IDE, install the development version of `progress`:

``` r
devtools::install_github("r-lib/progress")
devtools::install_github("echolab-stanford/heat")
```

## Citation

If you use this package in your research, please cite:

``` r
Wallstein, Jonas, Brandon de la Cuesta, and Marshall Burke. 
"Heat: An R Package for Calculating Environmental Exposures". 
Zenodo, December 10, 2025. 
https://doi.org/10.5281/zenodo.17882618.
```

BibTeX entry:

```bibtex
@software{wallstein_heat_2025,
  author       = {Wallstein, Jonas and de la Cuesta, Brandon and Burke, Marshall},
  title        = {Heat: An R Package for Calculating Environmental Exposures},
  month        = dec,
  year         = 2025,
  publisher    = {Zenodo},
  doi          = {10.5281/zenodo.17882618},
  url          = {https://doi.org/10.5281/zenodo.17882618}
}
```

## Issues & Contributions

Report bugs or request features on [GitHub Issues](https://github.com/echolab-stanford/heat/issues).
