# This script provides an example of how to use the shocks_wrapper function and validate its outputs

# Preliminaries
rm(list = ls())
closeAllConnections(); gc()

# Packages
if (!require("pacman")) install.packages("pacman")
library(pacman)
pacman::p_load(terra, exactextractr, tidyverse, sf, data.table, arrow, fst, rstudioapi)
# library(terra)
# library(exactextractr) 
# library(tidyverse)
# library(sf)
# library(data.table)
# library(arrow)
# library(fst)

# Paths
MAIN_WD <- dirname(dirname(rstudioapi::getActiveDocumentContext()$path)) # Path to repo folder
FUNCTIONS_PATH <- file.path(MAIN_WD, "R")
BASE_PARQUET_PATH <- file.path(MAIN_WD, "shocks_example/data/base_parquet.parquet")
POL_DATE_PATH <-  file.path(MAIN_WD, "shocks_example/data/ab_r2-r8_polygon_date_pairs.fst")

# Load functions --- for now as this repo is not yet ready to be downloaded
sapply(
  dir(FUNCTIONS_PATH, pattern = "*.R", full.names = T),
  source 
)

# Previous steps
# The shocks function requires a wide-format parquet file created by the r2_e2 function and stored in a folder. 
# You can use the toy dataset to explore the shocks functions. This toy dataset was generated from an existing 
# dataframe created by the r2_e2 function. 

# Explore the existing parquet file with climate exposures
base_pq <- arrow::open_dataset(BASE_PARQUET_PATH)
length(colnames(base_pq))                             # N. columns
nrow(base_pq)                                         # N. rows
colnames(base_pq)[1:10]                               # Column sample
print(                                                # Date range
  paste( 
    "The dates in this parquet file go from", 
    min(colnames(base_pq)[4:length(colnames(base_pq))]), 
    "to", 
    max(colnames(base_pq)[4:length(colnames(base_pq))])
  )
)
base_pq$schema[1:10]                                  # Column sample description
geoms <- unique(base_pq %>% select(poly_id) %>% pull(as_vector = TRUE)); geoms # Polygons in parquet file
#base_pq_data <- base_pq %>% collect() # Run if you want to load the full parquet file to inspect it. This is not necessary for the shocks function to run.

# Load polygon-dates dataframe
poly_date_pairs <- read_fst(POL_DATE_PATH)

# Define conditions list
conditions = list(
  data_path = BASE_PARQUET_PATH,                       # Path to wide-format parquet file with climate exposures
  geom_id = unique(poly_date_pairs$poly_id),           # Geometry IDs for which climate shock measures are desired. Here we ask for all (note: however, in this example, the shocks will only be computed for the subset of 10 polygon IDs that are in the example parquet file) 
  trans_type = c("polynomial"),                        # Select desired transformation type
  trans_var = c("degree_1"),                           # Select only degree 1 transformations
  product_temp_res = "daily"                           # Indicate product´s resolution
)

# Call shocks functions
shocks_example <- shocks_wrapper(
  pol_date_pairs = poly_date_pairs,                    # Pass geometry-date dataframe
  conditions_list = conditions,                        # Pass conditions list
  geom_id_col_parquet = "poly_id",                     # Indicate the name of the column that identifies the geometries in the parquet file
  geom_id_col_df = "poly_id",                          # Indicate the name of the column that identifies the geometries in the geometry-date dataframe
  date_id_col_df = "date",                             # Idem, but for the dates in the polygon-date dataframe
  window = 10,                                         # Define window lenght. As the product´s resolution is daily, here we define a window of 10 days
  start = 0,                                           # No offset with respect to the geometry dates 
  hist_lags = 2,                                       # Create baseline using 2 year lags (necessary when window_spec is dynamic)                               
  align = "right",                                     # Alignment of the window with respect to the date
  time_step_size = 5,                                  # The window will be "binned" in groups of five days 
  window_spec = "dynamic",                             # Dynamic historic window
  start_date = NULL,                                   # Not required as historic window is not fixed or both
  stop_date = NULL,                                    # Idem
  disjoint= FALSE,                                     # The "bins" are overlapping
  int_threshold = 0.8,                                 # Date sequences mut have at least an 80% intersection with the column dates in the parquet to be valid
  prop_cores = 0.2,                                    # Use 20% of available workers for parallelization. Decrease if concerned about memory usage (eg: too many polygons, long historic baselines, long windows, etc.)
  date_tolerance_days = 7,                             # Polygons will be grouped by a proximity of seven days. 
  max_group_size = 10,                                 # Maximum number of polygons allowed per group
  query = "example_shocks",                            # Name that should be given to the output file
  output_path =  file.path(MAIN_WD, "shocks_example/output/results"),  # File to export resulting parquet file with shocks
  error_log_path = file.path(MAIN_WD, "shocks_example/output/errors")  # File to export error logs, if any. 
)

# Explore results
shocks_example[["output"]] # Path to output
shocks_example[["errors"]] # Errors recorded

# Validate results
res_parquet <- arrow::open_dataset(shocks_example[["output"]]) %>% collect()


# Validate
validate_global_summaries(res_parquet)
dist_plots <- validate_measure_distributions(res_parquet, show_plots = TRUE) # show_plots = TRUE prints the plots
