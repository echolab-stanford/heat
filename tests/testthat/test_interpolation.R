# ===============================================================================
# UNIT TEST FOR INTERPOLATION FUNCTIONALITY
# ===============================================================================
#
# Description:
#   This script tests the interpolation functions mean_interpol(), 
#   sinusoidal_interpol(), and interpol_min_max(). It verifies that:
#   - mean_interpol() correctly computes daily means from tmin/tmax
#   - sinusoidal_interpol() produces 24 hourly values per day
#   - interpol_min_max() integrates with both interpolation methods
#   - Date naming and formatting are preserved correctly
#
# Test Strategy:
#   - Creates synthetic tmin/tmax raster data with known values
#   - Tests standalone mean_interpol() and sinusoidal_interpol()
#   - Tests interpol_min_max() with both interpolation methods
#   - Verifies output layer counts, naming, and numerical correctness
#   - Tests integration with daily aggregation
#
# Author: Jonas Wallstein
# Last Updated: 2025-12-09
#
# ===============================================================================

library(testthat)

# Load necessary libraries
suppressPackageStartupMessages({
  library(terra)
  library(sf)
  library(data.table)
  library(dplyr)
  library(lubridate)
})

# Load the heat package
library(heat)

# ---- Helper Functions ----------------------------------------------------------

should_print_banners <- function() {
  nzchar(Sys.getenv("TESTTHAT_VERBOSE")) || isTRUE(getOption("testthat.verbose", FALSE))
}

# ---- Setup Test Data ----------------------------------------------------------

# Create synthetic tmin/tmax rasters for testing
create_tmin_tmax_rasters <- function(n_days = 3, temp_dir = tempdir()) {
  
  # Create directories
  tmin_dir <- file.path(temp_dir, "tmin")
  tmax_dir <- file.path(temp_dir, "tmax")
  dir.create(tmin_dir, showWarnings = FALSE, recursive = TRUE)
  dir.create(tmax_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Define extent (simple 3x3 grid)
  ext_obj <- ext(0, 3, 0, 3)
  
  # Determine year for file naming
  start_date <- as.Date("2000-01-01")
  year <- year(start_date)
  
  # Create rasters for all days and save as a single multi-layer file per year
  tmin_layers <- list()
  tmax_layers <- list()
  
  for (i in 1:n_days) {
    current_date <- start_date + (i - 1)
    
    # Create tmin raster (values between 0 and 10)
    set.seed(100 + i)
    tmin_values <- runif(9, min = 0, max = 10)
    tmin_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                      vals = tmin_values, crs = "EPSG:4326")
    names(tmin_rast) <- as.character(current_date)
    tmin_layers[[i]] <- tmin_rast
    
    # Create tmax raster (values between 15 and 25, always > tmin)
    set.seed(200 + i)
    tmax_values <- runif(9, min = 15, max = 25)
    tmax_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                      vals = tmax_values, crs = "EPSG:4326")
    names(tmax_rast) <- as.character(current_date)
    tmax_layers[[i]] <- tmax_rast
  }
  
  # Combine layers into multi-layer rasters
  tmin_stack <- rast(tmin_layers)
  tmax_stack <- rast(tmax_layers)
  
  # Save as year-named files (e.g., 2000.tif)
  writeRaster(tmin_stack, 
              file.path(tmin_dir, paste0(year, ".tif")),
              overwrite = TRUE)
  writeRaster(tmax_stack, 
              file.path(tmax_dir, paste0(year, ".tif")),
              overwrite = TRUE)
  
  return(list(tmin_dir = tmin_dir, tmax_dir = tmax_dir))
}

# Create test geometry
create_test_geometry <- function() {
  poly_coords <- matrix(c(
    0.5, 0.5,
    2.5, 0.5,
    2.5, 2.5,
    0.5, 2.5,
    0.5, 0.5
  ), ncol = 2, byrow = TRUE)
  
  poly <- st_polygon(list(poly_coords))
  
  geometry <- st_sf(
    geom_id = "test_poly",
    geometry = st_sfc(poly, crs = "EPSG:4326")
  )
  
  return(geometry)
}


# ===============================================================================
# TEST 1: mean_interpol() - Daily Mean Interpolation
# ===============================================================================

test_that("mean_interpol computes daily means correctly", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 1: mean_interpol() ===\n")
  }
  
  # Create simple test rasters
  ext_obj <- ext(0, 3, 0, 3)
  
  # Tmin: all values = 10
  tmin_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                    vals = rep(10, 9), crs = "EPSG:4326")
  names(tmin_rast) <- "2000-01-01"
  
  # Tmax: all values = 20
  tmax_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                    vals = rep(20, 9), crs = "EPSG:4326")
  names(tmax_rast) <- "2000-01-01"
  
  # Apply mean interpolation
  result <- mean_interpol(tmin_rast, tmax_rast)
  
  # Check that result is a SpatRaster
  expect_s4_class(result, "SpatRaster")
  
  # Check that all values are 15 (mean of 10 and 20)
  result_values <- values(result)
  expect_true(all(abs(result_values - 15) < 1e-10))
  
  # Check that layer name is preserved as date
  expect_equal(names(result), "2000-01-01")
  
  if (should_print_banners()) {
    cat("✓ mean_interpol computed correct values\n")
    cat("✓ Layer names preserved correctly\n")
  }
})


test_that("mean_interpol works with multiple days", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 2: mean_interpol() with multiple days ===\n")
  }
  
  ext_obj <- ext(0, 3, 0, 3)
  
  # Create 3 days of data
  tmin_list <- list()
  tmax_list <- list()
  
  for (i in 1:3) {
    current_date <- as.Date("2000-01-01") + (i - 1)
    
    tmin_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                      vals = rep(10 + i, 9), crs = "EPSG:4326")
    names(tmin_rast) <- as.character(current_date)
    tmin_list[[i]] <- tmin_rast
    
    tmax_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                      vals = rep(20 + i, 9), crs = "EPSG:4326")
    names(tmax_rast) <- as.character(current_date)
    tmax_list[[i]] <- tmax_rast
  }
  
  tmin_rast_stack <- rast(tmin_list)
  tmax_rast_stack <- rast(tmax_list)
  
  # Apply mean interpolation
  result <- mean_interpol(tmin_rast_stack, tmax_rast_stack)
  
  # Check number of layers
  expect_equal(nlyr(result), 3)
  
  # Check values for each day
  for (i in 1:3) {
    expected_mean <- (10 + i + 20 + i) / 2
    result_values <- values(result[[i]])
    expect_true(all(abs(result_values - expected_mean) < 1e-10))
  }
  
  # Check layer names
  expected_names <- as.character(seq(as.Date("2000-01-01"), 
                                     as.Date("2000-01-03"), 
                                     by = "day"))
  expect_equal(names(result), expected_names)
  
  if (should_print_banners()) {
    cat("✓ mean_interpol handled multiple days correctly\n")
  }
})


# ===============================================================================
# TEST 3: sinusoidal_interpol() - Hourly Sinusoidal Interpolation
# ===============================================================================

test_that("sinusoidal_interpol produces 24 hourly values per day", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 3: sinusoidal_interpol() ===\n")
  }
  
  ext_obj <- ext(0, 3, 0, 3)
  
  # Create 1 day of data
  tmin_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                    vals = rep(10, 9), crs = "EPSG:4326")
  names(tmin_rast) <- "2000-01-01"
  
  tmax_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                    vals = rep(20, 9), crs = "EPSG:4326")
  names(tmax_rast) <- "2000-01-01"
  
  # Apply sinusoidal interpolation
  result <- sinusoidal_interpol(tmin_rast, tmax_rast)
  
  # Check that result has 24 layers (24 hours)
  expect_equal(nlyr(result), 24)
  
  # Check that all values are between tmin and tmax
  result_values <- values(result)
  expect_true(all(result_values >= 10 & result_values <= 20))
  
  # Check layer names are in datetime format
  result_names <- names(result)
  expect_true(all(grepl("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}$", result_names)))
  
  if (should_print_banners()) {
    cat("✓ sinusoidal_interpol produced 24 hourly layers\n")
    cat("✓ Values within expected range\n")
    cat("✓ Datetime naming format correct\n")
  }
})


test_that("sinusoidal_interpol works with multiple days", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 4: sinusoidal_interpol() with multiple days ===\n")
  }
  
  ext_obj <- ext(0, 3, 0, 3)
  
  # Create 2 days of data
  tmin_list <- list()
  tmax_list <- list()
  
  for (i in 1:2) {
    current_date <- as.Date("2000-01-01") + (i - 1)
    
    tmin_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                      vals = rep(10, 9), crs = "EPSG:4326")
    names(tmin_rast) <- as.character(current_date)
    tmin_list[[i]] <- tmin_rast
    
    tmax_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                      vals = rep(20, 9), crs = "EPSG:4326")
    names(tmax_rast) <- as.character(current_date)
    tmax_list[[i]] <- tmax_rast
  }
  
  tmin_rast_stack <- rast(tmin_list)
  tmax_rast_stack <- rast(tmax_list)
  
  # Apply sinusoidal interpolation
  result <- sinusoidal_interpol(tmin_rast_stack, tmax_rast_stack)
  
  # Check that result has 48 layers (2 days * 24 hours)
  expect_equal(nlyr(result), 48)
  
  # Check that first 24 layers are for 2000-01-01
  first_day_names <- names(result)[1:24]
  expect_true(all(grepl("^2000-01-01", first_day_names)))
  
  # Check that last 24 layers are for 2000-01-02
  second_day_names <- names(result)[25:48]
  expect_true(all(grepl("^2000-01-02", second_day_names)))
  
  if (should_print_banners()) {
    cat("✓ sinusoidal_interpol produced 48 layers for 2 days\n")
    cat("✓ Layer names correctly span both days\n")
  }
})


# ===============================================================================
# TEST 5: interpol_min_max() - Integration with mean_interpol
# ===============================================================================

test_that("interpol_min_max works with mean_interpol", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 5: interpol_min_max() with mean_interpol ===\n")
  }
  
  # Create test data
  temp_dir <- tempdir()
  paths <- create_tmin_tmax_rasters(n_days = 3, temp_dir = temp_dir)
  geometry <- create_test_geometry()
  
  # Run interpol_min_max with mean_interpol
  result <- interpol_min_max(
    min_rast = paths$tmin_dir,
    max_rast = paths$tmax_dir,
    geometry = geometry,
    start_date = "2000-01-01",
    end_date = "2000-01-03",
    interpol_fun = mean_interpol,
    daily_agg_fun = "none"  # No aggregation needed for daily mean
  )
  
  # Check that result is a SpatRaster
  expect_s4_class(result, "SpatRaster")
  
  # Check that result has 3 layers (3 days)
  expect_equal(nlyr(result), 3)
  
  # Check layer names are dates
  result_names <- names(result)
  expect_true(all(grepl("^\\d{4}-\\d{2}-\\d{2}$", result_names)))
  
  if (should_print_banners()) {
    cat("✓ interpol_min_max integrated with mean_interpol successfully\n")
    cat("✓ Output has correct number of layers\n")
  }
  
  # Cleanup
  unlink(paths$tmin_dir, recursive = TRUE)
  unlink(paths$tmax_dir, recursive = TRUE)
})


# ===============================================================================
# TEST 6: interpol_min_max() - Integration with sinusoidal_interpol
# ===============================================================================

test_that("interpol_min_max works with sinusoidal_interpol", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 6: interpol_min_max() with sinusoidal_interpol ===\n")
  }
  
  # Create test data
  temp_dir <- tempdir()
  paths <- create_tmin_tmax_rasters(n_days = 2, temp_dir = temp_dir)
  geometry <- create_test_geometry()
  
  # Run interpol_min_max with sinusoidal_interpol
  result <- interpol_min_max(
    min_rast = paths$tmin_dir,
    max_rast = paths$tmax_dir,
    geometry = geometry,
    start_date = "2000-01-01",
    end_date = "2000-01-02",
    interpol_fun = sinusoidal_interpol,
    daily_agg_fun = "none"  # No aggregation for hourly output
  )
  
  # Check that result is a SpatRaster
  expect_s4_class(result, "SpatRaster")
  
  # Check that result has 48 layers (2 days * 24 hours)
  expect_equal(nlyr(result), 48)
  
  # Check layer names are datetime format
  result_names <- names(result)
  expect_true(all(grepl("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}$", result_names)))
  
  if (should_print_banners()) {
    cat("✓ interpol_min_max integrated with sinusoidal_interpol successfully\n")
    cat("✓ Output has 48 hourly layers for 2 days\n")
  }
  
  # Cleanup
  unlink(paths$tmin_dir, recursive = TRUE)
  unlink(paths$tmax_dir, recursive = TRUE)
})


# ===============================================================================
# TEST 7: interpol_min_max() with daily aggregation
# ===============================================================================

test_that("interpol_min_max aggregates hourly to daily correctly", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 7: interpol_min_max() with daily aggregation ===\n")
  }
  
  # Create test data
  temp_dir <- tempdir()
  paths <- create_tmin_tmax_rasters(n_days = 2, temp_dir = temp_dir)
  geometry <- create_test_geometry()
  
  # Run interpol_min_max with daily aggregation
  result <- interpol_min_max(
    min_rast = paths$tmin_dir,
    max_rast = paths$tmax_dir,
    geometry = geometry,
    start_date = "2000-01-01",
    end_date = "2000-01-02",
    interpol_fun = sinusoidal_interpol,
    daily_agg_fun = "mean"  # Aggregate to daily mean
  )
  
  # Check that result is a SpatRaster
  expect_s4_class(result, "SpatRaster")
  
  # Check that result has 2 layers (2 days, aggregated from hourly)
  expect_equal(nlyr(result), 2)
  
  # Check layer names are dates
  result_names <- names(result)
  expect_true(all(grepl("^\\d{4}-\\d{2}-\\d{2}$", result_names)))
  
  if (should_print_banners()) {
    cat("✓ interpol_min_max aggregated hourly data to daily successfully\n")
    cat("✓ Output has 2 daily layers\n")
  }
  
  # Cleanup
  unlink(paths$tmin_dir, recursive = TRUE)
  unlink(paths$tmax_dir, recursive = TRUE)
})


# ===============================================================================
# TEST 8: interpol_min_max() with save_path
# ===============================================================================

test_that("interpol_min_max saves output when save_path is provided", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 8: interpol_min_max() with save_path ===\n")
  }
  
  # Create test data
  temp_dir <- tempdir()
  paths <- create_tmin_tmax_rasters(n_days = 2, temp_dir = temp_dir)
  geometry <- create_test_geometry()
  
  # Create output directory
  output_dir <- file.path(temp_dir, "interpol_output")
  
  # Run interpol_min_max with save_path
  result <- interpol_min_max(
    min_rast = paths$tmin_dir,
    max_rast = paths$tmax_dir,
    geometry = geometry,
    start_date = "2000-01-01",
    end_date = "2000-01-02",
    interpol_fun = mean_interpol,
    daily_agg_fun = "none",
    save_path = output_dir
  )
  
  # Check that output directory was created
  expect_true(dir.exists(output_dir))
  
  # Check that interpolated_rasters subdirectory exists
  interpol_dir <- file.path(output_dir, "interpolated_rasters")
  expect_true(dir.exists(interpol_dir))
  
  # Check that batch files were created
  batch_files <- list.files(interpol_dir, pattern = "^batch_.*\\.tif$")
  expect_true(length(batch_files) > 0)
  
  if (should_print_banners()) {
    cat("✓ interpol_min_max created output directories\n")
    cat("✓ Batch files saved successfully\n")
  }
  
  # Cleanup
  unlink(paths$tmin_dir, recursive = TRUE)
  unlink(paths$tmax_dir, recursive = TRUE)
  unlink(output_dir, recursive = TRUE)
})


# ===============================================================================
# TEST 9: Error handling - Invalid date names
# ===============================================================================

test_that("mean_interpol errors with invalid date names", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 9: Error handling - Invalid date names ===\n")
  }
  
  ext_obj <- ext(0, 3, 0, 3)
  
  # Create rasters with invalid date names
  tmin_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                    vals = rep(10, 9), crs = "EPSG:4326")
  names(tmin_rast) <- "invalid_date"
  
  tmax_rast <- rast(ncols = 3, nrows = 3, ext = ext_obj, 
                    vals = rep(20, 9), crs = "EPSG:4326")
  names(tmax_rast) <- "invalid_date"
  
  # Expect error when applying mean_interpol
  expect_error(
    mean_interpol(tmin_rast, tmax_rast),
    "could not be converted to valid dates"
  )
  
  if (should_print_banners()) {
    cat("✓ mean_interpol correctly errors on invalid date names\n")
  }
})


# ===============================================================================
# Summary Banner
# ===============================================================================

if (should_print_banners()) {
  cat("\n")
  cat(strrep("=", 80), "\n")
  cat("INTERPOLATION TESTS COMPLETED\n")
  cat(strrep("=", 80), "\n")
}
