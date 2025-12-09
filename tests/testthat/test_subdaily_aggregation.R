# ===============================================================================
# UNIT TEST FOR SUBDAILY AGGREGATION FUNCTIONALITY
# ===============================================================================
#
# Description:
#   This script tests the agg_to_daily() function and its integration into the
#   r2e2() pipeline. It creates synthetic hourly data and verifies that subdaily
#   aggregation to daily works correctly with both mean and sum functions.
#
# Test Strategy:
#   - Creates synthetic hourly raster data with known values
#   - Tests standalone agg_to_daily() function with mean and sum
#   - Tests r2e2() pipeline with subdaily_agg_fun parameter
#   - Verifies output temporal resolution and numerical correctness
#
# Author: Jonas Wallstein
# Last Updated: 2025-12-08
#
# ===============================================================================

library(testthat)

# Load necessary libraries
suppressPackageStartupMessages({
  library(terra)
  library(sf)
  library(data.table)
  library(dplyr)
})

# Load the heat package
library(heat)

# ---- Helper Functions ----------------------------------------------------------

should_print_banners <- function() {
  nzchar(Sys.getenv("TESTTHAT_VERBOSE")) || isTRUE(getOption("testthat.verbose", FALSE))
}

# ---- Test 1: Standalone agg_to_daily() function ----------------------------------------------------------

test_that("agg_to_daily aggregates hourly data to daily means correctly", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 1: agg_to_daily() with mean ===\n")
  }
  
  # Create a simple hourly raster with 3 days of data (72 hours)
  # Each hour will have a constant value for simplicity
  dates <- seq(as.POSIXct("2000-01-01 00:00", tz = "UTC"),
               as.POSIXct("2000-01-03 23:00", tz = "UTC"),
               by = "hour")
  
  # Create a simple 2x2 raster
  r <- rast(ncols=2, nrows=2, xmin=-180, xmax=180, ymin=-90, ymax=90)
  
  # Create 72 layers (3 days × 24 hours)
  hourly_rast <- rast(replicate(72, r))
  
  # Set layer names in format "YYYY-MM-DD HH:MM"
  names(hourly_rast) <- format(dates, "%Y-%m-%d %H:%M")
  
  # Set values: day 1 = 10, day 2 = 20, day 3 = 30 (all hours same value per day)
  for (i in 1:72) {
    day_num <- ceiling(i / 24)
    values(hourly_rast[[i]]) <- day_num * 10
  }
  
  # Aggregate to daily means
  daily_rast <- agg_to_daily(hourly_rast, fun = "mean")
  
  # Check: should have 3 layers (3 days)
  expect_equal(nlyr(daily_rast), 3)
  
  # Check: layer names should be dates
  expect_equal(names(daily_rast), c("2000-01-01", "2000-01-02", "2000-01-03"))
  
  # Check: values should be 10, 20, 30
  expect_equal(values(daily_rast[[1]])[1], 10)
  expect_equal(values(daily_rast[[2]])[1], 20)
  expect_equal(values(daily_rast[[3]])[1], 30)
  
  if (should_print_banners()) {
    cat("  ✓ Mean aggregation works correctly\n")
  }
})

test_that("agg_to_daily aggregates hourly data to daily sums correctly", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 2: agg_to_daily() with sum ===\n")
  }
  
  # Create a simple hourly raster with 2 days of data (48 hours)
  dates <- seq(as.POSIXct("2000-01-01 00:00", tz = "UTC"),
               as.POSIXct("2000-01-02 23:00", tz = "UTC"),
               by = "hour")
  
  # Create a simple 2x2 raster
  r <- rast(ncols=2, nrows=2, xmin=-180, xmax=180, ymin=-90, ymax=90)
  hourly_rast <- rast(replicate(48, r))
  names(hourly_rast) <- format(dates, "%Y-%m-%d %H:%M")
  
  # Set values: all hours = 1, so daily sum should be 24
  for (i in 1:48) {
    values(hourly_rast[[i]]) <- 1
  }
  
  # Aggregate to daily sums
  daily_rast <- agg_to_daily(hourly_rast, fun = "sum")
  
  # Check: should have 2 layers (2 days)
  expect_equal(nlyr(daily_rast), 2)
  
  # Check: values should be 24 (24 hours × 1)
  expect_equal(values(daily_rast[[1]])[1], 24)
  expect_equal(values(daily_rast[[2]])[1], 24)
  
  if (should_print_banners()) {
    cat("  ✓ Sum aggregation works correctly\n")
  }
})

test_that("agg_to_daily with fun='none' returns original raster unchanged", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 3: agg_to_daily() with none ===\n")
  }
  
  # Create a simple hourly raster
  dates <- seq(as.POSIXct("2000-01-01 00:00", tz = "UTC"),
               as.POSIXct("2000-01-01 23:00", tz = "UTC"),
               by = "hour")
  
  r <- rast(ncols=2, nrows=2, xmin=-180, xmax=180, ymin=-90, ymax=90)
  hourly_rast <- rast(replicate(24, r))
  names(hourly_rast) <- format(dates, "%Y-%m-%d %H:%M")
  
  for (i in 1:24) {
    values(hourly_rast[[i]]) <- i
  }
  
  # Call with fun = "none"
  result_rast <- suppressMessages(agg_to_daily(hourly_rast, fun = "none"))
  
  # Should return the same raster
  expect_equal(nlyr(result_rast), 24)
  expect_equal(names(result_rast), names(hourly_rast))
  expect_equal(values(result_rast[[1]])[1], 1)
  expect_equal(values(result_rast[[24]])[1], 24)
  
  if (should_print_banners()) {
    cat("  ✓ 'none' option returns original data\n")
  }
})

# ---- Test 4: Integration with r2e2() pipeline ----------------------------------------------------------

test_that("r2e2 pipeline works with subdaily_agg_fun parameter", {
  
  if (should_print_banners()) {
    cat("\n=== TEST 4: r2e2() with daily_agg_fun ===\n")
  }
  
  skip_if_not(file.exists(testthat::test_path("fixtures", "data", "polygons.gpkg")),
              message = "Test data not available")
  
  # Create synthetic hourly data for 2 days (48 hours)
  dates <- seq(as.POSIXct("2000-01-01 00:00", tz = "UTC"),
               as.POSIXct("2000-01-02 23:00", tz = "UTC"),
               by = "hour")
  
  # Create a raster that covers the test polygon area
  r <- rast(ncols=10, nrows=10, xmin=-5, xmax=5, ymin=-5, ymax=5)
  hourly_rast <- rast(replicate(48, r))
  names(hourly_rast) <- format(dates, "%Y-%m-%d %H:%M")
  
  # Set values: hour 1-24 = 10, hour 25-48 = 20
  for (i in 1:48) {
    day_val <- ifelse(i <= 24, 10, 20)
    values(hourly_rast[[i]]) <- day_val
  }
  
  # Load test polygons
  geometry <- read_spatial_file(testthat::test_path("fixtures", "data", "polygons.gpkg"))
  
  # Run r2e2 with subdaily aggregation
  exposures <- suppressMessages(
    r2e2(
      env_rast = hourly_rast,
      geometry = geometry,
      geom_id_col = "geom_id",
      trans_type = "none",
      daily_agg_fun = "mean",  # Aggregate hourly to daily
      out_temp_res = "daily",
      temp_agg_fun = "mean",
      out_format = "long",
      validation = FALSE,
      verbose = 0
    )
  )
  
  # Check that output exists
  expect_true(!is.null(exposures))
  expect_true("daily_long" %in% names(exposures))
  
  # Check that we have 2 days of data (not 48 hours)
  daily_data <- exposures$daily_long
  expect_equal(length(unique(daily_data$date)), 2)
  
  # Check dates are in daily format
  dates_in_output <- unique(daily_data$date)
  expect_true(all(grepl("^\\d{4}-\\d{2}-\\d{2}$", dates_in_output)))
  
  if (should_print_banners()) {
    cat("  ✓ r2e2 pipeline with subdaily_agg_fun works correctly\n")
    cat(sprintf("  ✓ Input: 48 hourly layers → Output: %d daily values\n", 
                length(unique(daily_data$date))))
  }
})

# ---- Test Summary ----------------------------------------------------------

if (should_print_banners()) {
  cat("\n")
  cat(strrep("=", 80), "\n")
  cat("SUBDAILY AGGREGATION TESTS PASSED ✓\n")
  cat(strrep("=", 80), "\n")
  cat(sprintf("Test completed at: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")))
  cat(strrep("=", 80), "\n\n")
}
