# ===============================================================================
# UNIT TESTS FOR POLYGON TRANSFORMATION FUNCTIONS
# ===============================================================================
#
# Description:
#   This script tests the r2e2() pipeline with polygon geometries using
#   different transformation types (none, polynomial, natural_spline, b_spline, 
#   and bin) against baseline outputs to ensure numerical consistency after
#   code changes. Tests include secondary weight rasters.
#
# Test Strategy:
#   - Runs r2e2() with each transformation type on polygon data
#   - Compares spatial_agg_long and temp_agg_long outputs against saved baseline files
#   - Uses tolerance of 1e-2 for numerical comparisons
#   - Provides detailed error messages for easy debugging
#
# Requirements:
#   - Baseline outputs must exist in tests/testthat/fixtures/baseline_output/polygon_transformations/
#   - Test data must exist in tests/testthat/fixtures/data/
#
# Execution:
#   From project root:
#     testthat::test_dir("tests/testthat", reporter = "summary")
#     testthat::test_dir("tests/testthat", reporter = "progress")
#   Or use devtools:
#     devtools::test()
#
# Author: Jonas Wallstein
# Last Updated: 2025-12-03
#
# ===============================================================================

# ---- Setup ----------------------------------------------------------

library(testthat)

# Load necessary libraries
suppressPackageStartupMessages({
  library(terra)
  library(sf)
  library(data.table)
  library(dplyr)
})

# Load the heat package functions
library(heat)

# ---- Helper Functions ----------------------------------------------------------

# Control verbosity of custom prints so reporters (e.g., minimal) stay clean
should_print_banners <- function() {
  # Enable with env var TESTTHAT_VERBOSE=1 or option(testthat.verbose = TRUE)
  nzchar(Sys.getenv("TESTTHAT_VERBOSE")) || isTRUE(getOption("testthat.verbose", FALSE))
}

#' Compare Two Data Frames with Detailed Error Reporting
#'
#' @param actual The actual output from the pipeline
#' @param expected The expected output from baseline
#' @param tolerance Numerical tolerance for comparisons
#' @param label Descriptive label for the comparison
#' @return Invisibly returns TRUE if comparison passes, otherwise throws informative error
compare_outputs <- function(actual, expected, tolerance = 1e-2, label = "output") {
  
  # Check dimensions
  if (nrow(actual) != nrow(expected)) {
    stop(sprintf(
      "%s dimension mismatch:\n  Actual rows: %d\n  Expected rows: %d",
      label, nrow(actual), nrow(expected)
    ))
  }
  
  if (ncol(actual) != ncol(expected)) {
    stop(sprintf(
      "%s dimension mismatch:\n  Actual columns: %d\n  Expected columns: %d",
      label, ncol(actual), ncol(expected)
    ))
  }
  
  # Check column names
  if (!identical(sort(names(actual)), sort(names(expected)))) {
    missing_in_actual <- setdiff(names(expected), names(actual))
    missing_in_expected <- setdiff(names(actual), names(expected))
    
    error_msg <- sprintf("%s column name mismatch:", label)
    if (length(missing_in_actual) > 0) {
      error_msg <- paste0(error_msg, sprintf(
        "\n  Columns in expected but not in actual: %s",
        paste(missing_in_actual, collapse = ", ")
      ))
    }
    if (length(missing_in_expected) > 0) {
      error_msg <- paste0(error_msg, sprintf(
        "\n  Columns in actual but not in expected: %s",
        paste(missing_in_expected, collapse = ", ")
      ))
    }
    stop(error_msg)
  }
  
  # Reorder columns to match
  actual <- actual[, names(expected), drop = FALSE]
  
  # Identify numeric and non-numeric columns
  numeric_cols <- names(actual)[sapply(actual, is.numeric)]
  non_numeric_cols <- setdiff(names(actual), numeric_cols)
  
  # Check non-numeric columns for exact equality
  for (col in non_numeric_cols) {
    if (!identical(actual[[col]], expected[[col]])) {
      # Find first difference
      diffs <- which(actual[[col]] != expected[[col]])
      if (length(diffs) > 0) {
        first_diff <- diffs[1]
        stop(sprintf(
          "%s mismatch in column '%s' at row %d:\n  Actual: %s\n  Expected: %s",
          label, col, first_diff, 
          as.character(actual[[col]][first_diff]),
          as.character(expected[[col]][first_diff])
        ))
      }
    }
  }
  
  # Check numeric columns with tolerance
  for (col in numeric_cols) {
    max_diff <- max(abs(actual[[col]] - expected[[col]]), na.rm = TRUE)
    
    if (max_diff > tolerance) {
      # Find the row with maximum difference
      diff_idx <- which.max(abs(actual[[col]] - expected[[col]]))
      
      # Check for NA mismatches
      na_actual <- is.na(actual[[col]])
      na_expected <- is.na(expected[[col]])
      if (!identical(na_actual, na_expected)) {
        na_mismatch <- which(na_actual != na_expected)[1]
        stop(sprintf(
          "%s NA mismatch in column '%s' at row %d:\n  Actual is NA: %s\n  Expected is NA: %s",
          label, col, na_mismatch,
          na_actual[na_mismatch], na_expected[na_mismatch]
        ))
      }
      
      stop(sprintf(
        "%s numerical difference exceeds tolerance in column '%s':\n  Maximum difference: %.6e (tolerance: %.2e)\n  Occurred at row %d:\n    Actual value: %.6f\n    Expected value: %.6f",
        label, col, max_diff, tolerance, diff_idx,
        actual[[col]][diff_idx], expected[[col]][diff_idx]
      ))
    }
  }
  
  invisible(TRUE)
}

#' Run Pipeline and Compare with Baseline
#'
#' @param trans_type Transformation type name
#' @param trans_args Transformation arguments
#' @param baseline_dir Directory containing baseline files
#' @param env_rast_path Path to environmental raster directory
#' @param geometry Geometry data (polygons or points)
#' @param geom_id_col Polygon/point ID column name
#' @param boundary_dates Date range
test_transformation <- function(trans_type, trans_args, baseline_dir,
                                env_rast_path, geometry, geom_id_col, boundary_dates,
                                sec_weight_rast, out_temp_res, temp_agg_fun = "mean", tolerance = 1e-2) {
  
  # Load baseline outputs
  baseline_daily_path <- file.path(baseline_dir, "daily.rds")
  baseline_monthly_path <- file.path(baseline_dir, "monthly.rds")
  
  expect_true(
    file.exists(baseline_daily_path),
    info = sprintf("Baseline file not found: %s\nRun create_baseline_output.R first!", baseline_daily_path)
  )
  
  expect_true(
    file.exists(baseline_monthly_path),
    info = sprintf("Baseline file not found: %s\nRun create_baseline_output.R first!", baseline_monthly_path)
  )
  
  baseline_daily <- readRDS(baseline_daily_path)
  baseline_monthly <- readRDS(baseline_monthly_path)
  
  # Run the pipeline using r2e2()
  if (should_print_banners()) {
    message(sprintf("\n  Running pipeline with %s transformation...", trans_type))
  }
  
  # Suppress messages and output from r2e2() during tests
  exposures <- suppressMessages(
    r2e2(
      env_rast = env_rast_path,
      geometry = geometry,
      geom_id_col = geom_id_col,
      trans_type = trans_type,
      trans_args = trans_args,
      out_temp_res = out_temp_res,
  temp_agg_fun = temp_agg_fun,
      sec_weight_rast = sec_weight_rast,
      boundary_dates = boundary_dates,
      out_format = "long",
      validation = FALSE,
      save_console_output = FALSE,
      verbose = 0
    )
  )
  
  # Extract results using dynamic naming
  # Input resolution is daily, output is monthly
  actual_daily <- exposures$daily_long
  actual_monthly <- exposures$monthly_long
  
  # Test daily output
  test_that(sprintf("%s transformation produces correct daily output", trans_type), {
    expect_no_error(
      compare_outputs(actual_daily, baseline_daily, tolerance, 
                     label = sprintf("%s daily output", trans_type)),
      message = sprintf("Daily output comparison failed for %s transformation", trans_type)
    )
  })
  
  # Test monthly output
  test_that(sprintf("%s transformation produces correct monthly output", trans_type), {
    expect_no_error(
      compare_outputs(actual_monthly, baseline_monthly, tolerance,
                     label = sprintf("%s monthly output", trans_type)),
      message = sprintf("Monthly output comparison failed for %s transformation", trans_type)
    )
  })
  
  if (should_print_banners()) {
    message(sprintf("  ✓ %s transformation tests passed\n", trans_type))
  }
}

# ---- Test Configuration ----------------------------------------------------------

# Paths to test data
env_rast_path <- testthat::test_path("fixtures", "data", "env_rast")
sec_weight_rast_path <- testthat::test_path("fixtures", "data", "sec_weight_rast")
polygons_path <- testthat::test_path("fixtures", "data", "polygons.gpkg")

# Common parameters
geom_id_col <- "geom_id"
boundary_dates <- c(
  as.Date("1999-12-01"),
  as.Date("2000-03-09")
)

out_temp_res <- "monthly"
temp_agg_fun <- "mean"

# Tolerance for numerical comparisons
tolerance <- 1e-2

# Load polygons once
# Load polygons once
geometry <- read_spatial_file(polygons_path)

# ---- Run Tests ----------------------------------------------------------

if (should_print_banners()) {
  cat("\n")
  cat(strrep("=", 80), "\n")
  cat("TESTING CLIMATE DATA PIPELINE TRANSFORMATIONS\n")
  cat(strrep("=", 80), "\n")
  cat(sprintf("Tolerance: %.0e\n", tolerance))
  cat(strrep("=", 80), "\n\n")
}

# Test 1: None Transformation
if (should_print_banners()) {
  cat("TEST 1: None Transformation\n")
  cat(strrep("-", 80), "\n")
}
test_transformation(
  trans_type = "none",
  trans_args = NULL,
  baseline_dir = testthat::test_path("fixtures", "baseline_output", "polygon_transformations", "none"),
  env_rast_path = env_rast_path,
  geometry = geometry,
  geom_id_col = geom_id_col,
  boundary_dates = boundary_dates,
  sec_weight_rast = sec_weight_rast_path,
  out_temp_res = out_temp_res,
  temp_agg_fun = temp_agg_fun,
  tolerance = tolerance
)

# Test 2: Polynomial Transformation
if (should_print_banners()) {
  cat("TEST 2: Polynomial Transformation\n")
  cat(strrep("-", 80), "\n")
}
test_transformation(
  trans_type = "polynomial",
  trans_args = list(degree = 5),
  baseline_dir = testthat::test_path("fixtures", "baseline_output", "polygon_transformations", "polynomial"),
  env_rast_path = env_rast_path,
  geometry = geometry,
  geom_id_col = geom_id_col,
  boundary_dates = boundary_dates,
  sec_weight_rast = sec_weight_rast_path,
  out_temp_res = out_temp_res,
  temp_agg_fun = temp_agg_fun,
  tolerance = tolerance
)

# Test 3: Natural Spline Transformation
if (should_print_banners()) {
  cat("TEST 3: Natural Spline Transformation\n")
  cat(strrep("-", 80), "\n")
}
test_transformation(
  trans_type = "natural_spline",
  trans_args = list(knots = c(-5, 0, 5)),
  baseline_dir = testthat::test_path("fixtures", "baseline_output", "polygon_transformations", "natural_spline"),
  env_rast_path = env_rast_path,
  geometry = geometry,
  geom_id_col = geom_id_col,
  boundary_dates = boundary_dates,
  sec_weight_rast = sec_weight_rast_path,
  out_temp_res = out_temp_res,
  temp_agg_fun = temp_agg_fun,
  tolerance = tolerance
)

# Test 4: B-Spline Transformation
if (should_print_banners()) {
  cat("TEST 4: B-Spline Transformation\n")
  cat(strrep("-", 80), "\n")
}
test_transformation(
  trans_type = "b_spline",
  trans_args = list(knots = c(-5, 0, 5), degree = 3),
  baseline_dir = testthat::test_path("fixtures", "baseline_output", "polygon_transformations", "b_spline"),
  env_rast_path = env_rast_path,
  geometry = geometry,
  geom_id_col = geom_id_col,
  boundary_dates = boundary_dates,
  sec_weight_rast = sec_weight_rast_path,
  out_temp_res = out_temp_res,
  temp_agg_fun = temp_agg_fun,
  tolerance = tolerance
)

# Test 5: Bin Transformation
if (should_print_banners()) {
  cat("TEST 5: Bin Transformation\n")
  cat(strrep("-", 80), "\n")
}
test_transformation(
  trans_type = "bin",
  trans_args = list(breaks = c(-5, 0, 5)),
  baseline_dir = testthat::test_path("fixtures", "baseline_output", "polygon_transformations", "bin"),
  env_rast_path = env_rast_path,
  geometry = geometry,
  geom_id_col = geom_id_col,
  boundary_dates = boundary_dates,
  sec_weight_rast = sec_weight_rast_path,
  out_temp_res = out_temp_res,
  temp_agg_fun = temp_agg_fun,
  tolerance = tolerance
)

# ---- Test Summary ----------------------------------------------------------

if (should_print_banners()) {
  cat("\n")
  cat(strrep("=", 80), "\n")
  cat("ALL TRANSFORMATION TESTS PASSED ✓\n")
  cat(strrep("=", 80), "\n")
  cat(sprintf("Test completed at: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")))
  cat(strrep("=", 80), "\n\n")
}
