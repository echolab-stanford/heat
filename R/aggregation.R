#' @importFrom terra rast crop resample window crs res ext tapp app
#' @importFrom sf st_geometry_type st_crs st_buffer st_is_longlat st_coordinates st_drop_geometry
#' @importFrom exactextractr exact_extract
#' @importFrom data.table as.data.table setnames setorder rbindlist copy setcolorder setkeyv setorderv
#' @importFrom dplyr left_join mutate select relocate any_of
#' @importFrom lubridate ymd ym ymd_hm year month day hour minute days_in_month
#' @importFrom arrow write_parquet
#' @importFrom progress progress_bar
NULL

#' Get the boundary dates for the weighting periods based on the layer names of the secondary weight raster.
#'
#' @description
#'   This function takes a secondary weight raster and a vector of boundary dates to 
#'   generate a sequence of dates. It then determines the temporal resolution of the 
#'   raster, assigning the relevant dates to the appropriate year, month, or day 
#'   accordingly, and returns the first date in each sequence along with the indices 
#'   of the corresponding secondary weight raster layers.
#'
#' @param sec_weight_raster A SpatRaster object containing secondary weight metrics.
#' @param boundary_dates A numeric vector of two dates defining the date range (start and end).
#'
#' @return A list with two elements:
#'   - A vector of first dates assigned based on the closest secondary weight.
#'   - A vector of indices corresponding to the secondary weight layers used for each assigned date.
get_period_boundaries <- function(sec_weight_raster, boundary_dates) {
  
  boundary_dates <- as.Date.character(boundary_dates, optional= T)
  
  # Check that boundary_dates are dates (transform if necessary) and have exactly two elements
  if (length(boundary_dates) != 2 || any(is.na(boundary_dates))) {
    stop("Error: boundary_dates must be a numeric vector of two valid dates.")
  }
  
  
  # Create a daily sequence of dates
  date_seq <- seq(from = as.Date(boundary_dates[1]), to = as.Date(boundary_dates[2]), by = "day")
  
  # Get the dates from the secondary weight raster layers and determine temporal resolution
  dates <- get_date_cols(sec_weight_raster)
  
  # Ensure dates are properly retrieved from the raster
  if (is.null(dates) || length(dates) == 0) {
    stop("Error: No valid dates in YYYY, YYYY-MM, or YYYY-MM-DD format could be retrieved from the sec_weight_raster layer names.")
  }
  
  temp_res <- get_temp_res(dates)
  
  # If yearly, assign the first of January; if monthly, assign the first of each month
  if (temp_res == "yearly") {
    dates <- as.Date(paste0(dates, '-01-01'))
  } else if (temp_res == "monthly") {
    dates <- as.Date(paste0(dates, '-01'))  # Convert to "YYYY-MM-01"
  }
  # Ensure dates are in Date format
  dates <- as.Date(dates)  
  
  # Create a distance matrix between each date in date_seq and the raster dates
  distance_matrix <- outer(date_seq, dates, FUN = function(x, y) abs(as.numeric(x - y)))
  
  # Find the closest dates
  closest_indices <- apply(distance_matrix, 1, which.min)
  # Create a list to store assigned dates
  assigned_dates <- vector("list", length(dates))
  assigned_indices <- integer(length(date_seq)) # To store the indices of the layers used
  
  # Assign dates based on the closest secondary weights
  for (i in seq_along(date_seq)) {
    closest_date <- dates[closest_indices[i]]
    assigned_dates[[as.character(closest_date)]] <- c(assigned_dates[[as.character(closest_date)]], date_seq[i])
    assigned_indices[i] <- closest_indices[i]  # Capture the index of the closest secondary weight
  }
  
  # Get the first date in each element of the list
  period_boundaries <- as.Date(unlist(lapply(assigned_dates, function(x) if (length(x) > 0) min(x) else NA)))
  # Remove NA values from period_boundaries
  period_boundaries <- period_boundaries[!is.na(period_boundaries)]
  # Add the last date from boundary_dates if necessary
  period_boundaries <- c(period_boundaries, as.Date(boundary_dates[2]))
  # Ensure the period_boundaries are unique and sorted
  period_boundaries <- sort(period_boundaries)
  # get the unique indices of the closest secondary weights
  unique_indices <- unique(assigned_indices)
  
  return(list(boundaries = period_boundaries, indices = unique_indices))  # Return both the assigned dates and their indices}
}




#' Create Periods from Boundary Dates
#'
#' @description
#'   This function generates a structured sequence of time periods based on a vector of boundary dates.
#'   Given a sorted vector of dates, it constructs periods where each start date corresponds to a boundary
#'   date, and the end date is one day before the next boundary date, except for the final period, which
#'   ends exactly at the last boundary date.
#'
#' @param:
#'   - boundary_dates: A sorted vector of dates (length >= 2) defining the breakpoints for periods.
#'
#' @return:
#'   - A data.frame with two columns:
#'       - `start_date`: The starting date of each period.
#'       - `end_date`: The ending date of each period (one day before the next boundary date, except for the last period).
#'
#' @examples
#'   boundary_dates <- as.Date(c("2020-01-01", "2020-06-01", "2020-12-31"))
#'   periods <- create_periods(boundary_dates)
#'   print(periods)
create_periods <- function(boundary_dates) {
  # Ensure boundary_dates is sorted in ascending order and has at least two elements
  n <- length(boundary_dates)
  if (n < 2) stop("Need at least two dates to define one period.")
  
  # Create periods: start_date is each boundary except the last, end_date is one day before the next boundary
  periods <- data.frame(
    start_date = boundary_dates[-n],
    end_date   = boundary_dates[-1] - 1  # Subtract 1 day by default
  )
  
  # Ensure the last period ends exactly at the last boundary date
  periods$end_date[n-1] <- boundary_dates[n]
  
  return(periods)
}



#' Split Raster by Weighting Periods
#'
#' @description
#'   This function splits an already cropped multi-layer SpatRaster into a list of raster stacks,
#'   one for each weighting period defined in the provided period definitions. The period definitions
#'   should be a data.frame with 'start_date' and 'end_date' columns (in Date format). The function
#'   returns a named list of raster stacks, where each element corresponds to a period (e.g., "period_1").
#'
#' @param:
#'   - raster: A cropped SpatRaster with layernames corresponding to the date e.g. name of layer 1: 2017-01-01
#'   - period_defs: A data.frame containing weighting period definitions with the following columns:
#'         - start_date: The start date of the period.
#'         - end_date: The end date of the period.
#'
#' @return:
#'   - A named list of SpatRaster objects, each corresponding to one period.
#'
#' @examples
#'   # Assuming 'cropped_raster' is a cropped SpatRaster with time metadata and
#'   # 'period_defs' is a data.frame with start_date and end_date columns:
#'   periods_list <- assign_weighting_periods(cropped_raster, period_defs)
assign_weighting_periods <- function(raster, period_defs, verbose = 1) {
  
  # Check that period_defs contains the required 'start_date' and 'end_date' columns.
  if (!all(c("start_date", "end_date") %in% names(period_defs))) {
    stop("Error: period_defs must contain 'start_date' and 'end_date' columns.")
  }
  
  # Convert period_defs to Date format.
  period_defs$start_date <- as.Date(period_defs$start_date)
  period_defs$end_date   <- as.Date(period_defs$end_date)
  
  filtered_raster <- filter_env_rast(raster, period_defs$start_date[1], period_defs$end_date[nrow(period_defs)], verbose = verbose)
  raster_dates <- names(filtered_raster)
  
  if (verbose >= 2) {
    message("Assigning raster layers to their respective weighting periods...")
  }

  # Initialize an empty list to store the raster stacks for each period.
  raster_stacks <- list()
  
  for (i in seq_len(nrow(period_defs))) {
    start_date <- period_defs$start_date[i]
    end_date   <- period_defs$end_date[i]
    period_name <- paste0("period_", i)
    
    # Identify raster layers whose dates fall within the current period.
    selected_layers <- which(raster_dates >= start_date & raster_dates <= end_date)
    
    if (length(selected_layers) > 0) {
      # raster_subset <- raster[[selected_layers]]
      raster_subset <- filtered_raster[[selected_layers]]
      raster_stacks[[period_name]] <- raster_subset
      if (verbose >= 2) {
        message("   Assigned ", length(selected_layers), " layers to ", period_name, 
                " (", start_date, " to ", end_date, ").")
      }
    } else {
      message("   Warning: No layers found for ", period_name, 
              " (", start_date, " to ", end_date, ").")
      raster_stacks[[period_name]] <- NULL
    }
  }
  
  if (verbose >= 2) {
    message("Done!")
  }
  return(raster_stacks)
}


#' Check Weighting Periods and Secondary Weight Rasters
#'
#' @description
#' This function verifies that the number of environmental raster periods matches the number of
#' secondary weight raster layers when secondary weighting is requested. If `sec_weights` is TRUE, it
#' stops with an error if the counts do not match and then generates a verification table displaying
#' each period's name, date range, and the corresponding secondary weight raster layer. If `sec_weights`
#' is FALSE, it prints a message stating that no secondary weighting was requested and creates the
#' verification table with the Secondary_Weight_Raster column left empty.
#'
#' @param env_rast_list A list of environmental raster subsets, one per weighting period.
#' @param sec_weight_layer_names (Optional) A character vector of secondary weight raster layer names. Required if sec_weights = TRUE.
#' @param sec_weights Logical flag indicating whether to use secondary weight rasters. Default is TRUE
#' @param sec_weight_name Name of the secondary weight product for display purposes
#'
#' @return A data.frame verification table with columns:
#'   - Period: The name of the period.
#'   - Date_Range: The date range for that period (derived from the raster layer names).
#'   - Secondary_Weight_Raster: The corresponding secondary weight raster layer name (or NA if sec_weights = FALSE).
#'
#' @examples
#' \dontrun{
#'   # When secondary weighting is used:
#'   check_periods(env_rast_list, sec_weight_layer_names, sec_weights = TRUE)
#'
#'   # When secondary weighting is not used:
#'   check_periods(env_rast_list, sec_weights = FALSE)
#' }
check_periods <- function(env_rast_list, sec_weight_layer_names = NULL, sec_weights = TRUE, sec_weight_name, verbose = 1) {
  # Get period names and date ranges from the environmental raster list.
  period_names <- names(env_rast_list)
  period_ranges <- sapply(env_rast_list, function(r) {
    paste0(min(names(r)), " to ", max(names(r)))
  })
  
  if (sec_weights) {
    if (is.null(sec_weight_layer_names)) {
      stop("Secondary weight raster layer names are required when sec_weights = TRUE.")
    }
    if (length(env_rast_list) != length(sec_weight_layer_names)) {
      stop("Error: Mismatch between the number of environmental raster periods (", 
           length(env_rast_list), 
           ") and secondary weight raster layers (", 
           length(sec_weight_layer_names), 
           ").")
    }
    if (verbose >= 2) {
      message("Number of environmental raster periods and secondary weight rasters match. Generating verification table...")
      message("\nPlease verify that the periods align correctly with the secondary weight rasters")
    }
    
  } else {
    if (verbose >= 2) {
      message("\nNo secondary weighting requested (sec_weights = FALSE).")
    }
  }
  
  # Compactly assign the Secondary_Weight_Raster column using an inline if/else.
  sec_values <- if (sec_weights) sec_weight_layer_names else rep("not provided", length(env_rast_list))
  
  verification_table <- data.frame(
    Period = period_names,
    Date_Range = period_ranges,
    Secondary_Weight_Raster = sec_values,
    Secondary_Weight_Product = sec_weight_name,
    stringsAsFactors = FALSE
  )
  
  # Print as properly formatted table
  if (verbose >= 2) {
    message("\nVerification Table:\n")
    message(paste(rep("-", 90), collapse = ""))
    cat("\n")
    print(verification_table, row.names = FALSE)
    cat("\n")
    message(paste(rep("-", 90), collapse = ""))
  }
  
  return(verification_table)
}

#' Create One-Hot Encoded Bin Matrix for Raster Data
#'
#' @description
#' This function converts a vector of raster cell values (across time steps) into a one-hot encoded
#' matrix based on specified bin breaks. It automatically prepends -Inf and appends Inf to the provided
#' inner break points, ensuring that every finite value is captured in one of the bins. The output
#' matrix has dimensions (n_time x n_bins), where each row represents a time step and contains a 
#' one-hot encoding indicating which bin that time step's value falls into.
#'
#' @param x A numeric vector of cell values across time steps.
#' @param breaks A numeric vector specifying the inner break points to be used for binning. The function
#'   automatically prepends `-Inf` and appends `Inf` to this vector.
#' @param ... Additional arguments to be passed to `findInterval`, allowing customization of the binning behavior.
#'
#' @return A matrix with dimensions (n_time x n_bins) where n_time is the length of x and n_bins is 
#'   length(breaks) + 1. Each row contains a one-hot encoding with a single 1 indicating which bin 
#'   that time step's value belongs to, and 0s elsewhere.
#'
#' @examples
#' # Example 1: Basic usage with a cell value vector across 3 time steps and inner breaks at 10 and 20.
#' # The full breaks become c(-Inf, 10, 20, Inf), resulting in 3 bins.
#' # For a cell with values c(2, 15, 30), the output will be a 3x3 matrix.
#' create_bins(c(2, 15, 30), breaks = c(10, 20))
#' #      [,1] [,2] [,3]
#' # [1,]    1    0    0  # value 2 is in bin 1 (-Inf to 10)
#' # [2,]    0    1    0  # value 15 is in bin 2 (10 to 20)
#' # [3,]    0    0    1  # value 30 is in bin 3 (20 to Inf)
#'
#' # Example 2: Passing additional arguments to findInterval.
#' create_bins(c(2, 15, 30), breaks = c(10, 20), rightmost.closed = TRUE)
create_bins <- function(x, breaks, ...) {
  # Construct the full set of breaks: outer boundaries plus the inner ones.
  full_breaks <- c(-Inf, breaks, Inf)
  n_bins <- length(full_breaks) - 1
  n_time <- length(x)

  # Determine the bin index for each time step's value.
  bin_idx <- findInterval(x, full_breaks, ...)

  # Create output matrix: [n_time, n_bins]
  # Each row is a one-hot encoding for that time step
  out <- matrix(0, nrow = n_time, ncol = n_bins)

  # Set the appropriate bin to 1 for each time step
  for (i in seq_len(n_time)) {
    out[i, bin_idx[i]] <- 1
  }

  return(out)
}



#' Select Transformation Function
#'
#' @description
#'   This function selects and returns the appropriate transformation function based on the
#'   provided transformation type. It supports the following transformation types:
#'     - "polynomial": returns the polynomial transformation function from stats::poly.
#'     - "natural_spline": returns the natural spline transformation function from splines::ns.
#'     - "b_spline": returns the B-spline transformation function from splines::bs.
#'     - "custom": returns NULL and prompts the user to assign a custom transformation function.
#'
#' @param:
#'   - trans_type: (character) A string specifying the desired transformation type.
#'       Supported values are "polynomial", "natural_spline", "b_spline", and "custom".
#'
#' @return:
#'   - A function object corresponding to the selected transformation. For example, if
#'     trans_type is "polynomial", it returns stats::poly.
#'
#' @examples
#'   # Select natural spline transformation
#'   ns_fun <- select_trans_fun("natural_spline")
#'
#'   # Select B-spline transformation
#'   bs_fun <- select_trans_fun("b_spline")
select_trans_fun <- function(trans_type, verbose = 1) { 
  
  if (trans_type == "none") {
    if (verbose >= 2) {
      message("No transformation selected: trans_type = 'none'")
    }
    return("none")  # If no transformation is needed, return "none"
  }
  if (trans_type == "polynomial") {
    if (verbose >= 2) {
      message("Selected polynomial transformation using function stats::poly.")
    }
    return(stats::poly)
    
  } else if (trans_type == "natural_spline") {
    if (verbose >= 2) {
      message("Selected natural cubic spline transformation using function splines::ns.")
    }
    return(splines::ns)
    
  } else if (trans_type == "b_spline") {
    if (verbose >= 2) {
      message("Selected B-spline transformation using function splines::bs.")
    }
    return(splines::bs)
    
  } else if (trans_type == "bin") {
    if (verbose >= 2) {
      message("Selected bin transformation using function create_bins (in aggregation_functions.R).")
    }
    return(create_bins)
    
  } else {
    message("Transformation function could not be inferred automatically, since trans_type is not one of 'none', 'polynomial', 'natural_spline', 'b_spline', and 'bin')\n",
            "Please set trans_fun to the desired function and trans_type to the corresponding function name. \n",
            "Please make sure that the function produces the right output format as explained in the climate-data-pipeline canvas under 'Important Information' \n",
            "Example: trans_fun = splines::ns and trans_type = 'natural_spline'")
    return(NULL)
  } 
}



#' Check Transformation Function Arguments
#'
#' @description
#'   This function verifies that the supplied transformation function is valid and that its 
#'   arguments meet the required conditions for processing. It validates that:
#'     - The provided object is a function (or "none")
#'     - The function includes an argument named "x"
#'     - The correct arguments are provided for each transformation type:
#'       * polynomial: requires 'degree'
#'       * bin: requires 'breaks'
#'       * natural_spline/b_spline: requires 'knots'
#'     - If the function is stats::poly, it forces 'raw' to TRUE
#'     - If the function is create_bins, it sorts the breaks
#'
#' @param fun The transformation function to be used (e.g., stats::poly, splines::bs)
#' @param args A list of arguments intended for the transformation function
#' @param trans_type A character string indicating the transformation type for validation
#' @param verbose Integer verbosity level (0 = silent, 1 = concise, 2 = detailed)
#'
#' @return A (possibly modified) list of arguments, ensuring that required conditions are met
#'
#' @examples
#'   # Example for a polynomial transformation:
#'   updated_args <- check_trans(stats::poly, list(degree = 3), "polynomial")
#'   # updated_args now contains degree = 3 and raw = TRUE.
check_trans <- function(fun, args, trans_type = NULL, verbose = 1) {
  
  if (identical(fun, "none")) {
    if (verbose >= 2) {
      message("No transformation function provided (trans_type = 'none'). Skipping transformation.")
    }
    return(args)  # If no transformation is needed, return the original args
  }
  
  # Verify that the supplied fun is indeed a function.
  if (!is.function(fun))
    stop("'fun' must be a function object.")
  
  # Ensure that the function accepts an argument named "x".
  if (all(names(formals(fun)) != "x"))
    stop("'fun' must contain an argument 'x'.")
  
  # Validate that required arguments are provided for each transformation type
  if (!is.null(trans_type)) {
    if (trans_type == "polynomial") {
      if (is.null(args$degree)) {
        stop("Transformation type 'polynomial' requires 'degree' argument. Please provide degree = <integer>.")
      }
      # Warn if bins or knots are provided (likely user error)
      if (!is.null(args$breaks)) {
        warning("Transformation type 'polynomial' does not typically use 'bins' argument. Did you mean to use trans_type = 'bin'?")
      }
      if (!is.null(args$knots)) {
        warning("Transformation type 'polynomial' does not typically use 'knots' argument. Did you mean to use trans_type = 'natural_spline' or 'b_spline'?")
      }
    } else if (trans_type == "bin") {
      if (is.null(args$breaks)) {
        stop("Transformation type 'bin' requires 'bins' argument. Please provide bins = c(<numeric vector>).")
      }
      # Warn if degree or knots are provided
      if (!is.null(args$degree)) {
        warning("Transformation type 'bin' does not typically use 'degree' argument. Did you mean to use trans_type = 'polynomial'?")
      }
      if (!is.null(args$knots)) {
        warning("Transformation type 'bin' does not typically use 'knots' argument. Did you mean to use trans_type = 'natural_spline' or 'b_spline'?")
      }
    } else if (trans_type %in% c("natural_spline", "b_spline")) {
      if (is.null(args$knots)) {
        stop("Transformation type '", trans_type, "' requires 'knots' argument. Please provide knots = c(<numeric vector>).")
      }
      # Note: 'degree' is a valid argument for b_spline, so we don't warn about it
      # Only warn about breaks
      if (!is.null(args$breaks)) {
        warning("Transformation type '", trans_type, "' does not typically use 'bins' argument. Did you mean to use trans_type = 'bin'?")
      }
    }
  }
  
  # If the function is stats::poly (for polynomial transformation),
  # force the argument 'raw' to TRUE.
  if (identical(fun, stats::poly)) {
    if (verbose >= 2) {
      message("Polynomial transformation selected using stats::poly. Forcing argument 'raw' to TRUE.")
    }
    args$raw <- TRUE
  }
  
  if(identical(fun, create_bins)) {
    # Sort the breaks
    args$breaks <- base::sort(args$breaks)
  }
  
  if (verbose >= 2) {
    message("Please make sure the following transformation arguments are correct:")
    print(args)
  }
  
  # Return the (possibly modified) arguments.
  args
}

#' Check Spatial Aggregation Arguments
#'
#' @description
#' This function verifies that the supplied spatial aggregation arguments are valid.
#' If the required arguments are not provided, it sets them to default values:
#'   - fun: "weighted_mean"
#'   - stack_apply: FALSE
#' Additionally, if an appended columns argument ("append_cols") is present, it is removed,
#' and a message is printed indicating that these columns will be added manually later.
#'
#' @param args A list of spatial aggregation arguments.
#'
#' @return A (possibly modified) list of spatial aggregation arguments.
#'
#' @examples
#' spatial_agg_args <- list(
#'   append_cols = c("col1", "col2")
#' )
#' updated_args <- check_spatial_agg_args(spatial_agg_args)
#' # updated_args now contains fun = "weighted_mean", stack_apply = FALSE, and append_cols = NULL.
check_spatial_agg_args <- function(args, verbose = 1) {
  if (is.null(args$fun)) {
    if (verbose >= 2) {
      message("No spatial aggregation function provided. Setting 'fun' to 'weighted_mean'.")
    }
    args$fun <- "weighted_mean"
  }
  if (is.null(args$stack_apply)) {
    if (verbose >= 2) {
      message("No 'stack_apply' value provided. Setting 'stack_apply' to FALSE.")
    }
    args$stack_apply <- FALSE
  }
  if (is.null(args$default_weight)) {
    if (verbose >= 2) {
      message("No 'default_weight' value provided. Setting 'default_weight' to 0, which replaces NA weights with 0")
    }
    args$default_weight <- 0
  }
  if (!is.null(args$append_cols)) {
    if (verbose >= 2) {
      message("Appended columns found in spatial_agg_args. Removing them for now; they will be added manually after the aggregation.")
    }
    args$append_cols <- NULL
  }
  
  if (verbose >= 2) {
    message("Spatial aggregation arguments have been checked successfully.")
    message("Passing the following arguments to exact_extract:")
    print(args)
  }
  
  return(args)
}



#' Aggregate Subdaily Rasters to Daily Values
#'
#' @title Aggregate Subdaily Rasters to Daily Values
#'
#' @description
#'   This function takes a multi-layer `SpatRaster` object containing subdaily data (e.g., hourly)
#'   and aggregates the values to a daily level. The layers are grouped by their corresponding dates,
#'   and an aggregation function (mean or sum) is applied to each group of layers to produce a single
#'   daily layer. The resulting layers are named according to the corresponding dates.
#'
#' @param subdaily_raster A `SpatRaster` object containing subdaily data. Each layer should correspond
#'   to a time step, and layer names should be formatted as "YYYY-MM-DD HH:MM" to facilitate date extraction.
#' @param fun A string specifying the aggregation function. Options are "mean" or "sum".
#'
#' @return A multi-layer `SpatRaster` object containing aggregated daily values, with layers named
#'   in the format "YYYY-MM-DD".
#'
#' @examples
#'   # Assuming 'your_subdaily_raster' is a SpatRaster containing hourly data:
#'   daily_aggregated_raster <- agg_to_daily(your_subdaily_raster, fun = "mean")
#'
#'   # Alternatively, to compute daily sums instead:
#'   daily_sum_raster <- agg_to_daily(your_subdaily_raster, fun = "sum")
#' @export
agg_to_daily <- function(subdaily_raster, fun = "none") {

  if (fun == "none") {
    message("\n     No daily aggregation function provided. Skipping aggregation to daily")
    return(subdaily_raster)  # If no aggregation is needed, return the original raster
  }
  
  else if (fun %in% c("mean", "sum")) {
    message("\n     Aggregating subdaily raster layers to daily values using function: ", fun)
  } else {
    stop("Invalid aggregation function specified. Use 'mean' or 'sum'.")
  }
  # Extract layer names and convert to dates
  layer_names <- names(subdaily_raster)
  
  # Parse dates from layer names assuming they are in "YYYY-MM-DD HH:MM" format
  date_vector <- as.Date(layer_names)  # Convert to Date format)
  
  # Check for NA values resulting from failed date parsing
  if (any(is.na(date_vector))) {
    warning("Some layer names could not be parsed into dates. Ensure they are in the 'YYYY-MM-DD HH:MM' format.")
  }

  # Aggregate the subdaily raster layers by date
  daily_raster <- terra::tapp(subdaily_raster, index = date_vector, fun = match.fun(fun))
  
  # Create a unique list of dates
  unique_dates <- unique(date_vector)
  
  # Set names of the resulting raster layers to the corresponding dates
  names(daily_raster) <- as.character(unique_dates)
  
  return(daily_raster)
  
}



#' Infer Number of Transformation Variables
#'
#' @description
#'   This function infers the number of transformation variables that will be created
#'   based on the specified transformation type and its associated arguments.
#'
#' @param trans_type A character string specifying the type of transformation.
#'                   Expected values include "bin", "polynomial", "natural_spline", "b_spline", or "none".
#' @param trans_args A list of arguments associated with the transformation type, which may
#'                   contain relevant parameters such as breaks, degree, knots, etc.
#'
#' @return An integer representing the number of transformation variables that will be created
#'         based on the logic associated with the transformation type.
#'
#' @examples
#'   num_vars_bin <- infer_num_trans_var(trans_type = "bin", trans_args = list(breaks = c(10, 20, 30)))
#'   print(num_vars_bin)  # Expected output: 4
#'
#'   num_vars_none <- infer_num_trans_var(trans_type = "none", trans_args = list())
#'   print(num_vars_none)  # Expected output: 1
#'
#' @keywords internal
#' @noRd
infer_num_trans_var <- function(trans_type, trans_args) {
  if (trans_type == "none") {
    # If transformation type is "none", return 1
    return(1)
    
  } else if (trans_type == "bin") {
    # Number of bins is the number of breaks + 1
    if (!is.null(trans_args$breaks)) {
      return(length(trans_args$breaks) + 1)
    } else {
      return(10)  # Return 10 as a safety value
    }
    
  } else if (trans_type == "polynomial") {
    # Polynomial transformation returns the degree
    if (!is.null(trans_args$degree)) {
      return(trans_args$degree)
    } else {
      return(10)  # Return 10 as a safety value
    }
    
  } else if (trans_type == "natural_spline") {
    # Natural spline returns knots + 2 
    if (!is.null(trans_args$knots)) {
      return(length(trans_args$knots) + 1)
    } else {
      return(10)  # Return 10 as a safety value
    }
    
  } else if (trans_type == "b_spline") {
    # B-spline returns knots + 2 
    if (!is.null(trans_args$knots)) {
      return(length(trans_args$knots) + 1)
    } else {
      return(10)  # Return 10 as a safety value
    }
    
  } else {
    return(10)  # Return 10 as a safety value for unrecognized types
  }
}

#' Clean Spatial Aggregation Output and Reattach Appended Columns
#'
#' @description
#' This function reshapes the raw spatial aggregation results from `exact_extract` into a long-format 
#' data.table with one row per polygon and transformation variable. It performs the following steps:
#'   1. Converts the extraction result to a data.table.
#'   2. Infers the number of time steps from the supplied `time_steps` vector.
#'   3. Determines the number of transformation variables (`n_trans`) as the number of aggregated columns 
#'      divided by the number of time steps; if this is not a whole number, an error is raised.
#'   4. Splits the remaining aggregated columns into blocks (one block per transformation variable), 
#'      renames these columns using the `time_steps`, and adds the polygon identifier and a transformation 
#'      variable indicator.
#'   5. Combines the results from all blocks 
#'
#' @param extraction_result A data.frame or data.table produced by exact_extract containing only the 
#' aggregated columns 
#' @param time_steps A vector of dates (or character representations) corresponding to the time steps 
#' present in the environmental raster subset.
#' @param geom_id_col A string specifying the column in the polygons object containing the geometry ID.

#'
#' @return A data.table with columns: geom_id, , trans_var, and one 
#' column per time step (named by the corresponding time step). Each row represents a polygon and a transformation 
#' variable.
#'
#' @examples
#' \dontrun{
#'   # Suppose spatial_agg_raw is the result of exact_extract, time_steps is a vector of dates, and 
#'   # polygons have an identifier in "geom_id". If spatial_agg_args$append_cols is provided, those columns 
#'   # will be reattached.
#'   cleaned_DT <- clean_spatial_agg_output(spatial_agg_raw, env_rast_time_steps, "geom_id")
#' }
clean_spatial_agg_output <- function(extraction_result, time_steps, geometry, geom_id_col, verbose = 1) {
  
  # Convert extraction_result to a data.table
  extraction_dt <- as.data.table(extraction_result)

  
  # Determine the number of time steps.
  n_time_steps <- length(time_steps)
  if (verbose == 2) {
    message("              Number of time steps: ", n_time_steps)
  }
  
  # Check that the number of aggregated columns is a multiple of n_time_steps.
  n_total_cols <- ncol(extraction_dt)
  if (n_total_cols %% n_time_steps != 0) {
    stop("Error: The number of aggregated columns (", n_total_cols, 
         ") is not a multiple of the number of time steps (", n_time_steps, "). ",
         "This may indicate that appended columns were not properly separated.")
  }
  n_trans <- n_total_cols / n_time_steps
  if (verbose == 2) {
    message("              Inferred number of transformation variables: ", n_trans)
  }
  
  # Convert time_steps to character names.
  time_step_names <- as.character(time_steps)
  
  # Get the polygon IDs using the metadata specification.
  geom_id_vector <- geometry[[ geom_id_col ]]
  
  # Split the aggregated columns into blocks corresponding to each transformation variable.
  agg_mat <- as.matrix(extraction_dt)
  time_steps_list <- lapply(seq_len(n_trans), function(j) {
    # Define column indices for the j-th transformation variable.
    cols <- ((j - 1) * n_time_steps + 1):(j * n_time_steps)
    dt_j <- as.data.table(agg_mat[, cols, drop = FALSE])
    # Rename the columns using the time step names.
    setnames(dt_j, old = names(dt_j), new = time_step_names)
    # Append the geom_id column.
    dt_j[, geom_id := geom_id_vector]
    # Create a new column 'trans_var' indicating the transformation variable number.
    dt_j[, trans_var := j]
    # Reorder columns: geom_id first, then trans_var, then the time step columns.
    setcolorder(dt_j, c("geom_id", "trans_var", time_step_names))
    dt_j
  })
  
  # Combine all transformation variable blocks.
  combined_DT <- rbindlist(time_steps_list)
  
  # Finally, sort the combined data.table by geom_id.
  setorder(combined_DT, geom_id)
  
  return(combined_DT)
}





#' Core function to Transform and Spatially Aggregate a Raster
#'
#' @description
#' This helper function processes a raster (subset) by applying a specified transformation (e.g., polynomial or spline expansion), performing spatial aggregation using the `exact_extract` function, and cleaning the aggregated output. It prints standardized elapsed time messages for each step (transformation, aggregation, and cleaning) to facilitate performance tracking. The final output is a cleaned data table (or data frame) that includes polygon identifiers and aggregated values for each time step.
#'
#' @param raster_subset A raster object or a subset of a raster to be processed.
#' @param trans_fun A transformation function to be applied to the raster subset.
#' @param checked_trans_args A list of pre-validated arguments for the transformation function.
#' @param geometry An sf object containing spatial geometries (polygons or points) over which the aggregation is performed.
#' @param agg_weights A raster or a character string (e.g., "area") used as weights in the spatial aggregation.
#' @param spatial_agg_args A list of arguments to pass to `exact_extract` for spatial aggregation (e.g., aggregation function, stack_apply).
#' @param geom_id_col The name of the column in the geometry object that identifies each geometry.
#' @param verbose Integer verbosity level (0 = silent, 1 = concise, 2 = detailed).
#'
#' @return A cleaned spatial aggregation output as a data table (or data frame) containing the geometry identifiers and aggregated values for each time step.
#'
#' @examples
#' \dontrun{
#'   result <- trans_spatial_agg_polygons(raster_subset, trans_fun, checked_trans_args,
#'                                    geometry, agg_weights, spatial_agg_args, geom_id_col, verbose = 1)
#' }
trans_spatial_agg_polygons <- function(raster_subset, trans_fun, checked_trans_args, geometry, agg_weights, spatial_agg_args, geom_id_col, verbose = 1) {
  
  step2_start <- Sys.time()
  
  
  if (identical(trans_fun, "none")) {
    if (verbose == 2) {
      message(" [Step 2] Transformation skipped. trans_type = 'none'\n")
    }
    transformed_raster_subset <- raster_subset
    
  } else {
    if (verbose == 2) {
      message(" [Step 2] Applying transformation to environmental raster...")
    }
    transformed_raster_subset <- do.call(terra::app,
                                       c(list(x = raster_subset, fun = trans_fun),
                                         checked_trans_args))
    step2_end <- Sys.time()
    if (verbose == 2) {
      message("          Finished. Elapsed time: ",
              round(difftime(step2_end, step2_start, units = "secs"), 2), " seconds.")
    }
  }

  step3_start <- Sys.time()
  if (verbose == 2) {
    message(" [Step 3] Spatial aggregation with exact_extract...")
  }
  spatial_agg_raw <- do.call(exact_extract,
                             c(list(x = transformed_raster_subset,
                                    y = geometry,
                                    weights = agg_weights),
                               spatial_agg_args,
                               progress = FALSE))
  step3_end <- Sys.time()
  if (verbose == 2) {
    message("          Finished. Elapsed time: ",
            round(difftime(step3_end, step3_start, units = "secs"), 2), " seconds.")
  }

  step4_start <- Sys.time()
  if (verbose == 2) {
    message(" [Step 4] Cleaning spatial aggregation output...")
  }
  # raster_time_steps <- as.Date(names(raster_subset))
  raster_time_steps <- names(raster_subset)
  
  spatial_agg_clean <- clean_spatial_agg_output(spatial_agg_raw, raster_time_steps, geometry, geom_id_col, verbose)
  step4_end <- Sys.time()
  if (verbose == 2) {
    message("          Finished. Elapsed time: ",
            round(difftime(step4_end, step4_start, units = "secs"), 2), " seconds.")
  }

  return(spatial_agg_clean)
}


#' Transform and Spatially Aggregate Environmental Rasters for Point Geometries
#'
#' @description
#' This helper function processes point geometries by extracting raster values directly at point locations, applying a specified transformation to the resulting time series, and formatting the output to match the structure of polygon-based aggregation. It is automatically called by `trans_spatial_agg()` when point geometries are detected. The function uses pre-buffered points that are created once at the beginning of processing for efficiency.
#'
#' @param raster_subset A SpatRaster object containing multiple layers (time steps) of environmental data.
#' @param trans_fun A transformation function to apply to the extracted time series, or "none" for no transformation.
#' @param checked_trans_args A list of pre-validated arguments for the transformation function.
#' @param points_buffered An sf object containing pre-buffered point geometries (required).
#' @param agg_weights Not used for point geometries (included for compatibility with polygon version).
#' @param spatial_agg_args A list of spatial aggregation arguments (included for compatibility).
#' @param geom_id_col The name of the column in the points_buffered object that identifies each point.
#' @param verbose Integer verbosity level (0 = silent, 1 = concise, 2 = detailed).
#'
#' @return A data table containing the point identifiers, transformation variable indicators, and extracted/transformed values for each time step.
#'
#' @examples
#' \dontrun{
#'   result <- trans_spatial_agg_points(raster_subset, trans_fun, checked_trans_args,
#'                                      points_buffered, agg_weights, spatial_agg_args, geom_id_col, verbose = 1)
#' }
trans_spatial_agg_points <- function(raster_subset, 
                                          trans_fun, 
                                          checked_trans_args, 
                                          points_buffered,
                                          agg_weights,
                                          spatial_agg_args, 
                                          geom_id_col,
                                          verbose = 1) {
  
  step2_start <- Sys.time()

  if (verbose == 2) {
    message(" [Step 2] Extracting raster values using exact_extract...")
  }

  # Use exact_extract with simple weighted mean
  # stack_apply = TRUE applies the function to each layer
  extracted_df <- exact_extract(
    raster_subset, 
    points_buffered,
    fun = "mean",  # Simple weighted mean by coverage fraction
    stack_apply = TRUE,
    progress = FALSE
  )
  
  # extracted_df is a data.frame with one row per point and one column per layer
  # Columns are named: mean.layer1, mean.layer2, etc.
  
  # Get layer names
  layer_names <- names(raster_subset)
  
  # Rename columns to match layer names (remove "mean." prefix)
  names(extracted_df) <- layer_names
  
  # Add ID column
  extracted_raw <- cbind(ID = seq_len(nrow(points_buffered)), extracted_df)
  
  step2_end <- Sys.time()
  if (verbose == 2) {
    message("          Finished extraction. Elapsed time: ",
            round(difftime(step2_end, step2_start, units = "secs"), 2), " seconds.")
  }
  
  
  # Step 3: Apply transformation to extracted values
  step3_start <- Sys.time()
  
  # Get the value columns (exclude ID column) - needed for both transformation and formatting
  value_cols <- setdiff(names(extracted_raw), "ID")
  
  if (identical(trans_fun, "none")) {
    if (verbose == 2) {
      message(" [Step 3] Transformation skipped. trans_type = 'none'")
    }
    transformed_values <- extracted_raw
    
  } else {
    if (verbose == 2) {
      message(" [Step 3] Applying transformation to extracted values...")
    }
    
    # Extract just the matrix of values
    values_matrix <- as.matrix(extracted_raw[, value_cols])
    
    # Apply transformation to each ROW (each point's time series)
    transformed_list <- apply(values_matrix, 1, function(point_timeseries) {
      # point_timeseries is a vector of values across time for one point
      # Apply transformation
      trans_result <- do.call(trans_fun, c(list(x = point_timeseries), checked_trans_args))
      
      # Return the transformation matrix
      return(trans_result)
    }, simplify = FALSE)
    
    # transformed_list now contains one matrix per point
    # Each matrix has dimensions: [n_time_steps, n_trans_vars]
  }
  
  step3_end <- Sys.time()
  if (verbose == 2) {
    message("          Finished transformation. Elapsed time: ",
            round(difftime(step3_end, step3_start, units = "secs"), 2), " seconds.")
  }
  
  
  # Step 4: Format output to match polygon version structure
  step4_start <- Sys.time()
  if (verbose == 2) {
    message(" [Step 4] Cleaning output...")
  }
  
  if (identical(trans_fun, "none")) {
    # No transformation: simple reshape
    result_dt <- as.data.table(transformed_values)
    result_dt[, geom_id := points_buffered[[geom_id_col]][ID]]
    result_dt[, trans_var := 1L]
    result_dt[, ID := NULL]
    
    setcolorder(result_dt, c("geom_id", "trans_var", value_cols))
    
  } else {
    # With transformation: reshape to match polygon output structure
    
    # Get dimensions
    n_points <- nrow(points_buffered)
    n_time_steps <- ncol(values_matrix)
    n_trans_vars <- ncol(transformed_list[[1]])
    time_names <- names(raster_subset)
    
    # Create result structure
    result_list <- list()
    
    for (trans_var_idx in 1:n_trans_vars) {
      # For each transformation variable, collect values across all points and time steps
      # Extract column trans_var_idx from each point's transformation matrix
      
      time_values_by_point <- lapply(transformed_list, function(mat) {
        # mat is [n_time_steps, n_trans_vars]
        # Extract the column for this trans_var
        mat[, trans_var_idx]
      })
      
      # Convert list of vectors to matrix
      # Each vector has length n_time_steps
      # Result: [n_points, n_time_steps]
      time_values <- do.call(rbind, time_values_by_point)
      
      # Convert to data.table
      dt <- as.data.table(time_values)
      setnames(dt, time_names)
      
      # Add metadata columns (using "geom_id" as column name for consistency)
      dt[, geom_id := points_buffered[[geom_id_col]]]
      dt[, trans_var := as.integer(trans_var_idx)]
      
      result_list[[trans_var_idx]] <- dt
    }
    
    # Combine all transformation variables
    result_dt <- rbindlist(result_list)
    
    # Reorder columns to match polygon output
    setcolorder(result_dt, c("geom_id", "trans_var", time_names))
  }
  
  step4_end <- Sys.time()
  if (verbose == 2) {
    message("          Finished formatting. Elapsed time: ",
            round(difftime(step4_end, step4_start, units = "secs"), 2), " seconds.")
  }
  
  return(result_dt)
}




#' Transform and Spatially Aggregate Environmental Rasters with Batch Processing and Memory Management
#'
#' @description
#' This function processes environmental raster data over multiple weighting periods by applying a specified transformation
#' and spatially aggregating over polygons. When raster dimensions exceed limits, it splits processing into batches
#' and can save intermediate outputs to disk for memory efficiency. When overwrite_batch_output is FALSE, it checks
#' for existing batch files and validates that the number of layers matches the current processing parameters.
#' 
#' All batch files are kept until the end of processing, allowing the function to resume if it fails partway through.
#' Even periods that fit in a single batch are saved to disk for consistency.
#'
#' @param env_rast_list A list of environmental raster subsets, one per weighting period.
#' @param sec_weight_rast_list A list of secondary weight raster layers (SpatRaster objects), one per weighting period.
#'                             Each element should be a single-layer or multi-layer SpatRaster corresponding to that period.
#'                             Use NULL if sec_weights = FALSE.
#' @param polygons An sf object containing the spatial polygons over which aggregation is performed.
#' @param buffered_extent A buffered extent used to crop the secondary weight rasters.
#' @param trans_type A character string indicating the type of transformation.
#' @param trans_fun A transformation function to be applied to each environmental raster subset.
#' @param checked_trans_args A list of pre-validated arguments for the transformation function.
#' @param spatial_agg_args A list of arguments to pass to `exact_extract` for spatial aggregation.
#' @param geom_id_col The name of the polygon identifier column in the polygons object.
#' @param weighting_periods A data structure containing information for each weighting period.
#' @param save_path Optional character string specifying the output directory. Required if save_batch_output = TRUE.
#' @param sec_weights Logical flag indicating whether to use secondary weight rasters (TRUE) or area weights (FALSE).
#' @param max_cells Numeric maximum allowed number of cells for processing a raster subset at once.
#' @param save_batch_output Logical indicating whether to save intermediate batch outputs to disk for memory efficiency (default: TRUE).
#' @param overwrite_batch_output Logical indicating whether to overwrite existing batch files. When FALSE, existing files
#'                                are validated to ensure layer counts match current processing parameters (default: FALSE).
#'
#' @details
#' When \code{overwrite_batch_output = FALSE}, the function checks for existing batch files with the naming convention:
#' \code{period_XXX_batch_XXX_nlayer_XXXX.rds}. If a file exists but has a different number of layers than expected
#' (indicating changed max_cells parameters), an error is thrown requiring either starting from scratch or using
#' consistent max_cells values.
#'
#' Batch files are saved in RDS format with compression disabled for optimal read/write speed. All batch files are
#' retained until final processing is complete, allowing the function to resume from where it left off if interrupted.
#' 
#' Secondary weight rasters must be in EPSG:4326 coordinate system, otherwise an error will be thrown.
#'
#' @return A data.table containing the final spatial aggregation results.
#'
#' @examples
#' \dontrun{
#'   result <- trans_spatial_agg(env_rast_list, sec_weight_rast_list, polygons, buffered_extent,
#'                               trans_type, trans_fun, checked_trans_args, spatial_agg_args, 
#'                               geom_id_col = "geom_id", weighting_periods, save_path, 
#'                               sec_weights = TRUE, max_cells = 3e7)
#' }
trans_spatial_agg <- function(env_rast_list,
                              sec_weight_rast_list,
                              polygons,
                              crop_extent,
                              trans_type,
                              trans_fun,
                              checked_trans_args,
                              spatial_agg_args,
                              geom_id_col,
                              weighting_periods,
                              save_path,
                              sec_weights = TRUE,
                              max_cells = 3e7,
                              save_batch_output = TRUE,
                              overwrite_batch_output = FALSE,
                              verbose = 1) {
  
  # Start overall timer
  overall_start <- Sys.time()
  
  # Detect geometry type
  geom_types <- st_geometry_type(polygons)
  is_points <- all(grepl("POINT", geom_types))
  
  if (verbose >= 2) {
    if (is_points) {
      message("Point geometries detected. Using optimized point extraction method: \n
    1. Extract raster values at points \n
    2. Apply transformation to extracted values. \n
    Note: The order of operation is reverse compared to polygon geometries, to gain efficiency by applying the transformation only to extracted cells. Secondary weights are not supported with point geometries.")
    } else {
      message("Polygon geometries detected. Using standard polygon aggregation method")
    }
  }
  
  # Validate secondary weights with points
  if (is_points && sec_weights) {
    warning("Secondary weights are not supported with point geometries. Setting sec_weights = FALSE.")
    sec_weights <- FALSE
  }
  
  # Buffer points once at the beginning for efficiency
  polygons_buffered <- NULL
  if (is_points) {
    point_buffer <- 1e-10  # buffer distance in degrees
    point_crs <- st_crs(polygons)
    
    if (is.na(point_crs) || is.null(point_crs)) { 
      stop("Points must have a defined CRS for buffering")
    }
    
    message("Turning points into polygons by creating a negligible buffer, since exact_extract requires polygon inputs...")
    polygons_buffered <- st_buffer(polygons, dist = point_buffer)
    message("  Created buffers of ", point_buffer, " ", 
            ifelse(st_is_longlat(polygons), "degrees", "units"), " around ", 
            nrow(polygons), " points")
  }
  
  # Infer transformation variables and max layers
  num_trans_var <- infer_num_trans_var(trans_type = trans_type, trans_args = checked_trans_args)
  max_layers <- floor(65000 / num_trans_var)
  
  # Setup batch directory if saving to disk
  batch_dir <- NULL
  if (save_batch_output) {
    batch_dir <- file.path(save_path, "temporary_batch_output")
    if (!dir.exists(batch_dir)) {
      dir.create(batch_dir, recursive = TRUE)
    }
  }
  
  # Track all periods and their processing strategy
  period_info <- list()
  
  # Process each weighting period
  for (i in seq_along(env_rast_list)) {
    period_name <- names(env_rast_list)[i]
    if (is.null(period_name) || period_name == "") {
      period_name <- paste0("period_", i)
    }
    
    period_start <- Sys.time()
    if (verbose == 1) {
      cat("  Period ", i, "/", length(env_rast_list), ": ", weighting_periods$Date_Range[i], "\n", sep = "")
    } else if (verbose == 2) {
      message("\n -------------------\n Period ", i, ": ", weighting_periods$Date_Range[i], "\n")
    }
    # Step 1: Secondary Weight Raster Processing
    if (sec_weights) {
      
      # sec_weight_rast_list[[i]] is already a SpatRaster layer
      sec_weight_rast <- sec_weight_rast_list[[i]]
      
      # Check if secondary weight raster is in EPSG:4326
      sec_weight_epsg <- crs(sec_weight_rast, describe = TRUE)$code
      if (is.null(sec_weight_epsg) || is.na(sec_weight_epsg) || sec_weight_epsg != "4326") {
        stop("Secondary weight raster must be in EPSG:4326 coordinate system. Current CRS: ", 
             ifelse(is.null(sec_weight_epsg) || is.na(sec_weight_epsg), "Unknown", paste0("EPSG:", sec_weight_epsg)))
      }
      
      terra::window(sec_weight_rast) <- crop_extent
      env_rast_subset <- env_rast_list[[i]]
      agg_weights <- resample(sec_weight_rast, env_rast_subset, method = "average")
    } else {
      env_rast_subset <- env_rast_list[[i]]
      agg_weights <- "area"
    }
    
    # Determine processing strategy
    dims <- dim(env_rast_subset)
    total_cells <- dims[1] * dims[2] * dims[3]
    
    # Calculate batch size (will be all layers if under max_cells limit)
    num_layers <- min(floor(max_cells / (dims[1] * dims[2])), max_layers, dims[3])
    num_batches <- ceiling(dims[3] / num_layers)
      
      # Initialize progress bar for multiple batches
      pb <- tryCatch({
        if (verbose == 1) {
          progress::progress_bar$new(
            format = "  [:bar] :percent | Batch :current/:total | ETA: :eta",
            total = num_batches,
            clear = FALSE,
            width = 80,
            force = TRUE,  # Force display even when output is redirected
            show_after = 0  # Show immediately (important for Positron compatibility)
          )
        } else {
          NULL
        }
      }, error = function(e) {
        if (verbose == 1) {
          message("Note: progress package not available, using message-based progress")
        }
        NULL
      })
      
      if (!is.null(pb)) {
        pb$tick(0)  # Initialize progress bar display
        flush.console()  # Ensure it's displayed
      }
      
      if (save_batch_output) {
        batch_files <- character()
      } else {
        period_batches <- list()
      }
      
      # Process batches
      layer_indices <- seq(1, dims[3], by = num_layers)
      for (j in seq_along(layer_indices)) {
        start_layer <- layer_indices[j]
        end_layer <- min(start_layer + num_layers - 1, dims[3])
        actual_layers <- end_layer - start_layer + 1
        
        # Create batch filename with layer count (using %04d for layers to handle up to 9999)
        batch_file <- file.path(batch_dir, sprintf("period_%03d_batch_%03d_nlayer_%04d.rds", i, j, actual_layers))
        
        # Check if file exists when not overwriting
        if (save_batch_output && file.exists(batch_file) && !overwrite_batch_output) {
          # Extract layer count from existing filename
          filename <- basename(batch_file)
          existing_layers <- as.integer(sub(".*_nlayer_(\\d+)\\.rds$", "\\1", filename))
          
          if (existing_layers != actual_layers) {
            # Delete the incompatible batch file
            file.remove(batch_file)
            message("Deleted incompatible batch file '", filename, "' (had ", existing_layers, 
                    " layers but expected ", actual_layers, ")")
            stop("Batch file had wrong layer count. max_cells setting has changed. ",
                 "Incompatible file deleted. Please re-run to regenerate all batches with consistent settings.")
          }
          
          if (!is.null(pb)) {
            pb$tick(1)
          } else {
            message("  Skipped batch ", j, "/", num_batches, " (exists)")
          }
          batch_files <- c(batch_files, batch_file)
          next
        }
        # Process batch
        raster_batch <- env_rast_subset[[start_layer:end_layer]]
        
        if (is_points) {
          # Use point-optimized method
          output_batch <- trans_spatial_agg_points(
            raster_subset = raster_batch,
            trans_fun = trans_fun,
            checked_trans_args = checked_trans_args,
            points_buffered = polygons_buffered,
            agg_weights = NULL,
            spatial_agg_args = spatial_agg_args,
            geom_id_col = geom_id_col,
            verbose = verbose
          )
        } else {
          # Use standard polygon method
          output_batch <- trans_spatial_agg_polygons(
            raster_subset = raster_batch,
            trans_fun = trans_fun,
            checked_trans_args = checked_trans_args,
            geometry = polygons,
            agg_weights = agg_weights,
            spatial_agg_args = spatial_agg_args,
            geom_id_col = geom_id_col,
            verbose = verbose
          )
        }
        
        if (!is.null(pb)) {
          pb$tick(1)  # Update progress after batch completes
          flush.console()  # Ensure update is displayed
        } else if (verbose > 0) {
          message("  Completed batch ", j, "/", num_batches)
        }
        
        if (save_batch_output) {
          # Save to disk and clear from memory
          tryCatch({
            saveRDS(output_batch, batch_file, compress = FALSE)
          }, error = function(e) {
            stop("ERROR saving batch ", sprintf("period_%03d_batch_%03d", i, j), ": ", e$message)
          })
          
          batch_files <- c(batch_files, batch_file)
          rm(output_batch)
          gc(verbose = FALSE)
          
        } else {
          # Keep in memory
          period_batches[[j]] <- output_batch
        }
      }
      
      # Store batch information for later loading
      if (save_batch_output) {
        period_info[[i]] <- list(
          batched = TRUE,
          batch_files = batch_files
        )
      } else {
        period_info[[i]] <- list(
          batched = TRUE,
          batches_in_memory = period_batches
        )
      }
    
    period_end <- Sys.time()
  }
  
  # Now combine all periods into final result
  if (verbose >= 2) {
    message("\n===================================================")
    message("COMBINING ALL PERIODS INTO FINAL RESULT")
    message("===================================================")
  }
  
  spatial_agg <- NULL
  
  for (i in seq_along(period_info)) {
    period_name <- names(env_rast_list)[i]
    if (is.null(period_name) || period_name == "") {
      period_name <- paste0("period_", i)
    }
    
    if (verbose >= 2) {
      message("Loading period ", i, "/", length(period_info), ": ", period_name)
    }
    
    if (!period_info[[i]]$batched) {
      # Data already in memory, no batching was used (only when save_batch_output = FALSE)
      spatial_agg_clean <- period_info[[i]]$data
    
    } else if (save_batch_output || !is.null(period_info[[i]]$batch_files)) {
      # Load and combine batches from disk
      batch_files <- period_info[[i]]$batch_files
      if (verbose >= 2) {
        message("  Combining ", length(batch_files), " batch file(s)...")
      }
      
      spatial_agg_clean <- NULL
      for (batch_file in batch_files) {
        if (!file.exists(batch_file)) {
          stop("Batch file not found: ", batch_file)
        }
        
        batch_data <- readRDS(batch_file)
        if (is.null(spatial_agg_clean)) {
          spatial_agg_clean <- batch_data
        } else {
          spatial_agg_clean <- cbind(spatial_agg_clean, batch_data[, !c("geom_id", "trans_var"), with = FALSE])
        }
      }
      
    } else {
      # Combine batches from memory
      period_batches <- period_info[[i]]$batches_in_memory
      if (verbose >= 2) {
        message("  Combining ", length(period_batches), " batches from memory...")
      }
      
      spatial_agg_clean <- period_batches[[1]]
      if (length(period_batches) > 1) {
        for (k in 2:length(period_batches)) {
          spatial_agg_clean <- cbind(spatial_agg_clean, 
                                     period_batches[[k]][, !c("geom_id", "trans_var"), with = FALSE])
        }
      }
    }
    
    # Append period result to final result
    if (is.null(spatial_agg)) {
      spatial_agg <- spatial_agg_clean
    } else {
      spatial_agg <- cbind(spatial_agg, spatial_agg_clean[, !c("geom_id", "trans_var"), with = FALSE])
    }
    
    if (verbose >= 2) {
      message("  Period ", i, " combined successfully")
    }
  }
  
  # Clean up all batch files only after successful completion
  if (save_batch_output && !is.null(batch_dir) && dir.exists(batch_dir)) {
    if (verbose >= 2) {
      message("\nCleaning up temporary batch files...")
    }
    unlink(batch_dir, recursive = TRUE)
    if (verbose >= 2) {
      message("Cleanup complete")
    }
  }
  
  overall_end <- Sys.time()
  overall_total <- round(difftime(overall_end, overall_start, units = "min"), 2)
  if (verbose >= 2) {
    message("\n===================================================")
    message("All processing completed in ", overall_total, " minutes.")
    message("Final result has ", ncol(spatial_agg), " columns.")
    message("===================================================")
  }
  
  return(spatial_agg)
}

#' Add Metadata Columns to Data Table
#'
#' @description
#' This function adds metadata columns to a spatial aggregation data.table (dt). Specifically, it:
#'   - Adds a column "trans_type" with the provided transformation type.
#'   - Adds columns from the supplied metadata (a named list) to dt, if provided.
#'   - Concatenates the transformation arguments (provided as a named list) into a single string,
#'     stored in the "trans_call" column.
#'   - Reorders the columns so that key metadata fields appear first: "geom_id", "trans_type", "trans_var"
#'     then the metadata columns (if any), followed by "trans_call", and finally any remaining columns.
#'
#' @param dt A data.table containing spatial aggregation results.
#' @param trans_type A string specifying the transformation type (e.g., "polynomial").
#' @param metadata A named list of metadata values to add as new columns; defaults to NULL.
#' @param trans_args A named list of transformation arguments; these will be concatenated into a string.
#'
#' @return The modified data.table (dt) with the added metadata columns and reordered columns.
#'
#' @examples
#' \dontrun{
#'   dt <- data.table(geom_id = 1:3, trans_var = c("A", "B", "C"), other_col = 1:3)
#'   trans_type <- "polynomial"
#'   metadata <- list(source = "ERA5", resolution = "0.25")
#'   trans_args <- list(degree = 3, raw = TRUE)
#'
#'   dt <- add_metadata_cols(dt, trans_type, metadata, trans_args)
#' }
add_metadata_cols <- function(dt, trans_type, metadata = NULL, trans_args) {
  # Add the transformation type column.
  dt[, trans_type := trans_type]
  
  # Add the metadata columns if provided.
  if (!is.null(metadata)) {
    dt[, (names(metadata)) := metadata]
  }
  
  # Concatenate the transformation arguments into a single string.
  trans_args_str <- paste0(names(trans_args), " = ", sapply(trans_args, deparse), collapse = ", ")
  
  # Add the trans_call column.
  dt[, trans_call := trans_args_str]
  
  # Reorder columns: base columns first, then any remaining columns.
  base_cols <- c("geom_id", "trans_type", "trans_var", names(metadata), "trans_call")
  # If metadata is NULL, omit it from base_cols
  if (is.null(metadata)) {
    base_cols <- setdiff(base_cols, names(metadata))
  }
  
  remaining_cols <- setdiff(names(dt), base_cols)
  new_order <- unique(c(base_cols, remaining_cols))
  
  # Set the new column order.
  setcolorder(dt, new_order)
  
  return(dt)
}



#' Count and Report Missing Date Values in a Data Table
#'
#' @description
#' This function checks the specified date columns in a data.table (or data.frame) for missing (NA) values.
#' It first makes a shallow copy of the input data.table to avoid issues with internal self-references.
#' The function then flags rows where any of the specified date columns are NA, extracts the unique polygon IDs
#' from the "geom_id" column for those rows, and prints a warning if any are found. If no missing values are detected,
#' a message is printed indicating that all polygons have complete date data.
#'
#' @param dt A data.table (or data.frame) containing the date columns and a "geom_id" column.
#' @param date_columns A character vector specifying the names of the date columns to check.
#'
#' @return A vector of unique polygon IDs (from the "geom_id" column) for which at least one specified date column is missing.
#'
#' @examples
#' \dontrun{
#'   library(data.table)
#'   dt <- data.table(geom_id = 1:5,
#'                    "2020-01-01" = c(1, NA, 3, 4, NA),
#'                    "2020-01-02" = c(NA, 2, NA, 4, 5))
#'   missing_polys <- get_missing_vals(dt, date_columns = c("2020-01-01", "2020-01-02"))
#' }
get_missing_vals <- function(dt, date_columns, verbose = 1) {

  # Make a shallow copy to avoid .internal.selfref warnings.
  dt <- copy(dt)
  
  # Ensure dt is a data.table (it should be after copy)
  if (!("data.table" %in% class(dt))) {
    dt <- as.data.table(dt)
  }
  
  # Ensure the 'geom_id' column exists.
  if (!("geom_id" %in% names(dt))) {
    stop("Column 'geom_id' not found in dt.")
  }
  
  # Identify rows with any NA in the specified date columns.
  dt[, any_na := Reduce(`|`, lapply(.SD, is.na)), .SDcols = date_columns]
  
  # Extract unique polygon IDs for rows where any_na is TRUE.
  polys_with_na <- unique(dt[any_na == TRUE, geom_id])
  
  # Remove the temporary column.
  dt[, any_na := NULL]
  
  if (length(polys_with_na) > 0) {
    if (verbose == 1) {
      message("   Missing values detected in ", length(polys_with_na), " polygon(s).")
    } else if (verbose >= 2) {
      message("The following polygon(s) have missing values in at least one date column. \n  This can happen if all cells in the environmental raster that a polygon covers are missing values. This is often the case for islands. \n  Polygon IDs: ", 
              paste(polys_with_na, collapse = ", "))    # print(polys_with_na)
    }
  } else {
    if (verbose >= 2) {
      message("No missing values found for any polygon.")
    }
  }
  
  return(polys_with_na)
}


#' Get Area Weights from Raster Extraction
#'
#' @description
#' This function extracts cell information from a single-layer raster for each geometry (polygon or point) 
#' in an sf object. For polygons, it uses exact_extract to get all overlapping cells with their coordinates.
#' For points, it extracts the exact x, y coordinates of each point. The output includes geometry identifiers
#' and coordinates, useful for spatial aggregation tasks.
#'
#' @param raster A single-layer SpatRaster from which to extract cell information.
#' @param polygons An sf object containing polygon or point geometries.
#' @param geom_id_col A string indicating the column name in the polygons object that contains the geometry identifier.
#'
#' @return A data frame with columns:
#'   - geom_id: The geometry identifier (renamed from the column specified by geom_id_col).
#'   - x: The x-coordinate (cell center for polygons, exact coordinate for points).
#'   - y: The y-coordinate (cell center for polygons, exact coordinate for points).
#'
#' @examples
#' \dontrun{
#'   # For polygons:
#'   area_weights <- get_area_weights(env_rast[[1]], polygons, geom_id_col = "geom_id")
#'   
#'   # For points:
#'   area_weights <- get_area_weights(env_rast[[1]], points, geom_id_col = "site_id")
#'   
#'   arrow::write_parquet(area_weights, file.path(paths$path_out_folder, "area_weights.parquet"))
#' }
#' 
#' @export
get_area_weights <- function(raster, polygons, geom_id_col) {
  
  # Check that the polygons object contains the specified geom_id_col
  if (!(geom_id_col %in% names(polygons))) {
    stop("The polygons object must have a column named ", geom_id_col)
  }
  
  # Check geometry type
  geom_types <- st_geometry_type(polygons)
  is_points <- all(grepl("POINT", geom_types))
  
  if (is_points) {
    # For point geometries, extract coordinates directly
    coords <- st_coordinates(polygons)
    area_weights_df <- data.frame(
      geom_id = polygons[[geom_id_col]],
      x = coords[, "X"],
      y = coords[, "Y"]
    )
  } else {
    # For polygon geometries, use exact_extract
    # Extract cell information (including x, y) for each polygon.
    # This returns a list of data frames (one per polygon).
    area_weights_list <- exact_extract(raster, polygons, include_xy = TRUE, progress = FALSE)
    
    # Combine the list into a single data frame and add the corresponding polygon id.
    area_weights_df <- do.call(rbind, lapply(seq_along(area_weights_list), function(i) {
      df <- area_weights_list[[i]]
      df$geom_id <- polygons[[geom_id_col]][i]
      df
    }))
    
    # Select only the desired columns: geom_id, x, and y.
    area_weights_df <- dplyr::select(area_weights_df, geom_id, x, y)
  }
  
  return(area_weights_df)
  
}









#' Temporal Aggregation of Wide Spatial Extraction Results (With Optional Metadata Retention)
#'
#' @description
#'   This function aggregates the wide-format spatial extraction output over time according
#'   to a specified temporal resolution. It robustly identifies date columns using lubridate
#'   and a regex check (accepting formats "YYYY", "YYYY-MM", or "YYYY-MM-DD"). An aggregation key is
#'   created based on the desired resolution ("daily", "monthly", or "yearly"), and the specified
#'   aggregation function (default: sum) is applied row-wise to columns with the same key.
#'
#'   By default, the function retains all non-date columns (metadata) present in the input.
#'   If `keep_metadata = FALSE`, only the identifier columns (defined as "geom_id" and "trans_var")
#'   are retained.
#'
#' @param:
#'   - spatial_output: A data.table or data.frame produced by spatial aggregation, containing
#'         identifier columns (e.g., "geom_id" and "trans_var") and additional columns whose names
#'         represent dates in one of the formats: "YYYY", "YYYY-MM", or "YYYY-MM-DD". Any additional
#'         columns are assumed to be metadata.
#'   - agg_args: A list of aggregation arguments including:
#'         - out_temp_res: character; the desired output temporal resolution ("daily", "monthly", or "yearly").
#'         - temp_agg_fun: function; the aggregation function to apply (default is sum if not specified).
#'         - Any other arguments are passed to the aggregation function.
#'   - keep_metadata: Logical (default TRUE). If TRUE, retains all non-date columns from spatial_output.
#'                    If FALSE, only the identifier columns ("geom_id" and "trans_var") are retained.
#'
#' @return:
#'   - A wide-format data.table with the identifier columns (and, if keep_metadata = TRUE, additional metadata columns)
#'     and one column per aggregated time period (named by the aggregated date). For daily resolution, the original
#'     data is returned.
#'
#' @examples
#'   agg_args <- list(
#'     out_temp_res = "monthly",
#'     temp_agg_fun = mean
#'   )
#'   aggregated_DT <- temp_agg(spatial_output, agg_args)
#' 
#' @export
temp_agg <- function(spatial_output, agg_args, keep_metadata = TRUE, verbose = 1) {
  
  temp_agg_start <- Sys.time()
  
  out_temp_res <- agg_args$out_temp_res
  
  # Define identifier columns.
  id_vars <- c("geom_id", "trans_var", "trans_type")
  
  # Identify all columns other than the id_vars.
  other_cols <- setdiff(names(spatial_output), id_vars)
  
  # Identify date columns robustly from the remaining column names.
  date_cols <- get_date_cols(spatial_output)
  
  # Get temporal resolution
  in_temp_res <- get_temp_res(date_cols)
  
  if (out_temp_res == in_temp_res) {
    if (verbose >= 1) {
      message("Output temporal resolution is the same as input temporal resolution. Returning original data.")
    }
    return(spatial_output)
  }
  
  
  if (verbose >= 2) {
    message("Aggregating from ", in_temp_res, " to ", out_temp_res)
  }
  
  
  # Ensure the aggregation function is defined; default to sum.
  temp_agg_fun <- agg_args$temp_agg_fun
  if (is.null(temp_agg_fun)) {
    temp_agg_fun <- mean
    if (verbose >= 2) {
      message("temp_agg_fun not specified by user, defaulting to mean for aggregating over time (if out_temp_res is different from input temporal resolution)")
    }
  }
  
  extra_arg_names <- setdiff(names(agg_args), c("out_temp_res", "temp_agg_fun"))
  if (length(extra_arg_names) > 0) {
    if (verbose >= 2) {
      message("Additional aggregation arguments: ", paste(extra_arg_names, collapse = ", "))
    }
  }
  
  # # Define identifier columns.
  # id_vars <- c("geom_id", "trans_var", "trans_type")
  # 
  # # Identify all columns other than the id_vars.
  # other_cols <- setdiff(names(spatial_output), id_vars)
  # 
  # # Identify date columns robustly from the remaining column names.
  # date_cols <- get_date_cols(spatial_output)
  
  # Identify metadata columns (non-date columns).
  metadata_cols <- setdiff(other_cols, date_cols)
  
  # Create an aggregation key mapping based on the desired temporal resolution.
  if (out_temp_res == "daily") {
    agg_keys <- format(as.Date(date_cols), "%Y-%m-%d")
  } else if (out_temp_res == "monthly") {
    agg_keys <- format(as.Date(date_cols), "%Y-%m")
  } else if (out_temp_res == "yearly") {
    agg_keys <- format(as.Date(date_cols), "%Y")
  } else {
    stop("Unsupported temporal resolution: ", out_temp_res, "\n  Supported temporal resolutions: 'daily', 'monthly', 'yearly'")
  }
  
  unique_keys <- unique(agg_keys)
  
  # For each unique key, aggregate the corresponding columns row-wise.
  aggregated_cols <- lapply(unique_keys, function(key) {
    # Find columns corresponding to this aggregation key.
    cols <- date_cols[agg_keys == key]
    # Apply the aggregation function row-wise over these columns.
    agg_vals <- apply(as.matrix(spatial_output[, ..cols]), 1, function(x) {
      do.call(temp_agg_fun, c(list(x = x), agg_args[setdiff(names(agg_args), c("out_temp_res", "temp_agg_fun"))]))
    })
    return(agg_vals)
  })
  
  # Combine the aggregated columns into a data.table.
  agg_dt <- as.data.table(aggregated_cols)
  # Rename the aggregated columns using the unique_keys.
  setnames(agg_dt, unique_keys)
  
  # Determine which identifier columns to keep.
  if (keep_metadata) {
    dt_ids <- spatial_output[, c(id_vars, metadata_cols), with = FALSE]
  } else {
    dt_ids <- spatial_output[, ..id_vars]
  }
  
  # Combine the identifier (and metadata) columns with the aggregated results.
  result_dt <- cbind(dt_ids, agg_dt)
  
  # Order the result by geom_id.
  setorder(result_dt, geom_id)
  
  temp_agg_end <- Sys.time()
  if (verbose >= 2) {
    message("Temporal aggregation completed in ", 
            round(difftime(temp_agg_end, temp_agg_start, units = "secs"), 2), " seconds.")
  }
  
  return(result_dt)
}







rename_trans_var <- function(spatial_agg, trans_type, trans_args = list(), verbose = 1) {
  # Check that the 'trans_var' column exists in spatial_agg.
  if (!("trans_var" %in% names(spatial_agg))) {
    stop("The input data.table does not have a 'trans_var' column.")
  }
  
  # If trans_var is not numeric, assume it is already renamed and skip renaming with a warning.
  if (!is.numeric(spatial_agg$trans_var)) {
    warning("trans_var is not numeric; it is assumed to be already renamed. No renaming applied.")
    return(spatial_agg)
  }
  
  # For trans_type = none: rename trans_var to 'value'
  if (trans_type == "none") {
    if (verbose >= 2) {
      message("Renaming trans_var to 'value' for trans_type = 'none'")
    }
    spatial_agg[, trans_var := rep("value", .N)]
  }
  
  # For polynomial transformation: rename trans_var values to "degree_x"
  else if (trans_type == "polynomial") {
    if (verbose >= 2) {
      message("Renaming trans_var for polynomial transformation: prefixing with 'degree_'.")
    }
    spatial_agg[, trans_var := paste0("degree_", trans_var)]
    
    # For spline transformation: rename trans_var values to "term_x"
  } else if (trans_type == "natural_spline" | trans_type == "b_spline") {
    if (verbose >= 2) {
      message("Renaming trans_var for spline transformation: prefixing with 'term_'.")
    }
    spatial_agg[, trans_var := paste0("term_", trans_var)]
    
    # For bin transformation: use boundary_knots to generate bin names.
  } else if (trans_type == "bin") {
    # Check that 'boundary_knots' is provided in trans_args.
    if (is.null(trans_args$breaks)) {
      stop("For bin transformation, 'breaks' must be provided in trans_args.")
    }
    
    # Sort the provided boundary knots.
    bin_breaks <- sort(trans_args$breaks)
    
    # Create names for new columns.
    list_names <- sapply(0:(length(bin_breaks)), FUN = function(x) {
      if (x == 0) {
        paste("bin", "ninf", "to", sub("-", "n", min(bin_breaks)), sep = "_")
      } else if (x == length(bin_breaks)) {
        paste("bin", sub("-", "n", max(bin_breaks)), "to", "inf", sep = "_")
      } else {
        paste("bin", sub("-", "n", bin_breaks[x]), "to", sub("-", "n", bin_breaks[x + 1]), sep = "_")
      }
    })
    
    if (verbose >= 2) {
      message("Renaming trans_var for bin transformation using boundary_knots. Generated names: ",
              paste(list_names, collapse = ", "))
    }
    
    # Map each numeric trans_var to the corresponding name from list_names.
    # Issue a warning if the maximum value in trans_var doesn't match the expected number of bins.
    unique_vals <- sort(unique(as.integer(spatial_agg$trans_var)))
    if (max(unique_vals) != length(list_names)) {
      warning("The maximum value in trans_var (", max(unique_vals), 
              ") does not match the expected number of bins (", length(list_names), "). Proceeding with renaming based on position.")
    }
    
    spatial_agg[, trans_var := list_names[as.integer(trans_var)]]
    
  } else {
    message("Transformation type '", trans_type, 
            "' not recognized for renaming trans_var. No renaming applied.")
  }
  
  return(spatial_agg)
}

#' Add Appended Columns to Aggregation Results
#'
#' @description
#' This function optionally appends additional columns from an sf object (polygons) to a 
#' long-format spatial aggregation data frame (`temp_agg_long`). It does so by:
#'   1. Dropping the geometry from the polygons.
#'   2. Creating a new column named "geom_id" using the column specified by `geom_id_col`.
#'   3. Checking for column name conflicts (other than "geom_id") and renaming conflicting
#'      columns in polygons by appending '_SPATIAL_FILE'.
#'   4. Selecting the "geom_id" and the specified appended columns.
#'   5. Left joining these columns to `temp_agg_long` by "geom_id".
#'   6. Relocating the appended columns to immediately follow the "geom_id" column.
#'
#' @param temp_agg_long A data frame containing long-format spatial aggregation results that includes a "geom_id" column.
#' @param polygons An sf object containing spatial polygon data and additional attributes.
#' @param geom_id_col A string representing the column name in `polygons` that uniquely identifies each polygon.
#' @param appended_cols A character vector of column names in `polygons` to append to `temp_agg_long`.
#'
#' @return A data frame with the appended columns merged (and relocated immediately after "geom_id").
#'
#' @examples
#' \dontrun{
#'   # Suppose temp_agg_long has a column "geom_id", and polygons is an sf object with an identifier
#'   # column "ID" and additional columns "region" and "area" that you want to add:
#'   result <- add_appended_cols(temp_agg_long, polygons, geom_id_col = "ID", appended_cols = c("region", "area"))
#' }
add_appended_cols <- function(temp_agg_long, polygons, geom_id_col, appended_cols) {
  
  if (!is.null(appended_cols)) {
    # Get column names from both datasets
    temp_agg_cols <- names(temp_agg_long)
    polygons_cols <- names(polygons)
    
    # Find conflicting column names (excluding geom_id which will be the join key)
    conflicting_cols <- intersect(temp_agg_cols, polygons_cols)
    conflicting_cols <- conflicting_cols[conflicting_cols != "geom_id"]
    
    # Start with polygons data
    polygons_data <- polygons %>% st_drop_geometry()
    
    # Handle conflicts by removing them from appended_cols
    if (length(conflicting_cols) > 0) {
      conflicting_appended <- intersect(appended_cols, conflicting_cols)
      
      if (length(conflicting_appended) > 0) {
        message("Columns in the spatial file intended to be kept (part of appended_cols) conflict with aggregation output columns. Dropping conflicting columns from spatial file: ", 
                paste(conflicting_appended, collapse = ", "))
        
        # Remove conflicting columns from appended_cols
        appended_cols <- setdiff(appended_cols, conflicting_appended)
        
        if (length(appended_cols) == 0) {
          message("No columns left to append after removing conflicts")
          return(temp_agg_long)
        }
        
        message("Remaining columns to append: ", paste(appended_cols, collapse = ", "))
      }
    }
    
    # Check that all remaining appended_cols exist in polygons_data
    missing_cols <- setdiff(appended_cols, names(polygons_data))
    if (length(missing_cols) > 0) {
      stop("Missing columns in polygons data: ", paste(missing_cols, collapse = ", "))
    }
    
    # Proceed with the join
    temp_agg_long <- left_join(
      temp_agg_long,
      polygons_data %>% 
        mutate(geom_id = .data[[geom_id_col]]) %>% 
        dplyr::select(all_of(c("geom_id", appended_cols))),
      by = "geom_id"
    ) %>% relocate(any_of(appended_cols), .after = "geom_id")
  }
  
  return(temp_agg_long)
}

