#' @importFrom terra rast window xapp
#' @importFrom lubridate year ymd_hm
NULL

#' Mean Interpolation for Daily Temperature Values
#'
#' @description
#'   This function computes a simple mean interpolation from daily minimum and 
#'   maximum temperature values by calculating the arithmetic mean (tmin + tmax) / 2
#'   for each day. This provides a single representative daily temperature value
#'   that falls halfway between the daily extremes.
#'
#' @param tmin A numeric vector of daily minimum temperatures, where each value 
#'             corresponds to a day. When used with rasters, this represents the 
#'             values from the tmin raster layers.
#' @param tmax A numeric vector of daily maximum temperatures, where each value 
#'             corresponds to a day. When used with rasters, this represents the 
#'             values from the tmax raster layers. Must be the same length as tmin.
#'
#' @return A numeric vector containing the computed mean temperatures for each day.
#'         The length of the returned vector equals the length of the input vectors.
#'         When used with rasters, the output layers retain the same names as the 
#'         input tmin layers, formatted as dates.
#'
#' @details
#'   This function validates that the layer names from the tmin input can be 
#'   converted to valid dates. It expects date names in a standard format 
#'   (e.g., "YYYY-MM-DD"). The function preserves the original date formatting
#'   in the output layer names.
#'
#' @note
#'   This function is designed to work with the `interpol_min_max()` 
#'   framework and can be passed as the `interpol_fun` argument. When using
#'   this interpolation method, set `daily_agg_fun = "none"` since the output
#'   is already at daily resolution.
mean_interpol <- function(tmin, tmax) {
  # Get the layer names from the tmin raster
  dates <- names(tmin) 
  
  # Calculate simple mean between tmin and tmax
  mean_temps <- (tmin + tmax) / 2
  
  # Try converting layer names to Date format and handle errors
  raster_dates <- tryCatch(
    as.Date(dates),
    error = function(e) {
      stop("Error: Raster layer names could not be converted to valid dates. ",
           "Ensure they are in a recognizable date format such as 'YYYY-MM-DD'.")
    }
  )
  
  # Ensure that at least some raster layer names were successfully converted
  if (all(is.na(raster_dates))) {
    stop("Error: All raster layer names are NA after attempting date conversion. ",
         "Please check the naming format.")
  }
  
  # Keep the original date names (just the date part, no time)
  names(mean_temps) <- raster_dates
  
  return(mean_temps)
}



#' Sinusoidal Interpolation for Hourly Temperature Values
#'
#' @description
#'   This function computes hourly temperature interpolations from daily minimum and 
#'   maximum temperature values using a sinusoidal approach. It calculates interpolated 
#'   hourly temperatures for each day represented in the input tmin and tmax vectors.
#'
#' @param tmin A numeric vector of daily minimum temperatures (tmin), where each value corresponds to a day.
#' @param tmax A numeric vector of daily maximum temperatures (tmax), where each value corresponds to a day.
#' @param hour A numeric vector representing the hours of the day (1 to 24).
#'
#' @return A numeric vector containing the computed hourly temperatures for each hour across all days represented 
#'         in the input tmin and tmax vectors. The length of the returned vector will be 24 times the number of days 
#'         represented by the input vectors. The output layer are named in format "YYYY-MM-DD HH:MM", where the date corresponds to the day of the respective tmin and tmax values.
#'
#' @examples
#' \dontrun{
#'   # Assuming tmin and tmax are numeric vectors of length equal to the number of days:
#'   hourly_temperatures <- sinusoidal_interpol(tmin = c(15, 16, 14), 
#'                                                   tmax = c(22, 23, 21), 
#'                                                   hour = 1:24)
#' }
sinusoidal_interpol <- function(tmin, tmax, hour = 1:24) {
  # Broadcast the tmin and tmax temperatures for all hours
  # length(tmin) and length(tmax) should be the same and correspond to days
  
  # Get the layer names from the tmin raster
  dates <- names(tmin) 
  # Number of days
  n_days <- length(dates)
  
  # Create indices to repeat hour values for each day
  hours <- rep(hour, times = n_days)  # Replicate hours for all days
  
  # Calculate interpolated hourly temperatures
  hourly_temps <- (rep(tmax, each = 24) + rep(tmin, each = 24)) / 2 +
    ((rep(tmax, each = 24) - rep(tmin, each = 24)) / 2) *
    sin(pi * (hours - 6) / 12)
  
  
  # Try converting layer names to Date format and handle errors
  raster_dates <- tryCatch(
    as.Date(dates),
    error = function(e) {
      stop("Error: Raster layer names could not be converted to valid dates. ",
           "Ensure they are in a recognizable date format such as 'YYYY-MM-DD'.")
    }
  )
  
  # Ensure that at least some raster layer names were successfully converted
  if (all(is.na(raster_dates))) {
    stop("Error: All raster layer names are NA after attempting date conversion. ",
         "Please check the naming format.")
  }
  
  # Get the overall raster date range
  raster_start <- min(raster_dates, na.rm = TRUE)
  raster_end <- max(raster_dates, na.rm = TRUE)
  
  # Assign names to the layers in the format YYYY-MM-DD HH:MM
  start_time <- ymd_hm(paste(raster_start, "00:00"))  # Use ymd_hm for date-time
  end_time <- ymd_hm(paste(raster_end, "23:00"))      # and for end time
  
  # Create hourly sequence based on start_time and end_time
  hours_seq <- seq(from = start_time, to = end_time, by = "hour")
  
  # Format the layer names as "YYYY-MM-DD HH:MM"
  layer_names <- format(hours_seq, "%Y-%m-%d %H:%M") 
  
  names(hourly_temps) <- layer_names
  
  return(hourly_temps)
}


#' Interpolate Daily Minimum and Maximum Temperatures to Obtain Hourly Values
#'
#' @description
#'   This function interpolates daily minimum and maximum raster values using a specified 
#'   interpolation function. The function 
#'   also aggregates the hourly data back to daily values if requested.
#'   It handles large raster datasets by splitting them into manageable batches to avoid 
#'   processing limitations.
#'
#' @param min_rast Either a SpatRaster object or a character string path to the directory 
#'                  containing minimum temperature raster files.
#' @param max_rast Either a SpatRaster object or a character string path to the directory 
#'                  containing maximum temperature raster files.
#' @param geometry An sf object that defines geographical boundaries, used for cropping the rasters.
#' @param start_date Character string or Date object specifying the start date for interpolation (e.g., "2022-01-01").
#'                   If NULL and rasters are provided as objects, will use the full date range of the rasters.
#' @param end_date Character string or Date object specifying the end date for interpolation (e.g., "2022-01-07").
#'                 If NULL and rasters are provided as objects, will use the full date range of the rasters.
#' @param interpol_fun A function used for interpolating the temperatures. This function 
#'                     should accept minimum and maximum temperatures as inputs.
#' @param interpol_args A list of additional arguments to be passed to the interpolation function.
#' @param daily_agg_fun A character string specifying the aggregation function to be used 
#'                       when converting back to daily values. Defaults to "none".
#' @param save_path Optional character string specifying the output directory for storing 
#'                  the interpolated raster. If NULL (default), the raster is only returned 
#'                  without saving to disk. When provided, the output raster is saved as a 
#'                  GeoTIFF split up into batches.
#' @param overwrite_batches Logical indicating whether to overwrite existing batch files.
#'                          If FALSE (default), existing batches will be skipped and processing
#'                          will resume from the next missing batch (smart restart).
#' @param max_output_layers An integer defining the maximum number of output layers allowed; 
#'                          default is set to 30,000 to prevent exceeding raster writing limits.
#'
#' @return A SpatRaster object containing the daily aggregated temperatures calculated from the 
#'         interpolated hourly values based on the provided minimum and maximum temperatures.
#'
#' @examples
#' \dontrun{
#'   # Example 1: Using file paths
#'   result <- interpol_min_max(
#'     min_rast = "path/to/tmin",
#'     max_rast = "path/to/tmax",
#'     geometry = my_regions,
#'     start_date = "2022-01-01",
#'     end_date = "2022-01-07",
#'     interpol_fun = sinusoidal_interpol,
#'     interpol_args = list(),
#'     daily_agg_fun = "mean"
#'   )
#'   
#'   # Example 2: Using SpatRaster objects
#'   tmin_rast <- rast("path/to/tmin.tif")
#'   tmax_rast <- rast("path/to/tmax.tif")
#'   result <- interpol_min_max(
#'     min_rast = tmin_rast,
#'     max_rast = tmax_rast,
#'     geometry = my_regions,
#'     interpol_fun = sinusoidal_interpol
#'   )
#' }
#' 
#' @export
interpol_min_max <- function(min_rast, max_rast, geometry, start_date = NULL, end_date = NULL, interpol_fun, interpol_args = NULL, daily_agg_fun = "none", save_path = NULL, overwrite_batches = FALSE, max_cells = 3e7, max_output_layers = 30000) {
  
  # Check if save_path is provided and create directory if needed
  save_path_provided <- !is.null(save_path) && save_path != ""
  
  if (save_path_provided && !dir.exists(save_path)) {
    dir.create(save_path, recursive = TRUE)
  }
  
  # ---- Step 1: Load tmin and tmax rasters -----
  
  # Check if inputs are SpatRaster objects or paths
  min_is_raster <- inherits(min_rast, "SpatRaster")
  max_is_raster <- inherits(max_rast, "SpatRaster")
  
  if (min_is_raster && max_is_raster) {
    # Both are rasters - use them directly
    tmin_raster <- check_raster_layers(min_rast, "min_rast")
    tmax_raster <- check_raster_layers(max_rast, "max_rast")
    
    # If start_date and end_date are NULL, get from raster layer names
    if (is.null(start_date) || is.null(end_date)) {
      raster_dates <- as.Date(names(tmin_raster))
      if (is.null(start_date)) start_date <- min(raster_dates, na.rm = TRUE)
      if (is.null(end_date)) end_date <- max(raster_dates, na.rm = TRUE)
    } else {
      start_date <- as.Date(start_date)
      end_date <- as.Date(end_date)
    }
    
  } else if (!min_is_raster && !max_is_raster) {
    # Both are paths - read them
    if (is.null(start_date) || is.null(end_date)) {
      stop("start_date and end_date must be provided when min_rast and max_rast are file paths.")
    }
    
    # Convert start_date and end_date to Date objects and get year range
    start_date <- as.Date(start_date)
    end_date <- as.Date(end_date)
    boundary_date_range <- c(year(start_date), year(end_date))
    
    # Read the environmental rasters from the specified path, filtering by date range
    files_tmin <- filter_read_rast(min_rast, year_range = boundary_date_range)
    files_tmax <- filter_read_rast(max_rast, year_range = boundary_date_range)
    
    # Read the environmental rasters into a RasterStack
    tmin_raster <- rast(file.path(min_rast, files_tmin))
    tmax_raster <- rast(file.path(max_rast, files_tmax))
    
    # Check raster layers (handles time dimension fallback)
    tmin_raster <- check_raster_layers(tmin_raster, "min_rast")
    tmax_raster <- check_raster_layers(tmax_raster, "max_rast")
    
  } else {
    stop("min_rast and max_rast must both be either SpatRaster objects or character strings (file paths).")
  }
  
  # Crop raster to spatial data bounding box for efficiency
  # For point geometries, buffer by one cell to ensure cells containing points are included
  # For polygon geometries, use extent directly
  geom_types <- sf::st_geometry_type(geometry)
  is_points <- all(grepl("POINT", geom_types))
  
  if (is_points) {
    crop_extent <- buffer_extent(tmin_raster, geometry, buffer_factor = 1)
  } else {
    crop_extent <- terra::ext(geometry)
  }
  
  terra::window(tmin_raster) <- crop_extent
  terra::window(tmax_raster) <- crop_extent
  
  # Filter rasters by date range
  message('Filtering tmin date range...')
  tmin_raster <- filter_env_rast(tmin_raster, start_date, end_date)
  message('Filtering tmax date range...')
  tmax_raster <- filter_env_rast(tmax_raster, start_date, end_date)

  
  # ---- Step 2: Subset the raster into batches -----
  
  # Determine which interpolation method is being used
  interpol_method_name <- if (identical(interpol_fun, sinusoidal_interpol)) {
    "sinusoidal interpolation"
  } else if (identical(interpol_fun, mean_interpol)) {
    "mean interpolation"
  } else {
    "custom interpolation function"
  }
  
  message("\n Starting interpolation with method: ", interpol_method_name)
  
  # Get raster dimensions
  dims_tmin <- dim(tmin_raster)
  dims_tmax <- dim(tmax_raster)
  
  # Check if the number of layers is the same
  if (dims_tmin[3] != dims_tmax[3]) {
    stop("The number of layers in tmin_raster and tmax_raster must be the same. 
         tmin has ", dims_tmin[3], " layers, but tmax has ", dims_tmax[3], " layers.")
  }
  
  # Get the factor by which the number of output layers is increased based on the interpolation function (e.g. sinuoidal_interpolation creates 24 layers (= hours) per day)
  layer_increase_factor <- ifelse(identical(interpol_fun, sinusoidal_interpol), 24, 1) 
  
  # Get the number of input layers that can be maximally processed at once
  max_layers <- floor(max_output_layers / layer_increase_factor)
  total_cells <- dims_tmin[1] * dims_tmin[2] * max_layers
  
  # if total cells exceed max_cells, adjust max_layers accordingly
  if (total_cells > max_cells) {
    max_layers <- floor(max_cells / (dims_tmin[1] * dims_tmin[2]))
    message("Total cells exceed max_cells. Adjusting max_layers to ", max_layers, ".")
  }
  
  # Get the number of input layers
  num_layers <- dims_tmin[3]
  
  # Determine maximum layers per piece
  num_batches <- ceiling(num_layers / max_layers)
  
  
  # Initialize a list to store results
  output_list <- list()
  
  # Check for existing batches if save_path is provided
  existing_batches <- integer(0)
  if (save_path_provided) {
    interpolated_rasters_path <- file.path(save_path, "interpolated_rasters")
    if (dir.exists(interpolated_rasters_path)) {
      existing_files <- list.files(interpolated_rasters_path, pattern = "^batch_\\d+\\.tif$")
      if (length(existing_files) > 0) {
        existing_batches <- as.integer(gsub("batch_(\\d+)\\.tif", "\\1", existing_files))
        if (!overwrite_batches) {
          message("\nFound ", length(existing_batches), " existing batch files.")
          message("Smart restart enabled: will skip existing batches.")
        }
      }
    }
  }
  
  # Create indices to split layers
  layer_indices <- seq(1, dims_tmin[3], by = max_layers)
  for (j in seq_along(layer_indices)) {
    
    # Check if this batch already exists and skip if not overwriting
    batch_file <- if (save_path_provided) {
      file.path(interpolated_rasters_path, sprintf("batch_%04d.tif", j))
    } else {
      NULL
    }
    
    if (save_path_provided && !overwrite_batches && j %in% existing_batches) {
      message("\n---- Skipping batch ", j, "/", num_batches, " (already exists) ----")
      
      # Load existing batch for return value
      existing_rast <- rast(batch_file)
      output_list[[j]] <- existing_rast
      next
    }
    
    # start timer
    timer_start <- Sys.time()
    
    start_layer <- layer_indices[j]
    end_layer <- min(start_layer + max_layers - 1, dims_tmin[3])
    
    # Only print first and last date for the nested piece with extra indentation.
    batch_dates <- names(tmin_raster[[start_layer:end_layer]])
    if (length(batch_dates) > 0) {
      first_date <- as.Date(batch_dates[1])
      last_date <- as.Date(batch_dates[length(batch_dates)])
      message("\n---- Processing batch ", j, "/", num_batches, " from ", first_date, " to ", last_date, " ---- ")
    } else {
      message("\n---- Processing layers ", start_layer, " to ", end_layer, " ---- ")
    }
    
    # Subset the tmin and tmax layers
    tmin_batch <- tmin_raster[[start_layer:end_layer]]
    tmax_batch <- tmax_raster[[start_layer:end_layer]]
    
    
    # ---- Step 3: Apply the interpolation function to the rasters -----
    
    interpolated_rast <- do.call(terra::xapp,
                             c(list(x = tmin_batch, y = tmax_batch, fun = interpol_fun),
                               interpol_args))
    
    # Ensure that the output remains a SpatRaster
    if (!inherits(interpolated_rast, "SpatRaster")) {
      stop("Error: The output is not a valid SpatRaster object.")
    }
    
    # ---- Step 4: Aggregate the sub-daily raster to daily resolution (if daily_agg_fun is not 'none') -----
    
    output_batch <- agg_to_daily(interpolated_rast, fun = daily_agg_fun)  # or "sum" based on needs
    
    # ---- Step 5: Save the output (after aggregation) -----
    
    if (save_path_provided) {
      # create an 'interpolated_rasters' folder in the output directory if it does not exist
      if (!dir.exists(interpolated_rasters_path)) {
        dir.create(interpolated_rasters_path, recursive = TRUE)
      }
      
      # Save the aggregated output with compression
      terra::writeRaster(output_batch, 
                        filename = batch_file,
                        overwrite = overwrite_batches,
                        gdal = c("COMPRESS=DEFLATE", "PREDICTOR=2", "ZLEVEL=6"))
    }
    
    # stop the timer
    timer_end <- Sys.time()
    total_period_time <- round(difftime(timer_end, timer_start, units = "secs"), 2)
    
    message("     Processing time for batch ", j,
            ": ", total_period_time, " seconds.")
    # Store the result in the list
    output_list[[j]] <- output_batch
    
  }
  output_rast <- rast(output_list)
  return(output_rast)  
  
}
