#' @importFrom terra rast res ext xmin xmax rotate
#' @importFrom sf st_read st_write st_crs `st_crs<-` st_transform st_geometry `st_geometry<-` st_drop_geometry st_coordinates st_geometry_type st_buffer st_is_longlat st_is_empty st_is_valid st_make_valid st_wrap_dateline st_as_sfc st_as_sf st_sfc st_sf st_polygon st_point st_centroid
#' @importFrom arrow read_parquet
#' @importFrom dplyr select mutate relocate
NULL


#' Check and Validate Environmental Raster for Pipeline Compatibility
#'
#' @description
#' This function performs comprehensive validation of environmental rasters to ensure they are
#' suitable for use in the r2e2 pipeline. It checks:
#' - Layer names are in valid date format (via check_raster_layers)
#' - Longitude coordinates are in -180 to 180 degree format (not 0-360)
#' 
#' If issues are found, the function provides clear guidance on how to fix them.
#'
#' @param raster A SpatRaster object to validate
#' @param raster_name A character string describing the raster (for error/info messages)
#' @param lon_tolerance Numeric tolerance for detecting 0-360 degree format (default: 181).
#'   Rasters with xmax > lon_tolerance are considered to be in 0-360 format.
#'
#' @return The validated raster with properly formatted layer names and coordinate system
#'
#' @export
#' @examples
#' \dontrun{
#'   # Check an existing SpatRaster object
#'   checked_raster <- check_raster(my_raster, raster_name = "ERA5 Temperature")
#'   
#'   # If raster is in 0-360 format, you'll get an error with instructions:
#'   # Error: Raster uses 0-360 degree longitude format (xmin=0.5, xmax=359.5)
#'   # The pipeline requires -180 to 180 degree format.
#'   # 
#'   # To fix this, rotate the raster before using it:
#'   #   library(terra)
#'   #   env_rast <- rast("your_file.nc")
#'   #   env_rast <- rotate(env_rast)
#'   #   result <- r2e2(env_rast = env_rast, ...)
#' }
check_raster <- function(raster, raster_name = "raster", lon_tolerance = 181, verbose = 1) {
  
  # Check layer names using existing function
  raster <- check_raster_layers(raster, raster_name, verbose = verbose)
  
  # Check longitude format (must be -180 to 180, not 0 to 360)
  raster_xmin <- terra::xmin(raster)
  raster_xmax <- terra::xmax(raster)
  
  if (raster_xmax > lon_tolerance) {
    stop(
      raster_name, " uses 0-360 degree longitude format (xmin=", round(raster_xmin, 2), 
      ", xmax=", round(raster_xmax, 2), ")\n",
      "r2e2 requires -180 to 180 degree format.\n\n",
      "To fix this, rotate the raster before using it:\n",
      "  library(terra)\n",
      "  env_rast <- rotate(env_rast)\n",
      "  result <- r2e2(env_rast = env_rast, ...)\n\n",
      "Note: Make sure ALL rasters (environmental, secondary weights, tmin/tmax) use the same format."
    )
  }
  
  # Check if longitude range looks suspicious (might indicate issues)
  if (raster_xmin < -180.5 || raster_xmax > 180.5) {
    warning(
      raster_name, " has unusual longitude range (xmin=", round(raster_xmin, 2), 
      ", xmax=", round(raster_xmax, 2), ")\n",
      "Expected range: -180 to 180 degrees. Please verify the coordinate system."
    )
  }
  
  if (verbose >= 2) {
    message(raster_name, " longitude format validated: -180 to 180 degrees")
  }
  
  return(raster)
}


#' Check and Validate Raster Layer Names
#'
#' @description
#' This helper function checks that a raster has valid layer names in date format.
#' If layer names are not valid dates, it attempts to use the time dimension of the raster.
#'
#' @param raster A SpatRaster object to validate
#' @param raster_name A character string describing the raster (for error messages)
#'
#' @return The validated raster with properly formatted layer names
#'
#' @export
#' @examples
#' \dontrun{
#'   checked_raster <- check_raster_layers(my_raster, "temperature_data")
#' }
check_raster_layers <- function(raster, raster_name = "raster", verbose = 1) {
  
  if (!inherits(raster, "SpatRaster")) {
    stop(raster_name, " must be a SpatRaster object")
  }
  
  if (nlyr(raster) == 0) {
    stop(raster_name, " has no layers")
  }
  
  # Get layer names and check whether they are in date format
  layer_names <- names(raster)
  
  if (is.null(layer_names) || all(layer_names == "")) {
    stop(raster_name, " layers have no names. Layer names must be in date format (e.g., 'YYYY-MM-DD')")
  }
  
  # Try to determine temporal resolution
  temp_res <- tryCatch({
    get_temp_res(layer_names)
  }, error = function(e) {
    if (grepl("Multiple temporal resolutions detected", e$message)) {
      # Try using the time dimension of the raster
      time_dim <- time(raster)
      if (!is.null(time_dim) && length(time_dim) > 0) {
        if (verbose >= 2) {
          message("The layer names of ", raster_name, " do not appear to be in a standard time format (e.g., 'YYYY-MM-DD'). ",
                  "Attempting to use time dimension of the raster")
        }
        names(raster) <- as.character(time_dim)
        return(raster)
      } else {
        stop(e$message)
      }
    } else {
      stop(e$message)
    }
  })
  
  if (verbose >= 2) {
    message(raster_name, " validated successfully: ", nlyr(raster), " layers")
  }
  return(raster)
}


#' Get Valid Raster Files
#'
#' @description
#'   This function searches for raster files with specific extensions (.tif, .nc) in a given folder,
#'   checking whether they can be read into R. It returns a list of accessible raster files 
#'   while printing warnings for any files that cannot be read.
#'
#' @param path A character string specifying the path to the folder containing raster files.
#' @param pattern A character string specifying the file pattern to match (default is "\\.tif$|\\.nc$" 
#'   for GeoTIFF and NetCDF files).
#' @param verbose Integer verbosity level (0 = silent, 1 = concise, 2 = detailed). Default is 1.
#'
#' @return A character vector containing paths to the valid raster files that can be read.
#'
#' @examples
#' \dontrun{
#'   # Get all valid .tif and .nc files
#'   valid_rasters <- get_valid_raster_files("path/to/rasters/")
#'   
#'   # Get only NetCDF files
#'   nc_files <- get_valid_raster_files("path/to/rasters/", pattern = "\\.nc$")
#' }
get_valid_raster_files <- function(path, pattern = "\\.tif$|\\.nc$", verbose = 1) {
  # Identify all raster files in the folder with full paths
  rast_files <- list.files(
    path = path,
    pattern = pattern,
    full.names = FALSE,
    recursive = FALSE
  )
  
  # Initialize a vector to hold paths of readable files
  available_files <- character(0)
  
  # Check each file to confirm it can be read
  for (file in rast_files) {
    file_path <- file.path(path, file)  # Construct full file path
    if (file.exists(file_path)) {
      # Attempt to read the file and catch any errors
      try({
        # Attempt to create a SpatRaster object
        test_raster <- terra::rast(file_path)
        available_files <- c(available_files, file) # Add to available list if successful
      }, silent = TRUE)  # suppress error messages
      
    } else {
      warning("File does not exist or is not accessible: ", file)  # Print warning if file does not exist
    }
  }
  
  unavailable_files <- setdiff(rast_files, available_files)
  if (length(unavailable_files) > 0 && verbose >= 2) {
    message("The following files could not be read. If the raster is located in Dropbox, make sure it is available offline before executing the script. Unavailable files: \n", paste(unavailable_files, collapse = ", "))
  }
  
  return(available_files)
}



#' Filter and Read Rasters
#'
#' @description
#' This function loads files from a specified directory. It allows users to filter the files based on a year range, with the years inferred from the filenames, which are expected to be in the format 'yyyy.tif' or 'yyyy.nc'. If no filtering range is provided, all available raster files will be read. The function prints informative messages detailing the loading process, including the path and names of the files being read.
#'
#' @param path A character string specifying the directory path containing raster files.
#' @param year_range A numeric vector of length 2 (optional). This vector specifies the year range to filter the raster files. If NULL (default), all files are loaded without filtering.
#' @param verbose Integer verbosity level (0 = silent, 1 = concise, 2 = detailed)
#'
#' @return A character vector of file paths corresponding to the raster files that were read. The files are loaded into memory as a list of raster objects.
#'
#' @examples
#' \dontrun{
#'   # Load all secondary weight rasters
#'   all_rasters <- filter_read_sec_weights("path/to/rasters/")
#'   
#'   # Load secondary weight rasters filtered by year range
#'   filtered_rasters <- filter_read_sec_weights("path/to/rasters/", c(1970, 2010))
#' }
filter_read_rast <- function(path, year_range = NULL, verbose = 1) {
  
  # Identify all available rasters in the folder
  rast_files <- get_valid_raster_files(path, pattern = "\\.tif$|\\.nc$", verbose = verbose) 
  
  # Print the path from which files are being loaded (only if verbose >= 2)
  if (verbose >= 2) {
    message(sprintf("Loading files from path: %s", path))
  }

  if (is.null(year_range)) {
    # No specific range provided, read all files
    filtered_files <- rast_files
    if (verbose >= 2) {
      message("No filtering applied. Reading in all available files.")
    }
  } else {
    
    # Ensure dates in year_range are correctly formatted
    
    
    # Extract years from the file names
    years <- as.numeric(gsub("\\.tif$|\\.nc$", "", basename(rast_files)))

    # Filter files based on the specified year range
    filtered_files <- rast_files[years >= year_range[1] & years <= year_range[2]]
    if (verbose >= 2) {
      message(sprintf("Filtering files based on the date range provided: %d to %d", year_range[1], year_range[2]))
    }
  }
  
  # Print the names of the files being read (only if verbose >= 2)
  if (verbose >= 2) {
    message("Files being read:")
    # print filenames separated by commas
    message(paste(basename(filtered_files), collapse = ", "))
  }
  
  return(filtered_files)
}


#' Read Spatial File (Polygons or Points)
#'
#' @description
#' Reads spatial data from a file (Parquet, GPKG, Shapefile, GeoJSON, FGB, RDS), converts the geometry 
#' to an sf object. By default, validates and cleans the geometry using `check_spatial_file()`, which
#' validates that a CRS is defined, renames "geom" to "geometry", removes empty geometries, applies
#' st_make_valid() to polygons, and performs other validation steps. Validation can be disabled by
#' setting `validate = FALSE`.
#'
#' @param file_path A string specifying the path to the spatial data file.
#' @param validate Logical; if TRUE (default), validates and cleans the geometry using `check_spatial_file()`.
#'
#' @return An sf object with geometry in its original CRS, validated and cleaned.
#'   Point and polygon geometries are both supported and processed accordingly.
#'
#' @export
#' @examples
#' \dontrun{
#'   # Read and validate (default)
#'   polygons <- read_spatial_file("data/polygons/CONUS_consistent_counties.gpkg")
#'   
#'   # Read without validation
#'   polygons_raw <- read_spatial_file("data/polygons/polygons.gpkg", validate = FALSE)
#'   
#'   points <- read_spatial_file("data/points/sampling_points.gpkg")
#'   rds_data <- read_spatial_file("data/spatial_data.rds")
#' }
read_spatial_file <- function(file_path, validate = TRUE) {
  if (!file.exists(file_path)) {
    stop("Error: File does not exist - ", file_path)
  }
  
  # Extract file extension (lowercase)
  file_ext <- tolower(tools::file_ext(file_path))
  
  # Read based on format
  if (file_ext %in% c("parquet", "pq")) {
    message("Reading Parquet file: ", file_path)
    geometry_df <- arrow::read_parquet(file_path)
    
    if (!("geometry" %in% names(geometry_df)) && !("geom" %in% names(geometry_df))) {
      stop("Error: No geometry column found in Parquet file")
    }
    
    if ("geometry" %in% names(geometry_df)) {
      geometry_df$geometry <- st_as_sfc(geometry_df$geometry)
    } else {
      geometry_df$geom <- st_as_sfc(geometry_df$geom)
    }
    
    geometry <- st_as_sf(geometry_df, crs = "WGS84")
    
  } else if (file_ext == "rds") {
    message("Reading RDS file: ", file_path)
    geometry <- readRDS(file_path)
    
    # Check if the loaded object is an sf object
    if (!inherits(geometry, "sf")) {
      stop("Error: RDS file does not contain an sf object")
    }
    
  } else if (file_ext %in% c("gpkg", "shp", "geojson", "json", "fgb")) {
    message("Reading spatial file: ", file_path)
    geometry <- st_read(file_path, quiet = TRUE)
    
  } else {
    stop("Error: Unsupported file format - ", file_ext)
  }

  # Optionally validate and clean using check_spatial_file()
  if (validate) {
    geometry <- check_spatial_file(geometry, source = file_path)
  }

  return(geometry)
}


#' Check and Clean an sf Spatial Object
#'
#' @description
#' This function performs validation and cleaning on an sf object. It sets or transforms the CRS
#' to WGS84 if needed, renames geometry columns, removes empty geometries, reports mixed geometry
#' types, applies `st_make_valid()` to polygons, wraps dateline crossings, and issues final validity
#' checks. Designed to be used both after reading files and when the user supplies an sf object
#' directly.
#'
#' @param geometry An sf object to validate and clean.
#' @param source Optional character string describing the source (e.g., file path) for messaging.
#'
#' @return A cleaned and validated sf object with geometry in its original CRS.
#'
#' @export
#' @examples
#' \dontrun{
#'   # Validate and clean an sf object
#'   cleaned_geometry <- check_spatial_file(my_geometry)
#'   
#'   # With source information for better error messages
#'   cleaned_geometry <- check_spatial_file(my_geometry, source = "my_data.gpkg")
#' }
check_spatial_file <- function(geometry, source = NULL, verbose = 1) {
  # Provide a short source label for messages
  src <- if (!is.null(source)) paste0(source, ": ") else ""

  # Check that CRS is defined
  geometry_crs <- st_crs(geometry)
  
  if (is.na(geometry_crs)) {
    stop(src, "CRS is missing. Please define a coordinate reference system for your geometry.")
  }
  
  if (verbose >= 2) {
    crs_name <- if (!is.na(geometry_crs$epsg)) {
      paste0("EPSG:", geometry_crs$epsg)
    } else {
      "Custom CRS"
    }
    message(src, "Geometry CRS: ", crs_name)
  }

  # Rename geometry column 'geom' to 'geometry' if needed.
  if ("geom" %in% names(geometry)) {
    if (verbose >= 2) {
      message(src, "Renaming geometry column 'geom' to 'geometry'.")
    }
    names(geometry)[names(geometry) == "geom"] <- "geometry"
    st_geometry(geometry) <- "geometry"
  }

  # Remove empty geometries
  empty_idx <- which(st_is_empty(geometry))
  if (length(empty_idx) > 0) {
    warning(src, length(empty_idx), "/", nrow(geometry), " rows have empty geometries and will be removed: ", paste(empty_idx, collapse = ", "))
    geometry <- geometry[-empty_idx, ]
  }

  # Geometry type checks
  geom_types <- st_geometry_type(geometry)
  unique_geom_types <- unique(geom_types)
  has_points <- any(grepl("POINT", unique_geom_types))
  has_polygons <- any(grepl("POLYGON", unique_geom_types))

  if (has_points && has_polygons) {
    warning(src, "Mixed geometry types detected (points and polygons). This may affect downstream analysis.")
    if (verbose >= 2) {
      message(src, "Geometry types: ", paste(unique_geom_types, collapse = ", "))
    }
  }

  if (has_points) {
    n_points <- sum(grepl("POINT", geom_types))
    if (verbose >= 2) {
      message(src, n_points, " point geometries detected (optimized point extraction will be used).")
    }
  }

  # Handle polygon geometries (apply st_make_valid)
  if (has_polygons) {
    polygon_indices <- grepl("POLYGON", geom_types)
    n_polygons <- sum(polygon_indices)

    invalid_polygon_idx <- polygon_indices & !st_is_valid(geometry)
    n_invalid_polygons <- sum(invalid_polygon_idx)
    if (n_invalid_polygons > 0) {
      warning(src, n_invalid_polygons, "/", n_polygons, " polygon rows have invalid geometries and will be fixed.")
    }

    if (verbose >= 2) {
      message(src, "Applying st_make_valid() to ", n_polygons, " polygon geometries.")
    }
    temp_polygons <- geometry
    temp_polygons[polygon_indices, ] <- st_make_valid(temp_polygons[polygon_indices, ])

    post_valid_empty_idx <- which(polygon_indices & st_is_empty(temp_polygons))
    if (length(post_valid_empty_idx) > 0) {
      warning(src, length(post_valid_empty_idx), "/", n_polygons, " polygon rows became empty after st_make_valid() and will be removed: ", paste(post_valid_empty_idx, collapse = ", "))
      temp_polygons <- temp_polygons[-post_valid_empty_idx, ]
    }

    geometry <- temp_polygons
    geometry <- st_wrap_dateline(geometry, options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180"))
  }

  # Final checks
  final_empty_idx <- which(st_is_empty(geometry))
  if (length(final_empty_idx) > 0) {
    warning(src, length(final_empty_idx), "/", nrow(geometry), " rows have empty geometries and will be removed: ", paste(final_empty_idx, collapse = ", "))
    geometry <- geometry[-final_empty_idx, ]
  }

  final_invalid_idx <- which(!st_is_valid(geometry))
  if (length(final_invalid_idx) > 0) {
    warning(src, length(final_invalid_idx), "/", nrow(geometry), " rows still have invalid geometries: ", paste(final_invalid_idx, collapse = ", "))
  }

  if (verbose >= 2) {
    message(src, "Final result: ", nrow(geometry), " valid geometries loaded.")
  }

  return(geometry)
}

#' Buffer Spatial Extent Based on Raster Resolution
#'
#' @description
#'   This function computes a buffered extent around the input spatial data (points or polygons) 
#'   using the raster's resolution. The extent is expanded by a factor specified by the user 
#'   (buffer_factor). This is useful for ensuring that point locations fall within raster cells
#'   when using terra::window(), or for adding a buffer around polygon boundaries.
#'
#' @param raster A SpatRaster object from which the resolution is extracted.
#' @param spatial_data An sf or SpatVector object containing points or polygons.
#' @param buffer_factor Numeric factor by which to multiply the raster resolution when buffering
#'   the spatial extent. Default is 1. Use 0 for no buffer.
#'
#' @return A buffered extent (terra::ext object) computed as:
#'   xmin = spatial_extent$xmin - buffer_factor * x_res,
#'   xmax = spatial_extent$xmax + buffer_factor * x_res,
#'   ymin = spatial_extent$ymin - buffer_factor * y_res,
#'   ymax = spatial_extent$ymax + buffer_factor * y_res.
#'
#' @examples
#' \dontrun{
#'   # Buffer extent by one cell resolution (useful for points)
#'   buffered_extent <- buffer_extent(environmental_raster, points, buffer_factor = 1)
#'   
#'   # No buffer (useful for polygons)
#'   unbuffered_extent <- buffer_extent(environmental_raster, polygons, buffer_factor = 0)
#' }
buffer_extent <- function(raster, spatial_data, buffer_factor = 1) { 
  # Extract raster resolution
  x_res <- res(raster)[1]
  y_res <- res(raster)[2]
  
  # Get spatial data bounding box as a terra::ext object
  spatial_extent <- ext(spatial_data)
  
  # Create a buffered extent using the buffer_factor
  buffered_extent <- ext(c(
    xmin = spatial_extent$xmin - buffer_factor * x_res,
    xmax = spatial_extent$xmax + buffer_factor * x_res,
    ymin = spatial_extent$ymin - buffer_factor * y_res,
    ymax = spatial_extent$ymax + buffer_factor * y_res
  ))
  
  return(buffered_extent)
}

#' Filter Raster by Date Range
#'
#' @description
#'   This function filters a multi-layer raster based on a specified date range. The raster should have
#'   layer names corresponding to date strings in 'YYYY-MM-DD' format. The function checks whether the
#'   specified date range falls within the range of available raster dates and returns the filtered
#'   raster containing only the layers that match the specified dates.
#'
#' @return:
#'   - A filtered raster object containing only the layers corresponding to the specified date range.
#' Filter Environmental Raster by Date Range
#'
#' @description
#'   This function filters a raster to only include layers within a specified date range.
#'   It supports multiple temporal resolutions: yearly (YYYY), monthly (YYYY-MM), 
#'   daily (YYYY-MM-DD), and hourly (YYYY-MM-DD HH:MM).
#'
#' @param env_rast A SpatRaster object with named layers representing dates
#' @param start_date Character string specifying the start date
#' @param end_date Character string specifying the end date
#' @param verbose Integer controlling message verbosity (default: 1)
#'
#' @return A filtered SpatRaster object containing only layers within the date range
#'
#' @examples
#'   # Example: Filter a raster to a specific date range
#'   # filtered_raster <- filter_env_rast(my_raster, start_date = "2018-01-01", end_date = "2018-12-31")
filter_env_rast <- function(env_rast, start_date, end_date, verbose = 1) {
  # Check if raster layer names exist
  layer_names <- names(env_rast)
  if (is.null(layer_names)) {
    stop("Error: Raster layers have no names. Ensure layers are named with valid date strings (e.g., 'YYYY-MM-DD').")
  }
  
  # Determine temporal resolution
  temp_res <- get_temp_res(layer_names)
  
  # Convert layer names to Date format based on resolution
  # For start dates, always use first day of period
  # For end dates, use last day of period
  raster_dates_start <- tryCatch({
    if (temp_res == "yearly") {
      # Convert YYYY to YYYY-01-01
      as.Date(paste0(layer_names, "-01-01"))
    } else if (temp_res == "monthly") {
      # Convert YYYY-MM to YYYY-MM-01
      as.Date(paste0(layer_names, "-01"))
    } else {
      # Daily or hourly - extract date part
      as.Date(substr(layer_names, 1, 10))
    }
  }, error = function(e) {
    stop("Error: Raster layer names could not be converted to valid dates. ",
         "Ensure they are in a recognizable date format such as 'YYYY', 'YYYY-MM', or 'YYYY-MM-DD'.")
  })
  
  # For end dates, calculate last day of period
  raster_dates_end <- tryCatch({
    if (temp_res == "yearly") {
      # Convert YYYY to YYYY-12-31
      as.Date(paste0(layer_names, "-12-31"))
    } else if (temp_res == "monthly") {
      # Convert YYYY-MM to last day of month
      start_dates <- as.Date(paste0(layer_names, "-01"))
      # Get last day of month by going to next month and subtracting 1 day
      seq(start_dates[1], by = "month", length.out = 2)[2] - 1
      # For multiple months, calculate each
      sapply(start_dates, function(d) {
        seq(d, by = "month", length.out = 2)[2] - 1
      })
    } else {
      # Daily or hourly - same as start
      as.Date(substr(layer_names, 1, 10))
    }
  }, error = function(e) {
    stop("Error: Could not calculate end dates for raster periods.")
  })
  
  # Ensure that at least some raster layer names were successfully converted
  if (all(is.na(raster_dates_start))) {
    stop("Error: All raster layer names are NA after attempting date conversion. ",
         "Please check the naming format.")
  }
  
  # Get the overall raster date range (use start of first period and end of last period)
  raster_start <- min(raster_dates_start, na.rm = TRUE)
  raster_end <- max(raster_dates_end, na.rm = TRUE)
  
  # Convert input dates to Date format
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  
  # Check if period definitions exceed raster date range
  if (start_date < raster_start || end_date > raster_end) {
    stop("Error: The specified date range exceeds the raster date range.\n",
         "   Raster covers: ", raster_start, " to ", raster_end, "\n",
         "   Periods cover: ", start_date, " to ", end_date, "\n",
         "   Ensure that period definitions are within the available raster time range.")
  }
  
  if (start_date > raster_start || end_date < raster_end) {
    if (verbose >= 2) {
      message("Selected period date range is shorter than raster date range:\n",
              "   Raster covers: ", raster_start, " to ", raster_end, "\n",
              "   Periods cover: ", start_date, " to ", end_date, "\n",
              "Filtering to period date range...")
    }
  }
  
  
  # Filter the raster based on the specified date range
  # A layer is included if its period overlaps with the requested range
  date_mask <- raster_dates_start <= end_date & raster_dates_end >= start_date
  filtered_raster <- env_rast[[date_mask]]
  if (verbose >= 2) {
    message("Filtered Raster date range: ", start_date, " to ", end_date, "\n")
  }
  
  
  return(filtered_raster)
}


#' Parse Boundary Dates from Raster Layer Names
#'
#' @description
#'   Determines temporal resolution and converts first/last layer names to date boundaries.
#'   For hourly/daily: returns dates as-is
#'   For monthly: returns first day of first month and last day of last month
#'   For yearly: returns Jan 1st of first year and Dec 31st of last year
#'
#' @param:
#'   - env_rast: Terra raster with date-formatted layer names
#'
#' @return:
#'   - Vector of two Date objects representing start and end boundaries
#'
parse_boundary_dates <- function(env_rast) {
  
  # Get layer names and determine temporal resolution with fallback
  layer_names <- names(env_rast)  
  temp_res <- get_temp_res(layer_names)
  
  first_name <- layer_names[1]
  last_name <- layer_names[length(layer_names)]
  
  # Convert based on resolution
  if (temp_res == "hourly" || temp_res == "daily") {
    # Extract just the date part (YYYY-MM-DD) from YYYY-MM-DD HH:MM
    first_date <- as.Date(first_name)
    last_date <- as.Date(last_name)
    
  } else if (temp_res == "monthly") {
    # Convert YYYY-MM to first day of first month and last day of last month
    first_date <- as.Date(paste0(first_name, "-01"))
    
    last_month_first <- as.Date(paste0(last_name, "-01"))
    days_in_last_month <- lubridate::days_in_month(last_month_first)
    last_date <- as.Date(paste0(last_name, "-", sprintf("%02d", days_in_last_month)))
    
  } else if (temp_res == "yearly") {
    # Convert YYYY to Jan 1st and Dec 31st
    first_date <- as.Date(paste0(first_name, "-01-01"))
    last_date <- as.Date(paste0(last_name, "-12-31"))
    
  } else {
    stop("Unsupported temporal resolution: ", temp_res)
  }
  
  boundary_dates <- c(first_date, last_date)

  return(boundary_dates)
}
