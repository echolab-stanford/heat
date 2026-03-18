#' @importFrom stringr str_replace_all
#' @importFrom dplyr filter mutate select any_of
NULL

#' Given a baseline date, return a sequence of dates with a specified alignment, width and lags. 
#'
#' @description
#' This function takes a baseline date an returns and sequence of historic dates. 
#'  
#' @param d A character with dates in format YYYY-MM-DD
#' @param start An integer indicating the offset with respect to the polygon date. 
#' @param window An integer indicating the size of the date sequences. 
#' @param align A character indicating whether the window sequence is left- or right-aligned with respect to the polygon date. 
#' @param hist_lags An integer indicating the number of historical years in the dynamic baseline.  
#' @param window_spec A character vector indicating whether the historic window is dynamic, fixed or both. 
#' @param start_date A date in the format YYYY-MM-DD indicating the start date of the fixed window. Only required if window_spec = "fixed" or window_spec = "both".
#' @param stop_date A date in the format YYYY-MM-DD indicating the end date of the fixed window. Only required if window_spec = "fixed" or window_spec = "both".
#' @param only_hist A boolean indicating whether to return only the historical sequences (if TRUE) or not (if FALSE). Default is FALSE.
#'
#' @return A vector of dates
window_seq <- function(d, start, window, align, hist_lags, window_spec, start_date, stop_date, only_hist = FALSE){ 
  
  if (!align %in% c("left", "right", "center")) stop("Align  must be left, center or right.")
  #if (time_step %in% c("daily")) {time_step <- "day"}
  #if (time_step %in% c("weekly")) {time_step <- "week"}
  #if (time_step %in% c("monthly")) {time_step <- "month"}
  #if (time_step %in% c("yearly")) {time_step <- "year"}
  
  d <- sub('-02-29', '-02-28', d) # treat feb 29 (leap days) as feb 28
  d <- as.Date(d)
  
  if (align == "right"){
    start_d <- as.Date(
      ifelse(
        start > 0, 
        seq(d, by = "-1 day", length.out = ifelse(start == 0, 1, start))[start], 
        d
      )
    )
    d_s <- seq(start_d, by = "-1 day", length.out = window)
  }
  
  if (align == "left"){
    start_d <- as.Date(
      ifelse(
        start > 0, 
        seq(d, by = "1 day", length.out = ifelse(start == 0, 1, start))[start], 
        d
      )
    )
    d_s <- seq(start_d, by = "1 day", length.out = window)
  }
  
  if (align == "center"){
    start_d <- seq(d, by = "1 day", length.out = ifelse(start == 0, 1, start))[start] # Assumes centering is for dates before the interview
    d_s_left <- seq(d, by = "1 day", length.out = ceiling(window/2))
    d_s_right <- seq(d, by = "1 day", length.out = ceiling(window/2))
    d_s <- sort(unique(c(d_s_left, d_s_right))) # Sort to keep interview date in center
  }
  
  if (hist_lags == 0){return(sapply(d_s, \(x) as.character(as.Date(x))))}
  
  # Final vector of dates
  if (window_spec == "dynamic"){
    hist_lags_v <- sapply(d_s, relative_baseline, hist_lags = hist_lags, width = 0, align = "center")
    if (!is.matrix(hist_lags_v)){
      hist_lags_f <- as.vector(hist_lags_v)
    } else {
      t_m <- t(hist_lags_v)
      hist_lags_f <- as.vector(t_m)
    }
    
    dates_f <- c(
      hist_lags_f,                                                                     # hist_lags of time window | Use as.vector to flatten matrix
      sapply(d_s, \(x) as.character(as.Date(x)))                                       # Window in current year
    )
    
  } else if (window_spec == "fixed"){
    
    if (is.null(start_date) | is.null(stop_date)){stop("start_date and stop_date must be specified if window_spec is fixed.")}
    
    # Define first date of the polygon time window within the fixed window
    y_o <- paste0(
      c(
        as.character(format(as.Date(start_date), "%Y")), 
        as.character(format(as.Date(min(d_s)), "%m")), 
        as.character(format(as.Date(min(d_s)), "%d"))
      ), 
      collapse = "-"
    )
    
    # Make sure that the first date falls within the fixed window
    if (as.Date(y_o) < as.Date(start_date)){y_o <- seq(from = as.Date(y_o), by = "1 year", length.out = 2)[2]}
    
    # Recreate the polygon window
    f_w_seq <- seq(from = as.Date(y_o), by = "1 day", length.out = length(d_s))
    if(align == "right"){f_w_seq <- sort(f_w_seq, decreasing =  TRUE)} # If align = right, reorder to match the order of the original sequence
    f_w_seq <- sapply(f_w_seq, \(x) as.character(as.Date(x)))
    
    # Estimate number of years within the fixed window
    dif_years <- as.numeric(format(as.Date(stop_date), "%Y")) - as.numeric(format(as.Date(start_date), "%Y"))
    
    # Recreate the polygon window within the fixed window 
    y <- 1
    hist_lags_f <- f_w_seq
    while (y <= dif_years){
      
      new_seq <- sapply(f_w_seq, function(x) seq(from = as.Date(x), by = "1 year", length.out = 2)[2])
      new_seq <- sapply(new_seq, function(x) as.character(as.Date(x)))
      names(new_seq) <- NULL
      # Stop if the date range falls outside of the fixed window
      if (max(as.Date(new_seq)) <= as.Date(stop_date)){
        hist_lags_f <- c(hist_lags_f, new_seq)
        f_w_seq <- new_seq
      } else {
        break
      }
      y <- y + 1
    }
    
    # Check that historic dates are contained in the parquet file
    #check_dates(hist_lags_f, d, p_date_pairs)
    
    # Create final vector of dates
    dates_f <- c(
      hist_lags_f, 
      sapply(d_s, \(x) as.character(as.Date(x)))
    )
  }
  if (only_hist){return(hist_lags_f)}
  return(dates_f)
}

#' 
#' Create bin structure
#'
#' @description
#' This function creates a list of boolean vectors that indicate the position of the dates that fall into the specified bins.
#'
#' @param window Integer specifying the size of the sequence of dates.
#' @param time_step_size Integer indicating the size of each bin. Within the window. If window %% time_step_size == 0, then n bins of size time_step_size and 1 bin of size y (the remainder) are returned such that window = n * time_step_size + y. Otherwise, n bins of size time_step_size are returned.
#' @param align Character indicating the alignment of the window with respect to the interview date. 
#' @param disjoint whether the bins are disjoint or not. Default is to return overlapping bins. 
#' 
#' @return A named list with boolean vectors representing the bin scheme.
#'   
#' @examples:
#' \dontrun{
#'   create_bin_scheme(10, 5, "right", disjoint = FALSE)
#' }
create_bin_scheme <- function(window, time_step_size, align, disjoint = FALSE){
  
  # Input validation
  if (!align %in% c("left", "right", "center")) {
    stop("align must be 'left', 'right', or 'center'")
  }
  if (window <= 0 || time_step_size <= 0) {
    stop("window and time_step_size must be positive integers")
  }
  if (time_step_size > window) {
    stop("time_step_size cannot be larger than window")
  }
  
  # Estimate number of bins
  if (window %% time_step_size == 0){
    r <- 0
    n_bins <- window/time_step_size
    equal <- TRUE
  } else {
    r <- window %% time_step_size
    n_bins <- (window - r)/time_step_size
    equal <- FALSE
  }
  
  if (align == "center"){
    # Skip center alignment as per user request
    stop("Center alignment not implemented")
  }
  
  if (disjoint){
    bins <- lapply(
      1:n_bins, 
      function(i){
        bool_vec <- rep(FALSE, window)
        if(i == 1){
          bool_vec[i:time_step_size] <- TRUE
        } else {
          bool_vec[(time_step_size * (i - 1) + 1):(time_step_size * i)] <- TRUE
        } 
        return(bool_vec)
      }
    )
    
    if(!equal && r > 0){
      bool_vec <- rep(FALSE, window)
      bool_vec[(window - r + 1):window] <- TRUE
      bins[[length(bins) + 1]] <- bool_vec
    }
    
    bin_names <- sapply(1:length(bins), function(i){
      if(i <= n_bins){
        if(i == 1){
          b_name <- paste0(1, "_", time_step_size)
        } else {
          b_name <- paste0(time_step_size * (i - 1) + 1, "_", time_step_size * i) 
        }
      } else {
        # Remainder bin
        b_name <- paste0(window - r + 1, "_", window)
      }
      return(b_name)
    })
    
  } else {
    
    bins <- lapply(
      1:n_bins, 
      function(i){
        bool_vec <- rep(FALSE, window)
        bool_vec[1:(i * time_step_size)] <- TRUE
        return(bool_vec)
      }
    )
    
    
    if (!equal && r > 0){
      bool_vec <- rep(FALSE, window)
      bool_vec[1:window] <- TRUE
      bins[[length(bins) + 1]] <- bool_vec
    }
    
    bin_names <- sapply(1:length(bins), function(i){
      if(i <= n_bins){
        paste0("1_", i * time_step_size)
      } else {
        paste0("1_", window)
      }
    })
  }
  
  names(bins) <- bin_names
  return(bins)
  
}


#' Return a dataframe with a column where a unique ID in the form polygonid_date is included
#'
#' @param pol_date_pairs dataframe with unique polygon date pairs
#' @param geom_id_col character string with the name of the column that identifies geometries
#' @param date_id_col character string with the name of the column that identifies dates
#'
#' @return a dataframe equal to the original plus the polygonid_date column
#'
create_pairs <- function(pol_date_pairs, geom_id_col = "geom_id", date_id_col = "date"){
  
  # Input validation
  if (!geom_id_col %in% names(pol_date_pairs)) {
    stop(paste("Column", geom_id_col, "not found in pol_date_pairs"))
  }
  
  if (!date_id_col %in% names(pol_date_pairs)) {
    stop(paste("Column", date_id_col, "not found in pol_date_pairs"))
  }

  # Validate dates
  tryCatch({
    as.Date(pol_date_pairs[[date_id_col]])
  }, error = function(e) {
    stop("Invalid date format in pol_date_pairs$date. Use date format (e.g., YYYY-MM-DD).")
  })
  
  # Create the composite ID
  pol_date_pairs$pol_date_id <- 
    paste(
      pol_date_pairs[[geom_id_col]],
      stringr::str_replace_all(pol_date_pairs[[date_id_col]], "-", "_"),
      sep = "_"
    )
  
  # Optional: Remove original columns only if they're not the standard names
  if (geom_id_col != "geom_id") {
    # Create standardized column names
    pol_date_pairs$geom_id <- pol_date_pairs[[geom_id_col]]
    
    # Remove the original columns
    pol_date_pairs <- pol_date_pairs |> 
      dplyr::select(!dplyr::any_of(c(geom_id_col)))
  }

  if (date_id_col != "date") {
    # Create standardized column names
    pol_date_pairs$date <- pol_date_pairs[[date_id_col]]
    
    # Remove the original columns
    pol_date_pairs <- pol_date_pairs |> 
      dplyr::select(!dplyr::any_of(c(date_id_col)))
  }

  return(pol_date_pairs)
}


#' Checks if all required packages are installed.
#' 
#' @description
#' This functions checks if all packages required by the shock functions are installed and installs them if not. 
#' 
#' @return
#' - NULL
#' 
check_packages <- function() {
  required_packages <- c("stringr", "purrr", "rlang", "dplyr", "data.table", "future", "future.callr", "callr" , "fst", "arrow")
  missing_packages <- setdiff(required_packages, installed.packages()[,1])
  
  if (length(missing_packages) > 0) {
    stop(paste("Missing required packages:", paste(missing_packages, collapse = ", ")))
  }
  cat("All required packages are available!\n")
}

#' Creates groups of polygons to ensure they are all sized max_group_size or smaller
#' 
#' @description
#' This function splits groups of polygons grouped by date proximity into smaller groups to ensure that the max number of polygons per group is respected.
#' 
#' @param grouped_pol_pairs A data.frame with polygons grouped by date proximity. 
#' @param max_polygons_per_group maximum number of polygons allowed per group. 
#' 
#' @return A data.frame with new groupings assigned. 
split_large_groups <- function(grouped_pol_pairs, max_polygons_per_group) {
  
  # Return original grouping if max_polygons_per_group is -1 (this means no limits)
  if (max_polygons_per_group == -1){return(grouped_pol_pairs)}
  # Identify groups that exceed the limit
  group_sizes <- table(grouped_pol_pairs$date_group)
  large_groups <- which(group_sizes > max_polygons_per_group)
  
  if (length(large_groups) == 0) {
    return(grouped_pol_pairs)  # No splitting needed
  }
  
  # Split large groups
  result_df <- grouped_pol_pairs
  next_group_id <- max(grouped_pol_pairs$date_group) + 1
  
  for (group_id in large_groups) {
    group_data <- grouped_pol_pairs[grouped_pol_pairs$date_group == group_id, ]
    n_polygons <- nrow(group_data)
    n_subgroups <- ceiling(n_polygons / max_polygons_per_group)
    
    # Create subgroup assignments
    subgroup_assignments <- rep(1:n_subgroups, each = max_polygons_per_group)[1:n_polygons]
    
    # Update group IDs
    for (i in 1:n_subgroups) {
      mask <- result_df$date_group == group_id & 
        seq_len(nrow(result_df)) %in% which(grouped_pol_pairs$date_group == group_id)[subgroup_assignments == i]
      
      if (i == 1) {
        # Keep original group_id for first subgroup
        next
      } else {
        # Assign new group_id for additional subgroups
        result_df$date_group[mask] <- next_group_id
        next_group_id <- next_group_id + 1
      }
    }
  }
  
  return(result_df)
}

#' Function to validate inputs
#' @description
#' This functions validates that the inputs passed to the shocks_wrapper function are correct.
#' 
#' @param pol_date_pairs A data.frame with polygons and dates
#' @param conditions_list A named list with conditions to query the parquet file.  
#' @param window An integer indicating wht length of the date sequence
#' @param start An integer indicating the offset with respect to the polygon date. 
#' @param hist_lags An integer indicating the number of years used to build the dynamic historical window. 
#' @param align A character string indicating whether the sequence is left-, or right-aligned with respect to the polygon date. 
#' @param time_step_size An integer indicating the size of the bins into which the date sequence is divided. 
#' @param prop_cores A float number, between 0 and 1, indicating the proportion od available cores to use. 
#' @param int_threshold A float number, between 0 and 1, indicating the minimum intersection threshold with the parquet dates needed for a historic sequence to be valid. 
#' 
#' @return TRUE if all inputs are valid; otherwise raises an error.   
validate_inputs <- function(pol_date_pairs, conditions_list, window, start, hist_lags, align, time_step_size, prop_cores, int_threshold) {
  
 

  # Validate conditions
  if (!is.list(conditions_list)) {
    stop("conditions must be a list")
  }
  
  if (!"data_path" %in% names(conditions)) {
    stop("conditions must contain 'data_path' element")
  }
  
  if (!file.exists(conditions_list$data_path)) {
    stop(paste("Data path does not exist:", conditions_list$data_path))
  }

  if (!"geom_id" %in% names(conditions_list)){stop("The conditions list argument to identify geometries must be called geom_id.")}
  
  # Validate numeric parameters
  if (!is.numeric(window) || window <= 0 || window != floor(window)) {
    stop("window must be a positive integer")
  }
  
  if (!is.numeric(start) || start < 0 || start != floor(start)) {
    stop("start must be a non-negative integer")
  }
  
  if (!is.numeric(hist_lags) || hist_lags < 0 || hist_lags != floor(hist_lags)) {
    stop("hist_lags must be a non-negative integer")
  }
  
  if (!is.numeric(time_step_size) || time_step_size <= 0 || time_step_size != floor(time_step_size)) {
    stop("time_step_size must be a positive integer")
  }
  
  if (time_step_size > window) {
    stop("time_step_size cannot be larger than window")
  }
  
  if (!is.numeric(prop_cores) || prop_cores <= 0 || prop_cores > 1) {
    stop("prop_cores must be between 0 and 1")
  }
  
  if (!is.numeric(int_threshold) || int_threshold < 0 || int_threshold > 1) {
    stop("int_threshold must be between 0 and 1")
  }
  
  
  # Validate align
  if (!align %in% c("left", "right", "center")) {
    stop("align must be 'left', 'right', or 'center'")
  }
  
  cat("All input validations passed\n")
  return(TRUE)
}


#' Function to groupo polygons by date proximity
#' 
#' @description
#' This function groups polygons by date proximity, as indicated by the tolerance_days argument
#' 
#' @param pol_date_pairs A data.frame with polygons and dates. 
#' @param tolerance_days An integer indicating the maximum separation in time units tolerated for polygons to be grouped together.
#'    
#' @return A data.frame with the polygon groupings by date.

group_polygons_by_date <- function(pol_date_pairs, tolerance_days = 7) {
  
  if (nrow(pol_date_pairs) == 0) return(pol_date_pairs)
  
  # Convert dates to Date objects
  pol_date_pairs$date_obj <- as.Date(pol_date_pairs$date)
  
  # Sort by date
  pol_date_pairs <- pol_date_pairs[order(pol_date_pairs$date_obj), ]
  
  # Initialize groups
  pol_date_pairs$date_group <- 1
  current_group <- 1
  
  # Group subsequent polygons
  if (nrow(pol_date_pairs) > 1) {
    for (i in 2:nrow(pol_date_pairs)) {
      # Calculate days difference from previous polygon
      days_diff <- as.numeric(pol_date_pairs$date_obj[i] - pol_date_pairs$date_obj[i-1])
      
      if (!is.na(days_diff) && days_diff <= tolerance_days) {
        # Same group
        pol_date_pairs$date_group[i] <- current_group
      } else {
        # New group
        current_group <- current_group + 1
        pol_date_pairs$date_group[i] <- current_group
      }
    }
  }
  
  # Remove temporary date_obj column
  pol_date_pairs$date_obj <- NULL
  
  return(pol_date_pairs)
}

#' Function to create a relative baseline
#' 
#' @description
#' This function takes as input a date and creates a relative baseline following the hist_lags, width and align parameters. 
#' 
#' @param d date in YYYY-MM-DD format
#' @param hist_lags An integer indicating how many year lags are used to build the historical window
#' @param width An integer indicating the size of the window sequence
#' @param align An integer indicating how the window sequence is aligned with respect to the date
#' @return A vector of dates 
#' @examples 
#' \dontrun{
#' historic_baseline <- relative_baseline("2000-01-01", hist_lags = 10, width = 10, align = "center")
#' }
relative_baseline <- function(d, hist_lags = 10, width = 5, align = 'center') {
  
  d <- sub('-02-29', '-02-28', d) # treat feb 29 (leap days) as feb 28
  if (hist_lags > 0) {
    # Find start date by subtracting years
    d <- as.Date(paste0(as.numeric(substr(d, 1, 4)) - hist_lags, substr(d, 5, 10))) 
    # Get sequence of dates
    d <- seq(d, by = 'year', length.out = hist_lags) 
  } else {
    # If hist_lags is 0, 'd' is already the correct starting date
    d <- as.Date(d)
  }
  
  if (align=='center') {r <- floor(width/2); w <- -r:r}
  if (align=='right') {w <- -(width-1):0}
  if (align=='left') {w <- 0:(width-1)}
  as.character(as.Date(sapply(d, \(x) x + w)))
}



#' Create intermediate directories if they do not already exist
#' 
#' @description
#' Checks if a directory exists and creates it if not
#' 
#' @param base_path A character string indicating the base folder path
#' @param subfolders A vector with subfolders to create inside the baseline path. 
#' 
#' @return Creates, if necessary, the baseline folders and its respective subfolders
create_dirs <- function(base_path, subfolders){
  
  if (!dir.exists(base_path)){
    message(base_path, " does not exist. Creating it from scratch along with the required the sub-folders.\n")
  }
  
  for (subfolder in subfolders){
    full_path <- file.path(base_path, subfolder)
    dir.create(full_path, recursive = TRUE, showWarnings = FALSE)
  }
  
}
