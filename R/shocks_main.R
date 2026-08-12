#' @importFrom future availableCores plan sequential
#' @importFrom future.callr callr
#' @importFrom stringr str_replace_all
#' @importFrom future.apply future_lapply
#' @importFrom arrow open_dataset write_parquet
#' @importFrom data.table rbindlist data.table setorder setDT
#' @importFrom purrr map2 reduce
#' @importFrom rlang expr sym
#' @importFrom dplyr select any_of collect filter mutate
NULL


#' Wrapper function for shock estimation
#' 
#' @description
#' Wrapper function to estimates deviations in climate exposure relative to a baseline
#'
#' @param pol_date_pairs A dataframe with unique geometry id and date pairs. The geometry IDs must correspond to those
#'                       in the parquet file with climate measures. Dates must be in date format (eg: "YYYY-MM-DD"). 
#'                       
#' @param conditions_list A named list of conditions to query the parquet file with climate measures. The named conditions 
#'                        should be: 
#'                        1. data_path: A character string with the path to the parquet file.
#'                        2. geom_id: A vector of unique geometry IDs.
#'                        3. trans_type: A character string indicating the type of transformations to be included (e.g., "polynomial").
#'                        4. trans_var: A character string indicating the degrees of the transformation to include.
#'                        5. product_temp_res: A character indicating the temporal resolution of the product. Must be one of the following: "daily" or "monthly" ("yearly" not supported yet). 
#' @param geom_id_col_parquet: a character string indicating the name of the column in the parquet file that identifies the geometries
#' @param geom_id_col_df: a character string indicating the name of the column in the that identifies the geometries
#' @param date_id_col_df: a character string indicating the name of the column in the dataframe that identifies the dates
#' @param window An integer indicating the length of the data sequence (i.e., the number of tempora units in the data sequence, such as the number of days).
#' @param start: An integer indicating the offset in days with respect to the date polygon date. 
#' @param hist_lags: An integer indicating the number of years to construct the historical baseline. Required if window_spec is "dynamic" or "both".
#' @param align: A character string indicating whether the sequence is left- or right-aligned with respect to the polygon date. 
#'               Left-alignment means that the sequence contains dates AFTER the polygon date. Right-alingment means that the sequence includes dates BEFORE the polygon date. 
#' @param time_step_size: An integer indicating the number of days in each temporal bin that divides the date sequence (e.g., 30 means each bin contains 30 days).
#' @param disjoint: A boolean indicating whether the bins are disjoint or overlapping. Default is overlapping (disjoint = FALSE).
#' @param window_spec: A character string indicating whether the historical baseline is dynamic, fixed or both. Must be either "dynamic", "fixed", or "both".
#' @param start_date: A character string in date format ("YYYY-MM-DD") indicating the start date of the historical window. Only required if window_spec= "dynamic" or window_spec="both". 
#' @param stop_date: A character string in date format ("YYYY-MM-DD") indicating the end date of the historical window. Only required if window_spec= "dynamic" or window_spec="both". 
#' @param met_cols A vector of columns names in the parquet file that are used as metadata columns.
#' @param max_group_size: A integer indicating the maximum number of polygons in each processing group.
#' @param date_tolerance_days: An integer indicating the maximum number of dates that two polygon interviews should be apart to consider them part of two different polygon groups. 
#' @param int_threshold A float number indicating the minimum threshold that date sequences must intersect with the date columns in the parquet file to be considered valid.
#' @param prop_cores: A float unmber indicating the proportion of cores to use for parallelization. 
#' @param query: A character string with the name given to the output. 
#' @param output_path: A character string with the path to the folder where the output should be stored.
#' @param error_log_path: A character string with the path to the folder where the error logs are stored. 
#' 
#' @details
#' The function performs the following operations: 
#' - Input validation
#' - Groups polygons by date proximity. This helps decrease the size of the parquet query (i.e., polygons with dates close to each other share date columns, reducing the number of total columns)
#' - Executes the estimation of exposure to climate shocks
#' - Produces error logs, which register different kinds of anomalies. Check below. 
#' - Saves error logs and the results in a long format.  
#' - The error logs register the following anomalies:
#'   1. Dates not in the product´s range
#'   2. Not enough data to build historical baselines (according to the int_threshold parameter), and 
#'   3. Missing data in the parquet files. 
#' 
#' @return A list containing the output path and the error logs path. A NULL is returned if a fatal error prevented any estimation from taking place (e.g., the function failed). 
#' If the output path is NULL, an error was produced when generating the climate shock measures and no valid estimates were created. 
#' If the error logs list is NULL, no errors or anomalies (i.e., check details) were recorded.
#' 
#' @export
shocks_wrapper <- function(
    pol_date_pairs, 
    conditions_list, 
    geom_id_col_parquet,
    geom_id_col_df, 
    date_id_col_df,
    window, 
    start, 
    hist_lags, 
    align, 
    time_step_size, 
    window_spec, 
    disjoint, 
    int_threshold,
    prop_cores, 
    max_group_size,
    query,  
    output_path, 
    error_log_path, 
    start_date = NULL, 
    stop_date = NULL,
    met_cols = c("trans_type", "trans_var"),
    date_tolerance_days = 10
    ){
  
  # ------------------------ Input Validation ----------------------
  # Ensure all required packages are installed
  check_packages()
  
  # Add polygon-date unique ID for downstream processing
  pol_date_pairs <- create_pairs(pol_date_pairs, geom_id_col = geom_id_col_df, date_id_col = date_id_col_df) # Add renaming columns ids
  
  # Filter-out polygons not in conditions list
  keep_geoms <- conditions_list[["geom_id"]]
  pol_date_pairs <- pol_date_pairs %>% filter(geom_id %in% keep_geoms)
  print(paste("Rows pol_date pairs:", nrow(pol_date_pairs)))

  # Validate inputs
  validate_inputs(pol_date_pairs, conditions_list, window, start, hist_lags, align, time_step_size, prop_cores, int_threshold)
  if(is.null(output_path) | is.null(error_log_path)){stop("Please provide an output and/or and error log path.")}
  if(window_spec %in% c("fixed", "both") && (is.null(start_date) | is.null(stop_date))){stop("Provide a start date and stop date if window_spec is fixed or both.")}
  
  # Ensure product resolution is specified
  prod_res <- conditions_list[["product_temp_res"]]
  if (is.null(prod_res) || length(prod_res) == 0 || !(prod_res %in% c("daily", "monthly"))) {
    stop("product_temp_res must be specified in the conditions list as daily or monthly.")
  }
  
  # Create intermediary directories for outputs and error logs if they do not already exist
  required_folders <- list(
    output = c("intermediate", "final"),
    errors = c("intermediate")
  )
  
  create_dirs(output_path, required_folders$output)
  create_dirs(error_log_path, required_folders$errors)
  
  cat("Intermediate parquet files will be written to", file.path(output_path, "intermediate"), " and then deleted.\n")
  cat("The final parquet file will be written to", file.path(output_path, "final"), ".\n")
  cat("If errors are found, the error log will be returned and written to", file.path(error_log_path), ".\n")
  
  # Make sure the data path is included in the conditions list
  if (is.null(conditions_list$data_path) || conditions_list$data_path == "") {
    stop("Please provide the path to the parquet file by including it in the conditions list with the name data_path")
  }
  
  # Exclude geometry-interview dates for which we do not have climate data in the parquet file and geometries not in the parquet
  tryCatch({
    # Load parquet
    p <- arrow::open_dataset(conditions_list$data_path)
  }, error = function(e) {
    stop("Could not open the parquet file: ", e$message)
  })

  geoms_in_parquet <- p %>% dplyr::select(dplyr::any_of(geom_id_col_parquet)) %>% pull(as_vector = TRUE) %>% unique()
  pol_date_pairs <- pol_date_pairs %>% dplyr::filter(geom_id %in% geoms_in_parquet)
  print(paste("Processing", nrow(pol_date_pairs), "geometry-date pairs."))

  
  if (prod_res == "daily"){
    climate_dates <- colnames(p)[grepl("^\\d{4}-\\d{2}-\\d{2}$", colnames(p))]
    if (length(climate_dates) == 0){stop("The resolution in the parquet file is different from that indicated in the conditions list.")}
    print("Current date range in parquet:")
    print(paste("From:", min(climate_dates), "To:", max(climate_dates)))
    print("Date range in polygon-date dataframe:")
    print(paste("From:", min(pol_date_pairs$date), "To:", max(pol_date_pairs$date)))
    min_d <- as.Date(min(climate_dates))
    max_d <- as.Date(max(climate_dates))
    
    orig_nrow <- nrow(pol_date_pairs)
    excluded_pols <- pol_date_pairs[as.Date(pol_date_pairs$date) < min_d | as.Date(pol_date_pairs$date) > max_d, ]
    excluded_dates <- excluded_pols$date
    pol_date_pairs <- pol_date_pairs[as.Date(pol_date_pairs$date) >= min_d & as.Date(pol_date_pairs$date) <= max_d, ]
    
    filtered_nrow <- nrow(pol_date_pairs)
    if(filtered_nrow == 0){stop("There are no polygons to process as all the interview dates are outside the date range of the parquet file.")}
    print(paste("Kept", filtered_nrow, "out of", orig_nrow, "polygon-date pairs (the dropped polygon-dates were out of range) | ~", 100 * round((filtered_nrow/orig_nrow), 2), "% kept."))
    
    # Explicitly print dates not found in dataset
    print(paste("Dates excluded:", sort(unique(excluded_dates))))
    
  } else if (prod_res == "monthly"){
    climate_dates <- colnames(p)[grepl("^\\d{4}-\\d{2}$", colnames(p))]
    if (length(climate_dates) == 0){stop("The resolution in the parquet file is different from that indicated in the conditions list.")}
    print("Current date range in parquet:")
    print(paste("From:", min(climate_dates), "To:", max(climate_dates)))
    pol_date_pairs$date_month_fmt <- as.character(format(as.Date(pol_date_pairs$date), "%Y-%m")) 
    print("Date range in polygon-date dataframe:")
    print(paste("From:", min(pol_date_pairs$date_month_fmt), "To:", max(pol_date_pairs$date_month_fmt)))
    min_d <- as.Date(min(climate_dates))
    max_d <- as.Date(max(climate_dates))
    
    orig_nrow <- nrow(pol_date_pairs)
    excluded_pols <- pol_date_pairs[as.Date(pol_date_pairs$date_month_fmt) < min_d | as.Date(pol_date_pairs$date_month_fmt) > max_d, ]$date
    excluded_dates <- excluded_pols$date
    pol_date_pairs <- pol_date_pairs[as.Date(pol_date_pairs$date_month_fmt) >= min_d & as.Date(pol_date_pairs$date_month_fmt) <= max_d, ]
    
    filtered_nrow <- nrow(pol_date_pairs)
    if(filtered_nrow == 0){stop("No polygons to process as the interview dates are outside the date range of the parquet file.")}
    print(paste("Kept", filtered_nrow, "out of", orig_nrow, "polygon-date pairs (dropped polygons were out of range) | ~", 100 * round((filtered_nrow/orig_nrow), 2), "% kept."))
    
    # Explicitly print dates not found in dataset
    print(paste("Dates excluded:", sort(unique(excluded_dates))))
  } 
  
  # Check metadata columns are correct
  metadata_cols <- setdiff(colnames(p), climate_dates)
  miss_cols_metad_b <- sapply(met_cols, function(x){if(x %in% metadata_cols){return(FALSE)} else {return(TRUE)}})
  miss_cols_metad <- met_cols[miss_cols_metad_b]
  if (length(miss_cols_metad) > 0){
    stop(paste("These metadata columns are not in the parquet file:", miss_cols_metad))
  }
  
  # Check that historic fixed window range is in the parquet
  if (window_spec == "fixed" | window_spec == "both"){
    if (prod_res == "daily"){
      if (as.Date(start_date) < as.Date(min_d)){
        cat("------------------------------------------------------------------------------------------------\n")
        cat("WARNING:\n The fixed window starts outside of the product´s range.\n Some sequences will be lost if they do not meet the intersection threshold of", 100 * int_threshold, "%.\n")
        cat("------------------------------------------------------------------------------------------------\n")
      }
    } else if(prod_res == "monthly"){
      if (as.Date(as.character(format(as.Date(start_date), "%Y-%m"))) < as.Date(min_d)){
        cat("------------------------------------------------------------------------------------------------\n")
        cat("WARNING:\n The fixed window starts outside of the product´s range.\n Some sequences will be lost if they do not meet the intersection threshold of", 100 * int_threshold, "%.\n")
        cat("------------------------------------------------------------------------------------------------\n")
      }
    }
  }
  
  # Erase previous error logs if they exist 
  previous_logs <- dir(path = file.path(error_log_path, "intermediate"), pattern = ".*error_log_", full.names = TRUE)
  if(length(previous_logs) !=0){
    cat("WARNING: deleting all previously existing error logs found in", file.path(error_log_path, "intermediate") ,".\n")
    unlink(previous_logs, recursive = TRUE)
  }
  
  # ------------------------------ Pre Processing ----------------------------------
  # Group polygons by date
  print(paste("Grouping polygons by date proximity (tolerance:", date_tolerance_days, "days)"))
  pol_date_pairs_grouped <- group_polygons_by_date(pol_date_pairs, tolerance_days = date_tolerance_days)
  
  # Ensure all groups have at most max_group_size_polygons
  pol_date_pairs_grouped <- split_large_groups(pol_date_pairs_grouped, max_group_size)
  
  # Show grouping results
  n_groups <- max(pol_date_pairs_grouped$date_group)
  group_sizes <- table(pol_date_pairs_grouped$date_group)
  
  print(paste("Created", n_groups, "date groups."))
  print("Group sizes:")
  print(as.vector(group_sizes))
  
  # Set workers
  n_workers <- max(1, floor(prop_cores * future::availableCores()))
  cat("Using", n_workers, "cores.\n")
  
  #  -------------------------- Shocks estimation ---------------------------
  if(window_spec == "dynamic"){
    cat("Estimating shocks with dynamic window \n")
    shock_res <- shocks(
      pol_date_pairs_grouped = pol_date_pairs_grouped,
      conditions = conditions_list, 
      geom_id_col = geom_id_col_parquet, 
      window = window, 
      start = start, 
      hist_lags = hist_lags, 
      align = align, 
      time_step_size = time_step_size, 
      window_spec = "dynamic", 
      prod_res = prod_res,
      start_date = NULL, 
      stop_date = NULL,
      met_cols = met_cols,
      climate_dates = climate_dates, 
      int_threshold = int_threshold, 
      query = query, 
      n_groups = n_groups,
      disjoint = disjoint, 
      n_workers = n_workers,
      output_path = output_path, 
      error_log_path = error_log_path
    )   
    
  } else if (window_spec == "fixed"){
    cat("Estimating shocks with fixed window \n")
    shock_res <- shocks(
      pol_date_pairs_grouped = pol_date_pairs_grouped,
      conditions = conditions_list, 
      geom_id_col = geom_id_col_parquet, 
      window = window, 
      start = start, 
      hist_lags = hist_lags, 
      align = align, 
      time_step_size = time_step_size, 
      window_spec = "fixed", 
      prod_res = prod_res,
      start_date = start_date, 
      stop_date = stop_date,
      met_cols = met_cols,
      climate_dates = climate_dates, 
      int_threshold = int_threshold, 
      query = query, 
      n_groups = n_groups,
      disjoint = disjoint, 
      n_workers = n_workers,
      output_path = output_path, 
      error_log_path = error_log_path
    )    
    
  } else if (window_spec == "both"){
    cat("Estimating shocks with dynamic and fixed windows \n")
    print("Begining dynamic window estimation")
    shock_res_d <- shocks(
      pol_date_pairs_grouped = pol_date_pairs_grouped,
      conditions = conditions_list, 
      geom_id_col = geom_id_col_parquet, 
      window = window, 
      start = start, 
      hist_lags = hist_lags, 
      align = align, 
      time_step_size = time_step_size, 
      window_spec = "dynamic", 
      prod_res = prod_res,
      start_date = NULL, 
      stop_date = NULL,
      met_cols = met_cols,
      climate_dates = climate_dates, 
      int_threshold = int_threshold, 
      query = query, 
      n_groups = n_groups,
      disjoint = disjoint, 
      n_workers = n_workers,
      output_path = output_path, 
      error_log_path = error_log_path
    )
    print("Finished dynamic window estimation")
    
    print("Begining fixed window estimation")
    shock_res_f <- shocks(
      pol_date_pairs_grouped = pol_date_pairs_grouped,
      conditions = conditions_list, 
      geom_id_col = geom_id_col_parquet, 
      window = window, 
      start = start, 
      hist_lags = hist_lags, 
      align = align, 
      time_step_size = time_step_size, 
      window_spec = "fixed", 
      prod_res = prod_res,
      start_date = start_date, 
      stop_date = stop_date,
      met_cols = met_cols,
      climate_dates = climate_dates, 
      int_threshold = int_threshold, 
      query = query, 
      n_groups = n_groups,
      disjoint = disjoint, 
      n_workers = n_workers,
      output_path = output_path, 
      error_log_path = error_log_path
    )
    
    print("fInished fixed window estimation")
    shock_res <- shock_res_d & shock_res_f
    
  }
  
  # ------------------------- Error logs ---------------------------------
  miss_cols_error_logs <- dir(path = file.path(error_log_path, "intermediate"), pattern = "missing_cols_error_log_", full.names = TRUE) 
  na_cols_error_logs <- dir(path = file.path(error_log_path, "intermediate"), pattern = "NA_data_error_log_", full.names = TRUE)
  final_error_list <- list()
  
  # Add excluded polygons
  if (nrow(excluded_pols) != 0) {
    cat("------------------------------------------------------------------------------------------------\n")
    cat("Some polygons were excluded because their date was outside of the product´s range.")
    cat("Returning a named list. Use [[errors]][[excluded_polygons]] to inspect the dates and the associated polygons that were excluded.\n")
    final_error_list[["excluded_polygons"]] <- excluded_pols
    
  }
  if (length(miss_cols_error_logs) != 0){
    cat("------------------------------------------------------------------------------------------------\n")
    cat("WARNING:\nThere are error logs concerning missing dates in the parquet file:\n")
    cat("Some date sequences did not meet the minimum threshold of ", 100 * int_threshold ," % of intersection with the date columns in the product´s range.\n")
    cat("The polygons with these dates were excluded from processing and analysis. Check the error log list.\n")
    cat("Returning a named list. Use [[errors]][[missing_dates]] to inspect the dates and the associated polygons that were excluded.\n")
    cat("The error log list was exported to: ", error_log_path ,".\n")
    cat("------------------------------------------------------------------------------------------------\n")
    miss_cols_error_log_list <- lapply(miss_cols_error_logs, readRDS)
    final_error_list[["missing_dates"]] <- do.call(c, miss_cols_error_log_list)
    file.remove(miss_cols_error_logs)
  }
  
  if (length(na_cols_error_logs) != 0){
    cat("------------------------------------------------------------------------------------------------\n")
    cat("WARNING:\nThere are error logs concerning NAs in the raw data:\n")
    cat("Some polygons had NAs in their raw data columns.\n")
    cat("Returning a named list. Use [[na_in_data]] to inspect these cases.\n")
    cat("The individual error log lists were exported to: ", error_log_path ,".\n")
    cat("------------------------------------------------------------------------------------------------\n")
    na_cols_error_log_list <- lapply(na_cols_error_logs, readRDS)
    final_error_list[["na_in_data"]] <- do.call(c, na_cols_error_log_list)
    file.remove(na_cols_error_logs)
  }
  
  if (length(final_error_list) > 0){
    saveRDS(final_error_list, file = file.path(error_log_path, paste0(query, "_", window_spec, "_all_errors_log.RDS")))
    cat("Returning error log list.\n")
    if (length(dir(path = file.path(output_path, "final"), pattern = paste0(query, ".*\\.parquet$"), full.names = TRUE)) > 0){
      cat("Output for valid polygons and sequences was exported to", paste0(output_path, "/final\n"))
      output_desc <- dir(path = file.path(output_path, "final"), pattern = paste0(query, ".*\\.parquet$"), full.names = TRUE)
    } else {
      cat("Error creating the final parquet file. Check the error logs.\n")
      output_desc <- ""
    }
    return(
      list(
        output = output_desc,
        errors = final_error_list)
      )
  }
  
  if (length(dir(path = file.path(output_path, "final"), pattern = paste0(query, ".*\\.parquet$"), full.names = TRUE)) > 0){
    cat("Output was succesfully exported to", paste0(output_path, "/final\n"))
    cat("No NA values in the raw data were found. All date sequences were valid as per the intersection threshold. No error log is returned or produced.\n")
    return(
      list(
        output = dir(path = file.path(output_path, "final"), pattern = paste0(query, ".*\\.parquet$"), full.names = TRUE),
        errors = NULL
        )
      )
  } else {
    cat("Error creating the final parquet file. Check the error logs.\n")
    return(NULL)
  }
}


#' Function to estimate climate shocks
#' 
#' @description
#' This function estimates the climate shocks as well as the climate measures (i.e., mean, SD) for each polygon-date pair. 
#' Several arguments used in this function are inherited from the shocks_wrapper function. 
#' 
#' @param n_groups An integer indicating the total number of polygon groups. Estimated by the shocks_wrapper function. 
#' @param n_workers An integer indicating how many cores are used for parallelization. Inherited from shocks_wrapper.
#' @param pol_date_pairs_grouped A dataframe with unique geometry id and date pairs and a group id. The geometry IDs must correspond to those
#'                               in the parquet file with climate measures. Dates must be in date format (eg: "YYYY-MM-DD"). Inherited from shocks_wrapper.
#'                       
#' @param conditions_list A named list of conditions to query the parquet file with climate measures. Inherited from shocks_wrapper.
#' @param geom_id_col: a character string indicating the name of the column in the parquet file that identifies the geometries
#' @param window An integer indicating the length of the data sequence. Inherited from shocks_wrapper.
#' @param start: An integer indicating the offset in days with respect to the date polygon date. Inherited from shocks_wrapper.
#' @param hist_lags: An integer indicating the number of years to construct the historical baseline. Required if window_spec is "dynamic" or "both". Inherited from shocks_wrapper.
#' @param align: A character string indicating whether the sequence is left- or right-aligned with respect to the polygon date. Inherited from shocks_wrapper.
#'               Left-alignment means that the sequence contains dates AFTER the polygon date. Right-alignment means that the sequence includes dates BEFORE the polygon date. Inherited from shocks_wrapper. 
#' @param time_step_size: An integer indicating the number of days in each temporal bin that divides the date sequence. Inherited from shocks_wrapper.
#' @param disjoint: A boolean indicating whether the bins are disjoint or overlapping. Inherited from shocks_wrapper.
#' @param window_spec: A character string indicating whether the historical baseline is dynamic, fixed or both. Inherited from shocks_wrapper.
#' @param start_date: A character string in date format ("YYYY-MM-DD") indicating the start date of the historical window. Only required if window_spec= "dynamic" or window_spec="both". Inherited from shocks_wrapper. 
#' @param stop_date: A character string in date format ("YYYY-MM-DD") indicating the end date of the historical window. Only required if window_spec= "dynamic" or window_spec="both". Inherited from shocks_wrapper.
#' @param met_cols A vector of columns names in the parquet file that are used as metadata columns. Inherited from shocks_wrapper.
#' @param max_group_size: A integer indicating the maximum number of polygons in each processing group. Inherited from shocks_wrapper.
#' @param date_tolerance_days: An integer indicating the maximum number of dates that two polygon interviews should be apart to consider them part of two different polygon groups. Inherited from shocks_wrapper.
#' @param int_threshold A float number indicating the minimum threshold that date sequences must intersect with the date columns in the parquet file to be considered valid. Inherited from shocks_wrapper.
#' @param prop_cores: A float number indicating the proportion of cores to use for parallelization. Inherited from shocks_wrapper.
#' @param query: A character string with the name given to the output. Inherited from shocks_wrapper.
#' @param output_path: A character string with the path to the folder where the output should be stored. Inherited from shocks_wrapper.
#' @param error_log_path: A character string with the path to the folder where the error logs are stored. Inherited from shocks_wrapper.
#' @details
#' This function exports intermediate parquet files with climate measures and shocks, as well as any error lists if produced. 
#' The function performs the following operations: 
#' - Estimates the number of binds within the date sequence according to the bin width. Automatically adjusts for unequal bin sizes. 
#' - Process in parallel (or sequentially if n_workers = 1) the estimation of shocks for the polygon groups. 
#' - Loads data from the parquet file corresponding only to the polygons in a specific group and according to the conditions list.
#' - Checks for NA in original parquet file and produces error logs indicating which polygons and dates have NAs, if any. 
#' - Estimates climate measures and shocks for each polygon-date pair, according to the conditions list and the binning structure. 
#' - Stores temporal intermediate parquet files in the intermediate folder. 
#' - Returns a list with metadata about the intermediate objects´ and the error logs´ paths. 
#'
#' @return: A a list with metadata (i.e., file paths to intermediate parquet files and error logs). 
#' @export
shocks <- function(
    n_groups, 
    n_workers,  
    pol_date_pairs_grouped, 
    conditions_list, 
    geom_id_col,
    window, 
    start, 
    hist_lags, 
    align, 
    time_step_size, 
    window_spec, 
    prod_res,
    start_date, 
    stop_date, 
    met_cols,
    climate_dates, 
    int_threshold,
    query, 
    output_path,
    error_log_path, 
    disjoint = FALSE
){
  
  # Define bin scheme
  bin_scheme <- tryCatch({
    create_bin_scheme(window = window, time_step_size = time_step_size, align = align, disjoint = disjoint)
  }, error = function(e) {
    stop(paste("Error creating bin scheme:", e$message))
  })
  
  # Process each group in parallel 
  future::plan(future.callr::callr, workers = n_workers)
  
  chunk_results <- tryCatch({
    future.apply::future_lapply(
      1:n_groups, 
      function(group_id) {
        
        # Get the subset of pol_date_pairs for this chunk
        chunk_pol_date_pairs <- pol_date_pairs_grouped[pol_date_pairs_grouped$date_group == group_id, ]
        
        # Extract data for this chunk only
        chunk_main_df <- tryCatch({
          get_data(
            group = group_id,
            pol_date_pairs = chunk_pol_date_pairs, 
            conditions_list = conditions_list, 
            geom_id_col,
            window = window, 
            start = start, 
            hist_lags = hist_lags, 
            align = align,
            time_step = prod_res,
            window_spec = window_spec, 
            start_date = start_date, 
            stop_date = stop_date, 
            climate_dates = climate_dates, 
            int_threshold = int_threshold,
            error_log_path = error_log_path
          )
        }, error = function(e) {
          warning(paste("Error getting data for chunk:", e$message))
          return(NULL)
        })
        
        if (is.null(chunk_main_df)){
          cat("No valid sequences for group", group_id,"using the current threshold. Check the error log.\n")
          return(NULL)
        }
        
        # Polygons to process
        # --- Read in cases where polygon-dates did not pass the threshold limit
        suppressWarnings(
          error_list <- tryCatch({
            readRDS(
              file.path(error_log_path, paste0("intermediate/", "missing_cols_error_log_", group_id, "_" , window_spec, ".RDS"))
            )
          }, 
          error = function(e){
            return(NULL)
          })
        )
        
        if (!is.null(error_list)){
          
          problematic_combinations <- data.frame(
            geom_id = character(0),
            date = character(0),
            int_pct = numeric(0),
            stringsAsFactors = FALSE
            )
          
          for (error_case in error_list) {
            
            error_date <- names(error_case)
            error_polygons <- error_case[[error_date]]$polygons
            error_int <- error_case[[error_date]]$intersection_pct
            
            problematic_combinations <- rbind(
              problematic_combinations,
              data.frame(
                geom_id = error_polygons,
                date = rep(error_date, length(error_polygons)),
                int_pct = rep(error_int, length(error_polygons)),
                stringsAsFactors = FALSE
              )
            )
          }
          
          problematic_combinations <- create_pairs(problematic_combinations)
          chunk_pol_date_pairs <- chunk_pol_date_pairs[!chunk_pol_date_pairs$pol_date_id %in% problematic_combinations$pol_date_id, ]
    
        }

        # Extract polygon ids to process
        unique_polygons <- unique(chunk_pol_date_pairs$geom_id)
        
        # Process ALL polygons in this group
        all_polygon_results <- lapply(unique_polygons, function(polygon_id) {
          
          # Get dates for this specific polygon
          polygon_dates <- chunk_pol_date_pairs[chunk_pol_date_pairs$geom_id == polygon_id, ]
          
          # Pre compute dates
          windows_data <- lapply(
            polygon_dates$date, 
            function(d) {
              current_ds <- window_seq(d = as.character(d), start = start, window = window, 
                                       align = align, hist_lags = 0, window_spec = window_spec, 
                                       start_date = start_date, stop_date = stop_date)
              
              dates_mat <- window_seq(d, start = start, window = window, align = align, 
                                      hist_lags = hist_lags, window_spec = window_spec, 
                                      start_date = start_date, stop_date = stop_date, only_hist = TRUE) # Only return historic sequences
              
              if (hist_lags == 1) {
                dates_mat <- matrix(dates_mat, nrow = 1, byrow = TRUE)
              } else {
                dates_mat <- matrix(dates_mat, ncol = length(current_ds), byrow = TRUE)
              }
              list(date = d, current_ds = current_ds, dates_mat = dates_mat)
            })
          
          # Filter data for this polygon
          poly_data <- chunk_main_df[geom_id == polygon_id]
          
          # Check for NAs in data
          na_check_results <- check_na_in_date_sequences(
            windows_data = windows_data,
            polygon_id = polygon_id, 
            main_dt = poly_data,
            metad_cols = met_cols,
            climate_dates = climate_dates
          )
          
          # Process all bins for this polygon
          shocks_results <- lapply(names(bin_scheme), function(bin_name) {
            bin <- bin_scheme[[bin_name]]
            
            get_window_estimates_vectorized(
              polygon_id = polygon_id,
              all_windows_data = windows_data,
              bin = bin,  
              bin_name = bin_name,
              main_dt = poly_data, 
              pol_date_pairs_set = polygon_dates, 
              start = start,
              window = window, 
              hist_lags = hist_lags, 
              time_step = prod_res, 
              align = align, 
              window_spec = window_spec, 
              start_date = start_date, 
              stop_date = stop_date, 
              metad_cols = met_cols, 
              climate_dates = climate_dates
            )
          })
          
          # Combine bins for this polygon
          f_data <- data.table::rbindlist(shocks_results)
          
          return(list(data = f_data, na_errors = na_check_results))
        })
        
        # Extract data and error logs
        # Combine data for all polygons in this group
        all_shocks <- data.table::rbindlist(lapply(all_polygon_results, `[[`, "data"))
        
        # Extract error list
        errors_list <- do.call(
          c, 
          lapply(
            all_polygon_results, 
            function(x){
              x_vals <- x[["na_errors"]]
              if (class(x_vals) == "logical" & !isTRUE(x)){return(NULL)} else{return(x_vals)}
            })
        )
        
        errors_list <- Filter(Negate(is.null), errors_list)
        
        # Export existing error logs, if any
        if (length(errors_list) > 0){
          error_export_int_path <- file.path(error_log_path, "intermediate")
          saveRDS(errors_list, file = file.path(error_export_int_path, paste0("NA_data_error_log_", group_id, "_", window_spec, ".RDS")))
        }
        
        # Write data.table with shocks and return only metadata
        data_export_int_path <- file.path(output_path, "intermediate")
        chunk_file_path <- file.path(data_export_int_path, paste0("chunk_", group_id, "_", window_spec, ".parquet"))
        arrow::write_parquet(all_shocks, chunk_file_path)
        gc(verbose = FALSE)
        
        return(list(
          group_id = group_id, 
          polygon_ids = unique_polygons,
          file_path = chunk_file_path
        ))
      },
      future.seed = TRUE
    ) # End of future_lapply
  }, error = function(e) {
    stop(paste("Error in chunked processing for chunk:", e$message))
  }) # End of Try Catch
  
  future::plan(future::sequential); gc(verbose = FALSE)
  cat("All groups have been processed. \n")
  
  # Combine all results
  data_export_final_path <- file.path(output_path, "final")
  final_file_path <- file.path(data_export_final_path, paste0(query, "_", window_spec, ".parquet"))
  chunk_files <- sapply(chunk_results, function(x){return(x$file_path)})
  
  # Combine datasets on disk
  if (!is.character(chunk_files) || length(chunk_files) == 0) {
    cat("No valid chunk files were created. Check error logs.\n") 
    return(NULL)
  }
  cat("Combining results into a single parquet file. \n")
  all_data <- arrow::open_dataset(chunk_files)
  arrow::write_parquet(all_data, final_file_path)
  
  # Remove intermediate files
  print("Cleaning intermediate objects.")
  file.remove(chunk_files)
  
  # Return metadata
  return(TRUE)
}


#' Inner function to estimate climate measures and shocks from data.table objects
#' @param polygon_id: A chatacter string with polygon identifier
#' @param all_windows_data: lA ist with contemporary and historic date sequences for a given polygon-date pair
#' @param bin: A boolean vector with binning structure
#' @param bin_name: A character string indicating the bin name
#' @param main_dt: A data.table object with climate data for a given polygon-date pair
#' @param pol_date_pairs_set: A subset of the polyigon-dates dataframe
#' @param climate_dates: A vector with the parquet file date columns
#' @param time_step product´s resolution. Inherited from shocks. 
#' @param window An integer indicating the length of the data sequence. Inherited from shocks
#' @param start: An integer indicating the offset in days with respect to the date polygon date. Inherited from shocks.
#' @param hist_lags: An integer indicating the number of years to construct the historical baseline. Required if window_spec is "dynamic" or "both". Inherited from shocks.
#' @param align: A character string indicating whether the sequence is left- or right-aligned with respect to the polygon date. Inherited from shocks_wrapper.
#'               Left-alignment means that the sequence contains dates AFTER the polygon date. Right-alignment means that the sequence includes dates BEFORE the polygon date. Inherited from shocks. 
#' @param window_spec: A character string indicating whether the historical baseline is dynamic, fixed or both. Inherited from shocks.
#' @param start_date: A character string in date format ("YYYY-MM-DD") indicating the start date of the historical window. Only required if window_spec= "dynamic" or window_spec="both". Inherited from shocks. 
#' @param stop_date: A character string in date format ("YYYY-MM-DD") indicating the end date of the historical window. Only required if window_spec= "dynamic" or window_spec="both". Inherited from shocks.
#' @param metad_cols A vector of columns names in the parquet file that are used as metadata columns. Inherited from shocks.
#'   
#' @returns: A data.table object with climate measures and shocks for a given polygon id
get_window_estimates_vectorized <- function(
    polygon_id, 
    all_windows_data, 
    bin, 
    bin_name, 
    main_dt, 
    pol_date_pairs_set,
    start,
    window, 
    time_step, 
    align, 
    hist_lags, 
    window_spec,
    start_date, 
    stop_date, 
    metad_cols, 
    climate_dates
) {
  
  # Ensure we get all dates for a specific polygon at once
  poly_dates <- pol_date_pairs_set[pol_date_pairs_set$geom_id == polygon_id, ]$date
  
  # Get unique date columns needed 
  all_date_columns <- unique(
    unlist(
      lapply(all_windows_data, function(x) {c(x$current_ds, as.vector(x$dates_mat))})
    )
  ) 
  
  # Subset all the necessary columns for the polygon-date pairs in question
  all_date_columns <- intersect(all_date_columns, climate_dates)
  polygon_data <- main_dt[geom_id == polygon_id, .SD, .SDcols = c(all_date_columns, metad_cols)]
  
  # Process all date-transformation combinations in one data.table operation
  results <- polygon_data[, {
    
    # Process each date for this transformation
    date_results <- lapply(all_windows_data, function(window_info) {
      d <- window_info$date
      current_ds <- window_info$current_ds
      dates_mat <- window_info$dates_mat
      
      if (time_step == "daily") {
        contemp_mean_val <- daily_shocks(current_ds = current_ds, dates_mat = dates_mat, 
                                         bin = bin, return_measure = "contemp_avg", target_df = .SD, climate_dates = climate_dates, 
                                         hist_lags = hist_lags)
        contemp_sd_val <- daily_shocks(current_ds = current_ds, dates_mat = dates_mat, 
                                       bin = bin, return_measure = "contemp_sd", target_df = .SD, climate_dates = climate_dates, 
                                       hist_lags = hist_lags)
        hist_mean_val <- daily_shocks(current_ds = current_ds, dates_mat = dates_mat, 
                                      bin = bin, return_measure = "hist_avg", target_df = .SD, climate_dates = climate_dates, 
                                      hist_lags = hist_lags)
        hist_sd_val <- daily_shocks(current_ds = current_ds, dates_mat = dates_mat, 
                                    bin = bin, return_measure = "hist_sd", target_df = .SD, climate_dates = climate_dates, 
                                    hist_lags = hist_lags)
      } else if (time_step == "monthly") {
        
        # Build the weights for the current bin
        m_dates <- as.character(format(as.Date(current_ds[bin]), "%Y-%m"))
        date_weights <- as.data.frame(table(m_dates)/length(current_ds[bin]))
        colnames(date_weights) <- c("date", "weight")
        
        # Update weight table if there are months in the bin outside of the product´s range
        d_intersect <- intersect(climate_dates, date_weights$weight)
        if (length(d_intersect) == 0){
          date_weights$weight <- NA 
        } else if (length(d_intersect) != nrow(date_weights)){
          date_weights <- date_weights |> 
            dplyr::filter(date %in% d_intersect) |>
            dplyr::mutate(weight = weight/sum(weight, na.rm = T))
        }
        
        # Add climate measures and shocks
        contemp_mean_val <- monthly_shocks(current_ds = current_ds, dates_mat = dates_mat, date_weights = date_weights, bin = bin, return_measure = "contemp_avg", target_df = .SD, climate_dates = climate_dates, hist_lags = hist_lags)
        contemp_sd_val <- monthly_shocks(current_ds = current_ds, dates_mat = dates_mat, date_weights = date_weights, bin = bin, return_measure = "contemp_sd", target_df = .SD, climate_dates = climate_dates, hist_lags = hist_lags)
        hist_mean_val <- monthly_shocks(current_ds = current_ds, dates_mat = dates_mat, date_weights = date_weights,  bin = bin, return_measure = "hist_avg", target_df = .SD, climate_dates = climate_dates, hist_lags = hist_lags)
        hist_sd_val <- monthly_shocks(current_ds = current_ds, dates_mat = dates_mat, date_weights = date_weights, bin = bin, return_measure = "hist_sd", target_df = .SD, climate_dates = climate_dates, hist_lags = hist_lags)
        
      }
      
      # Return wide format for this date
      data.table::data.table(
        date = as.character(d),
        contemp_mean = contemp_mean_val,
        contemp_sd = contemp_sd_val,
        hist_mean = hist_mean_val,
        hist_sd = hist_sd_val,
        mean_shock = contemp_mean_val - hist_mean_val,
        sd_shock = contemp_sd_val - hist_sd_val
      )
    })
    
    # Combine all dates for this transformation
    data.table::rbindlist(date_results)
    
  }, by = .I]
  
  # Add metadata and identifiers
  results[, geom_id := polygon_id]
  results[, time_step_id := bin_name]
  results[, window_type := window_spec]
  
  # Add metadata columns
  metadata_expanded <- polygon_data[, .SD, .SDcols = metad_cols]
  results <- cbind(results, metadata_expanded[rep(1:.N, each = length(poly_dates))])
  
  # Pivot from wide to long
  id_vars <- c("geom_id", "date", metad_cols, "window_type", "time_step_id")
  measure_vars <- c("contemp_mean", "contemp_sd", "hist_mean", "hist_sd", "mean_shock", "sd_shock")
  
  data_pivoted <- reshape2::melt(results, id.vars = id_vars, measure.vars = measure_vars, variable.name = "measure")
  
  # Arrrange
  order_cols <- intersect(c("geom_id", "date", metad_cols, "measure"), names(data_pivoted))
  data.table::setorderv(data_pivoted, cols = order_cols)
  
  return(data_pivoted) 
}


#' Function to estimate daily shocks
#'
#' @description
#' This function estimates climate shocks when the product´s resolution is daily. 
#' 
#' @param current_ds: A vector of contemporary sequence of dates
#' @param dates_mat: A matrix where each row corresponds to a historical sequence of dates
#' @param bin: The bin id within the bin structure.
#' @param return_measure: A character indicating which climate measure to return. Must be: "contemp_avg", "contemp_sd", "historic_avg", "historic_sd")
#' @param target_df: A data.table object to perform the calculations on
#' @param climate_dates: A vector with the parquet file climate dates. Inherited. 
#' @param hist_lags: An integer indicating the number of lags to build the historical baseline. Inherited.
#' 
#' @returns the value of the indicated historical measure 
daily_shocks <- function(current_ds, dates_mat, bin, return_measure, target_df, climate_dates, hist_lags){
  
  # Extract date columns corresponding to current bin
  date_columns <- current_ds[bin]
  
  # Get only columns that are in the parquet
  date_columns <- intersect(date_columns, climate_dates)    
  
  # Current measures with error handling
  # --- Contemp. mean
  window_avg <- tryCatch({
    apply(target_df[, ..date_columns], 1, mean, na.rm = TRUE)
  }, error = function(e) {
    # Return NA if no columns in the bin are present in the data
    NA
  })
  if (return_measure == "contemp_avg"){return(window_avg)}
  
  # --- Contemp. SD
  window_sd <- tryCatch({
    apply(target_df[, ..date_columns], 1, sd, na.rm = TRUE)
  }, error = function(e) {
    print(paste("Error:", e))
    print(target_df[, ..date_columns])
    # Idem
    NA
  })
  if (return_measure == "contemp_sd"){return(window_sd)}
  
  # Historical measures
  if (hist_lags == 0){
    hist_mean <- NA
    hist_sd <- NA
    
    if (return_measure == "hist_avg"){return(hist_mean)}
    if (return_measure == "hist_sd"){return(hist_sd)}
    
  }
  
  # --- Hist. mean
  yearly_mean <- tryCatch({
    sapply(1:nrow(dates_mat), function(i){
      m <- dates_mat[i, ]
      d_cols <- m[bin]
      # Check if bin columns are in parquet
      d_cols <- intersect(d_cols, climate_dates)
      if (length(d_cols) == 0){return(NA)}
      apply(target_df[, ..d_cols], 1, mean, na.rm = TRUE) # This is eseentially the same as assigning NA to dates not in the parquet and then taking the average with na.rm = T
    })
  }, error = function(e) {
    # Idem
    rep(NA, nrow(dates_mat))
  })
  
  hist_mean <- mean(yearly_mean, na.rm = TRUE)
  if (return_measure == "hist_avg"){return(hist_mean)}
  
  # --- Hist. SD
  yearly_sd <- tryCatch({
    sapply(1:nrow(dates_mat), function(i){
      m <- dates_mat[i, ]
      d_cols <- m[bin]
      d_cols <- intersect(d_cols, climate_dates)
      if (length(d_cols) == 0){return(NA)}
      apply(target_df[, ..d_cols], 1, sd, na.rm = TRUE)
    })
  }, error = function(e) {
    rep(NA, nrow(dates_mat))
  })
  
  hist_sd <- mean(yearly_sd, na.rm = TRUE)
  if (return_measure == "hist_sd"){return(hist_sd)}
}

#' Function to estimate monthly shocks
#'
#' @description
#' This function estimates climate shocks when the product´s resolution is monthly.
#'
#' @param current_ds: A vector of contemporary sequence of dates
#' @param dates_mat: A matrix where each row corresponds to a historical sequence of dates
#' @param date_weights: A data.frame with months and their corresponding weights according to the proportion of coverage of the bin.
#' @param bin: The bin id within the bin structure.
#' @param return_measure: A character indicating which climate measure to return. Must be: "contemp_avg", "contemp_sd", "historic_avg", "historic_sd")
#' @param target_df: A data.table object to perform the calculations on
#' @param climate_dates: A vector with the parquet file climate dates. Inherited. 
#' @param hist_lags: An integer indicating the number of lags to build the historical baseline. Inherited.
#' 
#' @returns the value of the indicated historical measure
monthly_shocks <- function(current_ds, dates_mat, date_weights, bin, return_measure, target_df, climate_dates, hist_lags){
  
  # Extract dates corresponding to the bin and convert to month format
  date_columns <- current_ds[bin]
  date_columns <- as.character(format(as.Date(date_columns), "%Y-%m"))
  
  # Check if columns are in parquet
  date_columns <- intersect(date_columns, climate_dates)
  if (length(date_columns) == 0){return(NA)}
  
  # Extract values for dates within the bin
  date_columns_unique <- unique(date_columns)
  vals <- target_df[, ..date_columns_unique]
  date_vals <- sapply(date_columns_unique, function(d) vals[[d]])
  
  # Extract weights for bin in the current year from weights table
  w_current <- date_weights[date_weights$date %in% date_columns_unique, ]
  w_current <- w_current[match(date_columns_unique, w_current$date), ]
  w_vect <- w_current$weight
  
  # Contemporary measaures
  # --- Contemp. mean
  window_avg <- tryCatch({
    w_avg <- sum(w_vect * date_vals)
  }, error = function(e) {
    # Return NA if there are no dates or weights
    NA
  })
  if (return_measure == "contemp_avg"){return(window_avg)}
  
  # --- Contemp. SD
  window_sd <- tryCatch({
    if (length(date_vals) == 1){w_sd <- 0} else {w_sd <- sd(w_vect * date_vals)}
  }, error = function(e) {
    # Idem
    NA
  })
  if (return_measure == "contemp_sd"){return(window_sd)}
  
  # Historical measures 
  if (hist_lags == 0){
    hist_mean <- NA
    hist_sd <- NA
    if (return_measure == "hist_avg"){return(hist_mean)}
    if (return_measure == "hist_sd"){return(hist_sd)}  
  }
  yearly_vals <- tryCatch({
    lapply(1:nrow(dates_mat), function(i){
      # Obtain window for each historic lag
      m <- dates_mat[i, ]
      
      # Obtain bin dates and convert to month format
      bin_cols <- m[bin]
      bin_cols <- as.character(format(as.Date(bin_cols), "%Y-%m"))
      bin_cols_unique <- unique(bin_cols)
      bin_cols_unique <- intersect(bin_cols_unique, climate_dates)
      if(length(bin_cols_unique) == 0){return(list(mean = NA, sd = NA))}
      
      # Build table of weights for historic bin 
      t_w <- as.data.frame(table(bin_cols)/length(bin_cols))
      names(t_w) <- c("date", "weight")
      
      # Update dictionary if not all dates are in parquet
      t_w <- t_w |> 
        dplyr::filter(date %in% bin_cols_unique) |>
        dplyr::mutate(weight = weight/sum(weight, na.rm = TRUE))
      
      # Extract values for the month-years within the bin of the historic window
      d_vals <- target_df[, ..bin_cols_unique]
      d_vals_vect <- sapply(bin_cols_unique, function(d) d_vals[[d]])
      
      # Make sure table weights only has weights corresponding to the month-years of the historic window
      t_w <- t_w[t_w$date %in% bin_cols_unique, ]
      t_w <- t_w[match(bin_cols_unique, t_w$date), ] # Ensure ordering is the same as in the values vector
      t_w_vect <- t_w$weight
      
      y_mean <- sum(t_w_vect * d_vals_vect)
      y_sd <- ifelse(length(d_vals_vect) == 1, 0, sd(t_w_vect * d_vals_vect))
      list(mean = y_mean, sd = y_sd)
    })
  }, error = function(e) {
    warning(paste("Error calculating yearly and SD means:", e$message))
    rep(NA, nrow(dates_mat))
  })
  
  yearly_mean <- unlist(sapply(1:length(yearly_vals), function(i) yearly_vals[[i]]["mean"]))
  yearly_sd <- unlist(sapply(1:length(yearly_vals), function(i) yearly_vals[[i]]["sd"]))
  
  hist_mean <- mean(yearly_mean, na.rm = TRUE)
  hist_sd <- mean(yearly_sd, na.rm = TRUE)
  if (return_measure == "hist_avg"){return(hist_mean)}
  if (return_measure == "hist_sd"){return(hist_sd)}  
}


#' Function to check for NAs in date sequences for each row
#' 
#' @description
#' This function checks if there are any NAs in the data corresponding to a given date sequence
#' 
#' @param windows_data: A data.table object with data corresponding to a given window sequence. 
#' @param polygon_id: A character string indicating the ID of the geometry. 
#' @param main_dt: The data.table object to subset the sequence data from. 
#' @param metad_cols: A vector with the name of the metadata columns in the parquet file. 
#' @param climate_dates: A vector with the parquet file date columns
#' 
#' @returns: named list if there are any NA values found for a given polygon and date sequences. Otherwise, returns FALSE (no NAs found)
check_na_in_date_sequences <- function(windows_data, polygon_id, main_dt, metad_cols, climate_dates) {
  
  # Get all unique date columns needed across all sequences
  all_date_columns <- unique(
    unlist(
      lapply(windows_data, function(x) {
        c(x$current_ds, as.vector(x$dates_mat))
      })
    )
  )
  
  # Initialize return structure
  polygon_key <- as.character(polygon_id)
  
  # Only keep columns that exist in the climate data
  all_date_columns <- intersect(all_date_columns, climate_dates)
  
  # Filter data for this polygon
  polygon_data <- main_dt[geom_id == polygon_id, .SD, .SDcols = c(all_date_columns, metad_cols)]
  
  if (nrow(polygon_data) == 0) {
    warning(paste("Polygon", polygon_id, "had no data."))
    result_list <- list()
    result_list[[polygon_key]] <- all_date_columns  # All dates are "missing" if no data
    return(result_list)
  }
  
  # Only check date columns (exclude metadata columns from NA check)
  date_columns_in_data <- intersect(all_date_columns, names(polygon_data))
  
  if (length(date_columns_in_data) == 0) {
    return(FALSE)  # No date columns to check
  }
  
  # Check for NAs across all date columns efficiently
  na_vect <- polygon_data[, lapply(.SD, anyNA), .SDcols = date_columns_in_data]
  na_cols <- names(na_vect)[unlist(na_vect)]
  
  # Return results
  if (length(na_cols) > 0) {
    result_list <- list()
    result_list[[polygon_key]] <- na_cols
    return(result_list)
  }
  
  return(FALSE)  # No NAs found
}

#' Function to retrieve the data from the parquet file
#'
#' @param group An integer indicating the group to which a polygon belongs. This number is assigned by the shocks_wrapper function. 
#' @param pol_date_pairs A dataframe with unique geometry id and date pairs and a group id. The geometry IDs must correspond to those
#'                       in the parquet file with climate measures. Dates must be in date format (eg: "YYYY-MM-DD"). Inherited from shocks_wrapper.
#'                       
#' @param geom_id_col: a character string indicating the name of the column in the parquet file that identifies the geometries
#' @param conditions_list A named list of conditions to query the parquet file with climate measures. Inherited from shocks_wrapper.
#' @param start: An integer indicating the offset in days with respect to the date polygon date. Inherited from shocks_wrapper.
#' @param window An integer indicating the length of the data sequence. Inherited from shocks_wrapper.
#' @param time_step A character indicating the product´s resolution
#' @param align: A character string indicating whether the sequence is left- or right-aligned with respect to the polygon date. Inherited from shocks_wrapper.
#'               Left-alignment means that the sequence contains dates AFTER the polygon date. Right-alignment means that the sequence includes dates BEFORE the polygon date. Inherited from shocks_wrapper. 
#' @param hist_lags: An integer indicating the number of years to construct the historical baseline. Required if window_spec is "dynamic" or "both". Inherited from shocks_wrapper.
#' @param window_spec: A character string indicating whether the historical baseline is dynamic, fixed or both. Inherited from shocks_wrapper.
#' @param start_date: A character string in date format ("YYYY-MM-DD") indicating the start date of the historical window. Only required if window_spec= "dynamic" or window_spec="both". Inherited from shocks_wrapper. 
#' @param stop_date: A character string in date format ("YYYY-MM-DD") indicating the end date of the historical window. Only required if window_spec= "dynamic" or window_spec="both". Inherited from shocks_wrapper.
#' @param climate_dates A chector with the parquet file date columns.
#' @param int_threshold A float number indicating the minimum threshold that date sequences must intersect with the date columns in the parquet file to be considered valid. Inherited from shocks_wrapper.
#' @param error_log_path: A character string with the path to the folder where the error logs are stored. Inherited from shocks_wrapper.
#'
#'
#' @returns a data.table object with selected date columns (as defined in the windows_seq function) and polygons.
#' @export
get_data <- function(
    group, 
    pol_date_pairs, 
    geom_id_col, 
    conditions_list, 
    start, 
    window, 
    time_step, 
    align, 
    hist_lags, 
    window_spec, 
    start_date, 
    stop_date, 
    climate_dates, 
    int_threshold, 
    error_log_path
){
  
  # --- Vector of unique interview days
  unique_dates <- unique(pol_date_pairs$date)
  
  # --- Extract the vector of date columns if the percentage of intersection exceeds the specified threshold
  dates_checks <- lapply(
    unique_dates, 
    function(u_date){
      
      # Get window sequence
      u_date <- as.character(u_date)
      check_window <- window_seq(d = u_date, start = start, window = window, align = align, hist_lags = hist_lags, 
                                 window_spec = window_spec, start_date = start_date, stop_date = stop_date, only_hist = FALSE)
      
      # Check the percentage of the window sequence that falls within the products range
      if (time_step == "daily"){
        dates_int <- intersect(check_window, climate_dates)
        pct_int <- length(unique(dates_int))/length(unique(check_window))
      }
      if (time_step == "monthly"){
        dates_int <- intersect(as.character(format(as.Date(check_window), "%Y-%m")), climate_dates)
        pct_int <- length(unique(dates_int))/length(unique(as.character(format(as.Date(check_window), "%Y-%m"))))
      }
      
      # Initialize result lists
      error_list <- list()
      success_list <- list()
      
      # Exclude sequences with a pct of intersection lower than specified threshold
      if (pct_int < int_threshold){ 
        problematic_pols <- unique(pol_date_pairs[pol_date_pairs$date == u_date, ]$geom_id)
        error_list[[u_date]] <- list(polygons = problematic_pols, intersection_pct = 100 * pct_int)  # Identify problematic polygons
      } else {
        success_list[[u_date]] <- dates_int  # Return only the intersection of dates
      }
      
      return(list(success = success_list, errors = error_list))
    }
  )
  
  # Extract successful dates
  dates_cols <- unique(
    unlist(
      as.vector(
        sapply(
          dates_checks, 
          function(x){return(x[["success"]])}
        )
      )
    )
  )
  
  # Extract errors
  errors <- as.vector(
    sapply(
      dates_checks, 
      function(x){return(x[["errors"]])}
    )
  )
  
  errors <-  Filter(function(x){if (length(x) == 0){return(FALSE)} else {return(TRUE)}}, errors)
  
  
  if (length(errors) != 0){
    cat("\nGroup", group, "\n")
    cat("\n WARNING:\n some dates sequences do not reach the minimum intersection threshold of", 100 * int_threshold ,"% with the product´s range. Check the error log list to inspect which sequences and polygons are dropped.\n \n")
    saveRDS(errors, file = file.path(error_log_path, paste0("intermediate/", "missing_cols_error_log_", group, "_" , window_spec, ".RDS")))
  }
  
  # Stop if no dates can be processed
  if(is.null(dates_cols)){
    saveRDS(errors, file = file.path(error_log_path, paste0("intermediate/", "missing_cols_error_log_", group, "_", window_spec, ".RDS")))
    stop("All dates are outside of the product´s range using the current threshold!")
  }
  
  if (time_step == "monthly"){
    dates_cols <- as.character(format(as.Date(dates_cols), "%Y-%m"))
  } else if (time_step == "yearly"){
    dates_cols <- as.character(format(as.Date(dates_cols), "%Y"))
  }
  
  # Extract path from conditions list
  data_path <- conditions_list[["data_path"]]
  
  # Copy conditions list
  conditions_copy <- conditions_list
  
  # Drop elements to prevent errors when querying the dataset
  conditions_copy[["data_path"]] <- NULL
  conditions_copy[["product_temp_res"]] <- NULL
  
  
  # Create a vector of the expressions of the conditions based on the named conditions list
  filter_expressions <- purrr::map2(
    names(conditions_copy),
    conditions_copy, 
    ~{rlang::expr(!!rlang::sym(.x) %in% !!.y)}
  )
  
  # Combine conditions
  combined_conditions <- purrr::reduce(filter_expressions, function(x, y){rlang::expr(!!x & !!y)})
  
  # Scan the parquet file
  parquet_scan <- arrow::open_dataset(data_path)
  if (!geom_id_col %in% colnames(parquet_scan)){stop("Error: the geom_id column is not present in the parquet file. Please check.")}
  

  # Return selected rows and columns based on conditions
  cols_to_load <- names(conditions_copy)
  if ((!"trans_type" %in% cols_to_load) && ("trans_type" %in% colnames(parquet_scan))){cols_to_load <- c(cols_to_load, "trans_type")}
  if ((!"trans_var" %in% cols_to_load) && ("trans_var" %in% colnames(parquet_scan))){cols_to_load <- c(cols_to_load, "trans_var")}
  data_main <- parquet_scan |>
    dplyr::rename(geom_id = !!rlang::sym(geom_id_col)) |>
    dplyr::select(
      dplyr::any_of(
        c(
          cols_to_load, 
          dates_cols
        )
      )
    ) |>
    dplyr::filter(!!combined_conditions) |>
    dplyr::collect()
  
  # Make sure data isdata.table
  data.table::setDT(data_main)
  
  # Return filtered data
  return(data_main)
}

