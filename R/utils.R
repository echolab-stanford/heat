#' @importFrom lubridate ymd ym ymd_hm year month day hour minute
#' @importFrom data.table is.data.table as.data.table data.table setnames setkeyv setcolorder setorderv melt
NULL

#' Get Date Columns from Data
#'
#' @description
#'   This function scans a data.table or data.frame and identifies which columns
#'   have names that represent dates. Date columns are detected based on whether their names can be
#'   parsed as dates in one of the formats "YYYY-MM-DD", "YYYY-MM", or "YYYY". The function returns
#'   a character vector of the names of these date columns.
#'
#' @param:
#'   - dt: A data.table or data.frame whose column names may include dates.
#'
#' @return:
#'   - A character vector containing the names of the columns that are recognized as date columns.
#'
#' @examples
#' \dontrun{
#'   # Suppose dt is a data.table with columns "geom_id", "trans_var", "2017-01-01", "2017-01-02", ...
#'   date_cols <- get_date_cols(dt)
#' }
get_date_cols <- function(dt) {
  # Get all column names.
  col_names <- names(dt)
  
  # Identify date columns robustly from the column names.
  # This selects columns that can be parsed as "YYYY-MM-DD", "YYYY-MM", or "YYYY".
  date_cols <- col_names[
    !is.na(lubridate::ymd_hm(col_names, quiet = TRUE)) |
    !is.na(lubridate::ymd(col_names, quiet = TRUE)) | 
      !is.na(lubridate::ym(col_names, quiet = TRUE)) |
      grepl("^\\d{4}$", col_names)
  ]
  
  return(date_cols)
}


#' Get Temporal Resolution from Date Column Names
#'
#' @description
#'   This function determines the temporal resolution of a set of date column names. It expects a
#'   character vector of date column names, each formatted as "YYYY", "YYYY-MM", or "YYYY-MM-DD".
#'   The function first checks that all date column names share the same number of characters.
#'   If they do, it returns the temporal resolution in the format "yearly", "monthly", or "daily".
#'   If not, it stops with an error.
#'
#' @param:
#'   - date_cols: A character vector of date column names.
#'
#' @return:
#'   - A character string representing the temporal resolution: "yearly", "monthly", or "daily".
#'
#' @examples
#' \dontrun{
#'   # Given a vector of date column names:
#'   date_cols <- c("2017", "2018", "2019")
#'   res <- get_temp_res(date_cols)  # returns "yearly"
#'
#'   # For monthly resolution:
#'   date_cols <- c("2017-01", "2017-02", "2017-03")
#'   res <- get_temp_res(date_cols)  # returns "monthly"
#' }
get_temp_res <- function(date_cols) {
  # Check that all date column names have the same number of characters.
  char_lengths <- sapply(date_cols, nchar)
  if (length(unique(char_lengths)) != 1) {
    stop("Error: Multiple temporal resolutions detected among date columns. ",
         "All date columns must have the same format (e.g., all 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', or 'YYYY-MM-DD HH:MM').")
  }
  
  # Determine the temporal resolution based on the length of the date strings.
  unique_length <- unique(char_lengths)
  if (unique_length == 4) {
    resolution <- "yearly"
  } else if (unique_length == 7) {
    resolution <- "monthly"
  } else if (unique_length == 10) {
    resolution <- "daily"
  } else if (unique_length == 16) {
    resolution <- "hourly"
  } else {
    stop("Error: Unrecognized date format. Expected formats: 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', or 'YYYY-MM-DD HH:MM'.")
  }
  return(resolution)
}







add_time_cols <- function(dt, date_col = "date", temp_res = "daily",
                          id_vars = c("geom_id", "trans_type"), verbose = 1) {
  # Ensure dt is a data.table.
  if (!data.table::is.data.table(dt)) {
    dt <- data.table::as.data.table(dt)
  }
  
  if (temp_res == "yearly") {
    if (verbose >= 2) {
      message("Detected temporal resolution: 'yearly'. Renaming '", date_col, "' column to 'year'.")
    }
    data.table::setnames(dt, date_col, "year")
    # Order by the identifier columns and year.
    data.table::setorderv(dt, c(id_vars, "year"))
    
  } else if (temp_res == "monthly") {
    if (verbose >= 2) {
      message("Detected temporal resolution: 'monthly'. Creating 'year' and 'month' columns.")
    }
    dt[, year := as.integer(substr(get(date_col), 1, 4))]
    dt[, month := as.integer(substr(get(date_col), 6, 7))]
    # Reorder columns: id_vars, then date_col, year, month, then the rest.
    new_order <- c(id_vars, date_col, "year", "month",
                   setdiff(names(dt), c(id_vars, date_col, "year", "month")))
    data.table::setcolorder(dt, new_order)
    # Order by id_vars, year, and month.
    data.table::setorderv(dt, c(id_vars, "year", "month"))
    
  } else if (temp_res == "daily") {
    if (verbose >= 2) {
      message("Detected temporal resolution: 'daily'. Creating 'year', 'month', and 'day' columns.")
    }
    # Build a lookup table to compute year, month, day once per unique date.
    unique_dates <- unique(dt[[date_col]])
    lookup_dt <- data.table::data.table(date = unique_dates)
    lookup_dt[, ymd_date := lubridate::ymd(date)]
    lookup_dt[, year := lubridate::year(ymd_date)]
    lookup_dt[, month := lubridate::month(ymd_date)]
    lookup_dt[, day := lubridate::day(ymd_date)]
    lookup_dt[, ymd_date := NULL]
    data.table::setkey(lookup_dt, date)
    
    # Set key on the main data.table by the date column.
    data.table::setkeyv(dt, date_col)
    # Join the lookup table back to the main data.table.
    dt <- lookup_dt[dt]
    
    # Reorder columns: id_vars, then date_col, year, month, day, then the rest.
    new_order <- c(id_vars, date_col, "year", "month", "day",
                   setdiff(names(dt), c(id_vars, date_col, "year", "month", "day")))
    data.table::setcolorder(dt, new_order)
    # Order by id_vars, year, month, and day.
    data.table::setorderv(dt, c(id_vars, "year", "month", "day"))
    
  } else if (temp_res == "hourly") {
    if (verbose >= 2) {
      message("Detected temporal resolution: 'hourly'. Creating 'year', 'month', 'day', and 'hour' columns.")
    }
    
    # Parse the date column to POSIXct
    dt[, parsed_date := lubridate::ymd_hm(get(date_col))]
    
    dt[, year := year(parsed_date)]
    dt[, month := month(parsed_date)]
    dt[, day := day(parsed_date)]
    dt[, hour := hour(parsed_date)]
    dt[, minute := minute(parsed_date)]
    
    # Drop the parsed_date as it won't be needed anymore
    dt[, parsed_date := NULL]
    
    # Reorder columns
    new_order <- c(id_vars, date_col, "year", "month", "day", "hour", "minute",
                   setdiff(names(dt), c(id_vars, date_col, "year", "month", "day", "hour", "minute")))
    data.table::setcolorder(dt, new_order)
    # Order by id_vars, year, month, day, hour, minute
    data.table::setorderv(dt, c(id_vars, "year", "month", "day", "hour", "minute"))
    # Drop the minute column if all values are zero
    if (all(dt$minute == 0)) {dt[, minute := NULL]}
    
  } else {
    stop("Error: Unrecognized temporal resolution. Expected 'yearly', 'monthly', 'daily', or 'hourly'.")
  }
  
  return(dt)
}



#' Reshape Wide Data to Long Format by Processing Each trans_var Separately with Optional Time Columns
#'
#' @description
#'   This function reshapes a wide-format data.table (or data.frame) into a long-format data.table
#'   by processing each unique trans_var separately and optimizing the merge operation using keys.
#'   It first identifies the date columns and their temporal resolution using the get_date_cols and get_temp_res functions.
#'   For each unique value in the trans_var column, it subsets the data, melts the subset using the identifier
#'   columns, and renames the resulting value column to the corresponding trans_var value. The melted columns are then
#'   merged sequentially into a long-format data.table using keyed joins.
#'
#'   Optionally, time columns (year, month, day) are added based on the detected temporal resolution.
#'
#' @param:
#'   - wide_dt: A wide-format data.table or data.frame containing identifier columns, a trans_var column,
#'              date columns, and possibly other columns.
#'   - id_cols: A character vector of identifier column names that must always be retained.
#'              Default is c("geom_id", "trans_type").
#'   - trans_var: A character string specifying the column name that holds the variable used for subsetting.
#'                Default is "trans_var".
#'   - add_time_columns: Logical. If TRUE (default), additional time columns are added based on the detected temporal resolution.
#'
#' @return:
#'   - A long-format data.table that contains the identifier columns, a "date" column, one column per unique trans_var
#'     containing the melted data, and optionally additional time columns.
#'
#' @examples
#' \dontrun{
#'   # Suppose wide_dt is a data.table with columns "geom_id", "trans_type", "trans_var",
#'   # and date columns like "2017-01-01", "2017-01-02", etc.
#'   long_dt <- reshape_to_long(wide_dt)
#' }
#' 
#' @export
reshape_to_long <- function(wide_dt, 
                            id_cols = c("geom_id", "trans_type"), 
                            trans_var = "trans_var",
                            add_time_columns = TRUE,
                            verbose = 1) {
  
  reshape_start <- Sys.time()
  
  # Ensure wide_dt is a data.table.
  if (!data.table::is.data.table(wide_dt)) {
    wide_dt <- data.table::as.data.table(wide_dt)
  }
  
  # Get date columns and temporal resolution.
  date_cols <- get_date_cols(wide_dt)
  temp_res <- get_temp_res(date_cols)
  
  # Get all column names.
  col_names <- names(wide_dt)
  
  # Keep only id_cols, trans_var, and date columns.
  wide_dt <- wide_dt[, c(id_cols, trans_var, date_cols), with = FALSE]
  id_vars_final <- id_cols
  
  # Initialize an empty data.table to accumulate the melted results.
  long_dt <- data.table::data.table()
  
  # Define the merge key columns.
  merge_by <- c(id_vars_final, "date")
  
  # Get the unique trans_var values.
  unique_trans <- unique(wide_dt[[trans_var]])
  
  # Initialize progress bar if verbose >= 1
  pb <- NULL
  if (verbose >= 1 && length(unique_trans) > 1) {
    pb <- tryCatch({
      progress::progress_bar$new(
        format = "  [:bar] :percent | Variable :current/:total | ETA: :eta",
        total = length(unique_trans),
        clear = FALSE,
        force = TRUE,
        show_after = 0,
        width = 80
      )
    }, error = function(e) {
      NULL
    })
  }
  
  # Loop over each unique trans_var value.
  for (idx in seq_along(unique_trans)) {
    i <- unique_trans[idx]
    val_name <- as.character(i)
    
    # Update progress bar
    if (!is.null(pb)) {
      pb$tick()
    }
    
    # Subset the data.table to rows corresponding to the current trans_var.
    sub_dt <- wide_dt[ trans_var == i, ]
    
    # Melt the subset:
    # - id.vars: identifier columns (id_vars_final)
    # - measure.vars: date columns (date_cols)
    # - variable.name: "date"
    # - value.name: set to the current trans_var value (val_name)
    melted_sub <- data.table::melt(sub_dt, 
                                   id.vars = id_vars_final, 
                                   measure.vars = date_cols, 
                                   variable.name = "date", 
                                   value.name = val_name)
    
    # Set keys for faster merging.
    data.table::setkeyv(melted_sub, merge_by)
    
    # Merge the new melted column into the long_dt data.table.
    if (nrow(long_dt) == 0) {
      long_dt <- melted_sub
      data.table::setkeyv(long_dt, merge_by)
    } else {
      long_dt <- merge(long_dt, 
                       melted_sub[, c(merge_by, val_name), with = FALSE], 
                       by = merge_by, 
                       all = TRUE, 
                       sort = FALSE)
      data.table::setkeyv(long_dt, merge_by)
    }
  }
  
  # Optionally add time columns based on temporal resolution.
  if (add_time_columns) {
    long_dt <- add_time_cols(long_dt, date_col = "date", temp_res = temp_res, id_vars = id_vars_final, verbose = verbose)
  }
  
  reshape_end <- Sys.time()
  if (verbose >= 2) {
    message("Reshaping to long format completed in ", 
            round(difftime(reshape_end, reshape_start, units = "secs"), 2), " seconds.")
  }
  
  return(long_dt)
}


#' Get Transformation Variable Columns
#'
#' @description
#'   Identifies transformation variable columns in a data frame. Throws error if multiple types found.
#'   Warns if none found.
#'
#' @param df A data frame to search for transformation variable columns
#'
#' @return A character vector of transformation variable column names
#'
#' @examples
#' \dontrun{
#'   trans_cols <- get_trans_var_cols(my_data)
#' }
get_trans_var_cols <- function(df) {
  col_names <- names(df)
  
  # Find each transformation type
  patterns <- list(
    polynomial = "^degree_\\d+$",
    spline = "^term_\\d+$", 
    bin = "^bin_.*_to_.*$",
    none = "^value$"
  )
  
  found_types <- sapply(patterns, function(p) any(grepl(p, col_names)))
  
  # Error if multiple types found
  if (sum(found_types) > 1) {
    stop("Multiple transformation types found: ", paste(names(found_types)[found_types], collapse = ", "))
  }
  
  # Warning if none found
  if (sum(found_types) == 0) {
    warning("No transformation variable columns found in dataset")
    return(character(0))
  }
  
  # Return matching columns
  pattern <- patterns[[which(found_types)]]
  return(col_names[grepl(pattern, col_names)])
}
