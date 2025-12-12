# ============================================================================
# Helper Functions for Validation
# ============================================================================

#' Process Data for Validation Checks
#'
#' @keywords internal
process_data <- function(dataframe, input_temporal_resolution, output_temporal_resolution = NULL,
                         filter_start_date = NULL, filter_end_date = NULL,
                         filter_geom_ids = NULL, average_over_time = FALSE,
                         average_over_geom_ids = FALSE, value_columns) {
  
  if (is.null(output_temporal_resolution)) {
    output_temporal_resolution <- input_temporal_resolution
  }
  
  if (input_temporal_resolution == "monthly" && output_temporal_resolution == "daily") {
    stop("Error: Cannot set `output_temporal_resolution` to 'daily' when `input_temporal_resolution` is 'monthly'.")
  }
  
  dataframe <- dataframe |>
    dplyr::mutate(date = if (input_temporal_resolution == "yearly") paste0(year, "-07-01") else date) |>
    dplyr::mutate(date = if (input_temporal_resolution == "monthly") lubridate::ym(date) else lubridate::ymd(date))
  
  if (is.character(value_columns)) {
    value_columns <- as.character(value_columns)
  } else if (is.list(value_columns)) {
    value_columns <- unlist(value_columns)
  }
  
  if (!is.null(filter_start_date) && !is.null(filter_end_date)) {
    filter_start_date <- if (output_temporal_resolution == "monthly") lubridate::ym(filter_start_date) else lubridate::ymd(filter_start_date)
    filter_end_date <- if (output_temporal_resolution == "monthly") lubridate::ym(filter_end_date) else lubridate::ymd(filter_end_date)
    dataframe <- dataframe |> dplyr::filter(date >= filter_start_date & date <= filter_end_date)
  }
  
  if (!is.null(filter_geom_ids)) {
    dataframe <- dataframe |> dplyr::filter(geom_id %in% filter_geom_ids)
  }
  
  if (input_temporal_resolution == "daily" && output_temporal_resolution == "monthly") {
    dataframe <- dataframe |>
      dplyr::group_by(geom_id, year = lubridate::year(date), month = lubridate::month(date)) |>
      dplyr::summarize(dplyr::across(dplyr::all_of(value_columns), ~ mean(.x, na.rm = TRUE)), .groups = 'drop') |>
      dplyr::mutate(date = lubridate::make_date(year, month, 1))
  }
  
  dataframe <- dataframe |>
    dplyr::mutate(date = if (output_temporal_resolution == "monthly") format(lubridate::make_date(year, month), "%Y-%m") else date)
  
  if (average_over_time & average_over_geom_ids) {
    dataframe <- dataframe |>
      dplyr::summarize(dplyr::across(dplyr::all_of(value_columns), ~ mean(.x, na.rm = TRUE)), .groups = 'drop')
  } else if (average_over_time) {
    dataframe <- dataframe |>
      dplyr::group_by(geom_id) |>
      dplyr::summarize(dplyr::across(dplyr::all_of(value_columns), ~ mean(.x, na.rm = TRUE)),
                       NA_count = sum(is.na(.data[[value_columns[1]]])),
                       .groups = 'drop')
  } else if (average_over_geom_ids) {
    dataframe <- dataframe |>
      dplyr::group_by(date) |>
      dplyr::summarize(dplyr::across(dplyr::all_of(value_columns), ~ mean(.x, na.rm = TRUE)), .groups = 'drop')
  }
  
  return(dataframe)
}

#' Plot Spatial Averages
#'
#' @keywords internal
plot_spatial_averages <- function(data, target_geometry, title, fill_variable, fill_label, 
                                  x_label = "Longitude", y_label = "Latitude") {
  
  merged_data <- data |>
    dplyr::left_join(target_geometry |> dplyr::select(geom_id, geometry), by = "geom_id") |>
    sf::st_as_sf()
  
  is_points <- any(grepl("POINT", sf::st_geometry_type(merged_data)))
  
  if (is_points) {
    bbox <- sf::st_bbox(merged_data)
    extent_range <- max(bbox["xmax"] - bbox["xmin"], bbox["ymax"] - bbox["ymin"])
    n_points <- nrow(merged_data)
    point_size <- max(0.5, min(3, 50 / (sqrt(extent_range) * log10(n_points + 10))))
    
    ggplot2::ggplot(merged_data) +
      ggplot2::geom_sf(ggplot2::aes(color = !!rlang::sym(fill_variable)), size = point_size, alpha = 0.8) +
      ggplot2::scale_color_viridis_c() +
      ggplot2::labs(title = title, color = fill_label, x = x_label, y = y_label) +
      ggplot2::theme_minimal()
  } else {
    ggplot2::ggplot(merged_data) +
      ggplot2::geom_sf(ggplot2::aes(fill = !!rlang::sym(fill_variable)), 
                       color = "gray30", linewidth = 0.1) +
      ggplot2::scale_fill_viridis_c() +
      ggplot2::labs(title = title, fill = fill_label, x = x_label, y = y_label) +
      ggplot2::theme_minimal()
  }
}

#' Plot Time Series
#'
#' @keywords internal
plot_time_series <- function(temporal_resolution, first_df, second_df = NULL, third_df = NULL, fourth_df = NULL,
                             value_variable = "environmental_variable",
                             value_label = "Environmental Variable",
                             first_title = "Dataset 1", second_title = "Dataset 2",
                             third_title = "Dataset 3", fourth_title = "Dataset 4") {
  
  dataframes <- list(first_df, second_df, third_df, fourth_df)
  titles <- c(first_title, second_title, third_title, fourth_title)
  
  dataframes <- dataframes[!sapply(dataframes, is.null)]
  titles <- titles[!sapply(dataframes, is.null)]
  
  combined_data <- dplyr::bind_rows(lapply(seq_along(dataframes), function(i) {
    data <- dataframes[[i]]
    data <- data |>
      dplyr::mutate(
        facet_label = factor(titles[i], levels = titles),
        date_parsed = if (temporal_resolution == "monthly") lubridate::ym(date) else lubridate::ymd(date)
      ) |>
      dplyr::mutate(
        year = lubridate::year(date_parsed),
        month_day = if (temporal_resolution == "daily") format(date_parsed, "%m-%d") else factor(format(date_parsed, "%b")),
        month_day_n = if (temporal_resolution == "daily") as.numeric(format(date_parsed, "%j")) else as.numeric(format(date_parsed, "%m"))
      ) |> dplyr::arrange(year, month_day_n)
    data
  }))
  
  combined_data <- combined_data |>
    dplyr::arrange(year) |>
    dplyr::mutate(
      color_year = as.numeric(factor(year, levels = unique(year))),
      color_value = scales::rescale(color_year, to = c(0, 1))
    )
  
  combined_data <- combined_data |>
    dplyr::mutate(month_day = forcats::fct_reorder(month_day, month_day_n))
  
  year_range <- range(combined_data$year, na.rm = TRUE)
  
  if (length(unique(combined_data$year)) == 1) {
    color_gradient <- ggplot2::scale_color_gradient(
      low = "blue", high = "red",
      breaks = c(0),
      labels = year_range[1],
      guide = ggplot2::guide_colorbar(title = "Year")
    )
  } else {
    color_gradient <- ggplot2::scale_color_gradient(
      low = "blue", high = "red",
      breaks = c(0, 1),
      labels = year_range,
      guide = ggplot2::guide_colorbar(title = "Year")
    )
  }
  
  x_axis_labels <- if (temporal_resolution == "daily") "Day of Year" else "Month"
  
  combined_data <- combined_data |>
    dplyr::mutate(month_day = factor(month_day, levels = sort(unique(month_day))))
  
  plot <- ggplot2::ggplot(combined_data, ggplot2::aes(x = month_day, y = .data[[value_variable]], 
                                                       group = year, color = color_value)) +
    ggplot2::geom_line(linewidth = 0.3, alpha = 0.8) +
    color_gradient +
    ggplot2::labs(
      title = paste("Time Series of", value_label),
      x = x_axis_labels, y = value_label
    ) +
    ggplot2::facet_wrap(~ facet_label, scales = "fixed") +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.position = "bottom")
  
  if (temporal_resolution == "daily") {
    plot <- plot +
      ggplot2::scale_x_discrete(breaks = c("01-01", "04-01", "07-01", "10-01"), 
                                labels = c("Jan", "Apr", "Jul", "Oct"))
  } else {
    plot <- plot +
      ggplot2::scale_x_discrete(breaks = levels(combined_data$month_day)[seq(1, 12, by = 3)])
  }
  
  plot
}

#' Calculate Summary Statistics
#'
#' @keywords internal
calculate_summary_statistics <- function(dataframe, variables) {
  
  dataframe <- dplyr::as_tibble(dataframe)
  
  missing_vars <- setdiff(variables, colnames(dataframe))
  if (length(missing_vars) > 0) {
    stop(paste("The following variables are not found in the dataframe:", paste(missing_vars, collapse = ", ")))
  }
  
  summary_list <- lapply(variables, function(var) {
    x <- dataframe[[var]]
    data.frame(
      var_name = var,
      mean = mean(x, na.rm = TRUE),
      median = median(x, na.rm = TRUE),
      sd = sd(x, na.rm = TRUE),
      min = min(x, na.rm = TRUE),
      max = max(x, na.rm = TRUE),
      n = sum(!is.na(x)),
      missing = sum(is.na(x)),
      stringsAsFactors = FALSE
    )
  })
  
  summary_statistics_df <- do.call(rbind, summary_list)
  rownames(summary_statistics_df) <- NULL
  
  return(summary_statistics_df)
}

#' Plot Summary Ridge Plot
#'
#' @keywords internal
plot_summary_ridge <- function(data, temporal_resolution, fill_variable, environmental_variable_description) {
  
  if (!requireNamespace("ggridges", quietly = TRUE)) {
    stop("Package 'ggridges' is required for ridge plots")
  }
  
  data <- data |>
    dplyr::mutate(date = if (temporal_resolution == "monthly") lubridate::ym(date) else lubridate::ymd(date))
  
  data_processed <- data |>
    dplyr::mutate(
      month = factor(format(date, "%b")),
      month_n = as.numeric(lubridate::month(date))
    ) |>
    dplyr::group_by(month, month_n) |>
    dplyr::mutate(mean_env_var = mean(.data[[fill_variable]], na.rm = TRUE)) |>
    dplyr::ungroup() |>
    tidyr::complete(month = factor(month)) |>
    dplyr::mutate(month = forcats::fct_reorder(month, month_n))
  
  suppressMessages(
    ggplot2::ggplot(data_processed, ggplot2::aes(x = .data[[fill_variable]], y = month, fill = mean_env_var)) +
      ggridges::geom_density_ridges(alpha = 0.85, scale = 3, na.rm = TRUE) +
      ggplot2::geom_segment(data = data_processed |> dplyr::distinct(month, mean_env_var),
                            ggplot2::aes(x = mean_env_var, xend = mean_env_var,
                                         y = as.numeric(month), yend = as.numeric(month) + 0.2),
                            color = "black", linewidth = 0.3, alpha = 0.6) +
      ggplot2::scale_fill_viridis_c(option = "viridis", na.value = "grey80") +
      ggplot2::labs(
        title = paste("Distribution of", environmental_variable_description, "by Month"),
        x = environmental_variable_description,
        y = "Month",
        fill = "Monthly Average"
      ) +
      ggplot2::theme_minimal() +
      ggplot2::theme(legend.position = "right")
  )
}

#' Plot Overall Density
#'
#' @keywords internal
plot_overall_density <- function(data) {
  
  group_colors <- viridis::viridis(3)
  names(group_colors) <- c("Lowest 5%", "Middle 90%", "Highest 5%")
  
  ggplot2::ggplot(data, ggplot2::aes(x = cell_count, fill = group)) +
    ggplot2::geom_density(alpha = 0.8, adjust = 0.1) +
    ggplot2::scale_fill_manual(values = group_colors) +
    ggplot2::labs(title = "Grid Cell Counts per Geometry",
                  x = "Grid Cell Count",
                  y = "Density",
                  fill = "Group") +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.position = "bottom")
}

#' Plot Density by Group
#'
#' @keywords internal
plot_density_by_group <- function(data, group_name) {
  
  group_colors <- viridis::viridis(3)
  names(group_colors) <- c("Lowest 5%", "Middle 90%", "Highest 5%")
  
  limit_values <- data |>
    dplyr::filter(group == group_name) |>
    dplyr::summarise(min_count = min(cell_count), max_count = max(cell_count))
  
  ggplot2::ggplot(dplyr::filter(data, group == group_name), ggplot2::aes(x = cell_count)) +
    ggplot2::geom_density(alpha = 0.8, fill = group_colors[group_name], adjust = 0.1) +
    ggplot2::labs(title = paste("Density of Grid Cell Counts -", group_name),
                  x = "Grid Cell Count",
                  y = "Density") +
    ggplot2::theme_minimal() +
    ggplot2::annotate("text", x = limit_values$min_count, y = 0, 
                      label = paste("Min:", limit_values$min_count), vjust = 1.5, hjust = -0.1) +
    ggplot2::annotate("text", x = limit_values$max_count, y = 0, 
                      label = paste("Max:", limit_values$max_count), vjust = 1.5, hjust = 1.1)
}

#' Transform Binned Output
#'
#' @keywords internal
transform_binned_output <- function(data, temporal_resolution) {
  
  if (any(data$trans_type != "bin")) {
    stop(paste0("Error: function expects trans_type = 'bin', trans_type provided: ", unique(data$trans_type)))
  }
  
  bin_vars <- names(data)[startsWith(names(data), "bin_")]
  
  data <- data |>
    dplyr::mutate(sum_of_bins = rowSums(dplyr::select(., dplyr::all_of(bin_vars)), na.rm = TRUE))
  
  data <- data |> 
    dplyr::mutate(expected_sum = if (temporal_resolution == "daily") 1 else lubridate::days_in_month(lubridate::make_date(year, month, 1)))
  
  return(data)
}

#' Plot Bin Proportion
#'
#' @keywords internal
plot_bin_proportion <- function(data, temporal_resolution, geom_id_col = "geom_id") {
  
  bin_vars <- names(data)[startsWith(names(data), "bin_")]
  
  data <- data |>
    dplyr::mutate(
      date = if (temporal_resolution == "monthly") format(lubridate::make_date(year, month), "%Y-%m") else lubridate::make_date(year, month, day),
      time_step = if (temporal_resolution == "monthly") lubridate::month(date) else lubridate::yday(date)
    ) |>
    dplyr::group_by(time_step) |>
    dplyr::summarize(dplyr::across(dplyr::all_of(bin_vars), ~ mean(.x, na.rm = TRUE)), .groups = "drop")
  
  data_long <- data |>
    tidyr::pivot_longer(cols = dplyr::all_of(bin_vars), names_to = "bin", values_to = "proportion") |>
    dplyr::mutate(bin = factor(bin, levels = bin_vars))
  
  x_breaks <- if (temporal_resolution == "daily") {
    seq(1, 365, by = 30.44)
  } else {
    1:12
  }
  
  x_labels <- month.abb
  
  ggplot2::ggplot(data_long, ggplot2::aes(x = time_step, y = proportion, fill = bin)) +
    ggplot2::geom_area(position = "stack") +
    ggplot2::scale_fill_viridis_d() +
    ggplot2::scale_x_continuous(breaks = x_breaks, labels = x_labels) +
    ggplot2::labs(
      title = "Proportion of Bin Variables averaged over years",
      x = "Month",
      y = "Proportion",
      fill = "Bin"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.position = "bottom")
}

# ============================================================================
# Main Validation Function
# ============================================================================

#' Validate R2E2 Pipeline Output
#'
#' Runs quality control checks on the output of the r2e2 pipeline.
#' Displays validation plots in the viewer and optionally saves them to disk.
#'
#' @param results Optional list output from r2e2() containing spatial_agg, temp_agg_long, area_weights, etc.
#'                If provided, the function will automatically select the best data for validation.
#' @param df_long Data frame in long format (output from r2e2). If results is provided, this is ignored.
#' @param geometry sf object containing geometries with geom_id column
#' @param area_weights Data frame with area weights (including geom_id, x, y columns); required for some checks.
#'                     If results is provided, this is extracted automatically.
#' @param validation_var Character string specifying which transformed variable to validate (e.g., "degree_1")
#' @param validation_var_name Character string describing the variable for plot labels (e.g., "Temperature (°C)")
#' @param save_path Optional path to save validation outputs. If NULL (default), plots are only displayed in viewer. If provided, creates a 'validation' subfolder
#' @param spatial_averages Logical; run spatial averages check (default TRUE)
#' @param time_series Logical; run time series check (default TRUE)
#' @param summary_stats Logical; run summary statistics check (default TRUE)
#' @param grid_cell_alignment Logical; run grid cell alignment check (default TRUE)
#' @param cell_count_per_geometry Logical; run cell count per geometry check (default TRUE)
#' @param cell_count_per_geometry_detailed Logical; show detailed plots by group (default FALSE)
#' @param binned_output Logical; run binned output check (default TRUE for bin transformations)
#' @param verbose Integer controlling message verbosity: 0 = silent, 1 = concise progress messages, 2 = detailed (default: 1)
#'
#' @return Invisibly returns a list of validation results
#' @export
validate_r2e2 <- function(results = NULL,
                          df_long = NULL,
                          geometry = NULL,
                          area_weights = NULL,
                          validation_var = NULL,
                          validation_var_name = NULL,
                          save_path = NULL,
                          spatial_averages = TRUE,
                          time_series = TRUE,
                          summary_stats = TRUE,
                          grid_cell_alignment = TRUE,
                          cell_count_per_geometry = TRUE,
                          cell_count_per_geometry_detailed = FALSE,
                          binned_output = NULL,
                          verbose = 1) {
  
  # If results list is provided, extract components
  if (!is.null(results)) {
    # Search for any _long or _wide data in results (e.g., monthly_long, daily_wide)
    if (is.null(df_long)) {
      # Find all available _long and _wide format data
      long_names <- grep("_long$", names(results), value = TRUE)
      wide_names <- grep("_wide$", names(results), value = TRUE)
      
      # Define temporal resolution hierarchy (coarsest to finest)
      resolution_order <- c("yearly", "monthly", "daily", "hourly", "subdaily")
      
      # Function to select highest level (coarsest) resolution
      select_highest_resolution <- function(names_vec) {
        for (res in resolution_order) {
          matching <- grep(paste0("^", res, "_"), names_vec, value = TRUE)
          if (length(matching) > 0) return(matching[1])
        }
        return(names_vec[1])  # Fallback to first if no match
      }
      
      if (length(long_names) > 0) {
        # Prefer highest (coarsest) resolution _long format
        selected_name <- select_highest_resolution(long_names)
        df_long <- results[[selected_name]]
      } else if (length(wide_names) > 0) {
        # If no _long format, reshape highest resolution _wide format
        selected_name <- select_highest_resolution(wide_names)
        df_long <- reshape_to_long(results[[selected_name]], add_time_columns = TRUE, verbose = 0)
      } else {
        stop("No suitable data found in results list for validation. Expected data with names ending in '_long' or '_wide'.")
      }
    }
    
    # Extract area_weights if not provided
    if (is.null(area_weights) && !is.null(results$area_weights)) {
      area_weights <- results$area_weights
    }
  }
  
  # Validate required parameters
  if (is.null(df_long)) {
    stop("Either 'results' or 'df_long' must be provided")
  }
  if (is.null(geometry)) {
    stop("'geometry' parameter is required")
  }
  
  # Auto-detect binned_output if not specified
  if (is.null(binned_output)) {
    binned_output <- any(df_long$trans_type == "bin")
  }
  
  # Check and optionally install core required packages (excluding check-specific packages)
  required_packages <- c("ggplot2", "dplyr", "sf", "ggrepel", "lubridate", "tidyr", 
                         "scales", "forcats", "viridis", "rlang")
  
  missing_packages <- character()
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      missing_packages <- c(missing_packages, pkg)
    }
  }
  
  if (length(missing_packages) > 0) {
    message("\n===============================================================================")
    message("VALIDATION PACKAGE REQUIREMENTS")
    message("===============================================================================")
    message("\nThe validate_r2e2() function requires additional visualization packages.")
    message("These packages are OPTIONAL and only needed for validation/quality control.")
    message("The core r2e2() pipeline works without them.\n")
    message("Missing packages: ", paste(missing_packages, collapse = ", "))
    message("\nWould you like to install these packages now? (y/n)")
    
    response <- tolower(trimws(readline(prompt = "> ")))
    
    if (response %in% c("y", "yes")) {
      message("\nInstalling missing packages...")
      tryCatch({
        utils::install.packages(missing_packages, quiet = FALSE)
        message("\nPackages installed successfully!")
        message("Please restart your R session and run validation again.")
        message("\nNote: You can run validation separately without re-running r2e2:")
        message("  validate_r2e2(results = my_results, geometry = my_geometries, ...)")
        message("===============================================================================\n")
        return(invisible(FALSE))
      }, error = function(e) {
        message("\nPackage installation failed: ", e$message)
        message("Please install manually using: install.packages(c('", 
             paste(missing_packages, collapse = "', '"), "'))")
        message("\nNote: You can run validation separately without re-running r2e2:")
        message("  validate_r2e2(results = my_results, geometry = my_geometries, ...)")
        message("===============================================================================\n")
        return(invisible(FALSE))
      })
    } else {
      message("\nValidation skipped due to missing packages.")
      message("To install them later, run:")
      message("  install.packages(c('", paste(missing_packages, collapse = "', '"), "'))")
      message("\nNote: You can run validation separately without re-running r2e2:")
      message("  validate_r2e2(results = my_results, geometry = my_geometries, ...)")
      message("===============================================================================\n")
      return(invisible(FALSE))
    }
  }
  
  # Setup save directory if needed
  validation_dir <- NULL
  if (!is.null(save_path)) {
    validation_dir <- file.path(save_path, "validation")
    if (!dir.exists(validation_dir)) {
      dir.create(validation_dir, recursive = TRUE)
    }
    if (verbose >= 2) {message("Validation outputs will be saved to: ", validation_dir)}
  } else {
    if (verbose >= 2) {message("No save_path provided. Validation plots will be displayed in viewer only.")}
  }
  
  # Detect temporal resolution and date columns
  dates <- as.character(unique(df_long$date))
  if (length(dates) == 0) {
    dates <- as.character(unique(df_long$year))
  }
  temporal_resolution <- get_temp_res(dates)
  
  if (verbose >= 2) {
    message("Date range: ", dates[1], " to ", dates[length(dates)])
    message("Temporal resolution: ", temporal_resolution)
  }
  
  
  # Ensure geometry is sf object
  target_geometry <- sf::st_as_sf(geometry)
  
  # Auto-detect validation_var if not provided
  if (is.null(validation_var)) {
    trans_var_cols <- get_trans_var_cols(df_long)
    if (length(trans_var_cols) > 0) {
      validation_var <- trans_var_cols[1]
      if (verbose >= 1) {
        message("   Auto-detected transformation variable for validation: ", validation_var)
      }
    } else {
      stop("No transformation variable found in data. Cannot run validation.")
    }
  }
  
  # Auto-generate validation_var_name if not provided
  if (is.null(validation_var_name)) {
    validation_var_name <- validation_var
    if (verbose >= 2) {
      message("Using variable name for plot labels: ", validation_var_name)
    }
  }
  
  # Initialize results list
  results <- list()
  
  # 1. Spatial Averages ----
  if (spatial_averages) {
    if (verbose >= 2) {
      message("\n---------------------------------------------------",
              "\n   5.1 Spatial Averages",
              "\n---------------------------------------------------\n")
    }
    
    # Process data: average over time
    average_over_time <- process_data(
      dataframe = df_long,
      input_temporal_resolution = temporal_resolution,
      average_over_time = TRUE,
      value_columns = validation_var
    )
    
    # Plot missing values
    p1 <- plot_spatial_averages(
      average_over_time, 
      target_geometry,
      title = "Sum of Missing Values over time",
      fill_variable = "NA_count",
      fill_label = "Missing Values"
    )
    print(p1)
    if (!is.null(save_path)) {
      ggplot2::ggsave(file.path(validation_dir, "missing_values.png"), p1, width = 10, height = 6)
    }
    
    # Plot spatial averages
    p2 <- plot_spatial_averages(
      average_over_time,
      target_geometry,
      title = paste0("Spatial Averages for ", validation_var_name, " over time"),
      fill_variable = validation_var,
      fill_label = validation_var_name
    )
    print(p2)
    if (!is.null(save_path)) {
      ggplot2::ggsave(file.path(validation_dir, "spatial_averages.png"), p2, width = 10, height = 6)
    }
    
    results$spatial_averages <- average_over_time
  }
  
  # 2. Time Series ----
  if (time_series) {
    if (verbose >= 2) {
      message("\n---------------------------------------------------",
              "\n   5.2 Time Series",
              "\n---------------------------------------------------\n")
    }
    
    if (temporal_resolution == "daily") {
      # Daily time series
      daily_df <- process_data(
        dataframe = df_long,
        input_temporal_resolution = temporal_resolution,
        average_over_geom_ids = TRUE,
        value_columns = validation_var
      )
      
      p3 <- plot_time_series(
        temporal_resolution = "daily",
        daily_df,
        value_variable = validation_var,
        value_label = validation_var_name,
        first_title = ""
      )
      print(p3)
      if (!is.null(save_path)) {
        ggplot2::ggsave(file.path(validation_dir, "time_series_daily.png"), p3, width = 12, height = 6)
      }
      
      results$time_series_daily <- daily_df
    }
    
    if (temporal_resolution %in% c("daily", "monthly")) {
      # Monthly time series
      monthly_df <- process_data(
        dataframe = df_long,
        input_temporal_resolution = temporal_resolution,
        output_temporal_resolution = "monthly",
        average_over_geom_ids = TRUE,
        value_columns = validation_var
      )
      
      p4 <- plot_time_series(
        temporal_resolution = "monthly",
        monthly_df,
        value_variable = validation_var,
        value_label = validation_var_name,
        first_title = ""
      )
      print(p4)
      if (!is.null(save_path)) {
        ggplot2::ggsave(file.path(validation_dir, "time_series_monthly.png"), p4, width = 12, height = 6)
      }
      
      results$time_series_monthly <- monthly_df
    }
  }
  
  # 3. Summary Statistics ----
  if (summary_stats) {
    if (verbose >= 2) {
      message("\n---------------------------------------------------",
              "\n   5.3 Summary Statistics",
              "\n---------------------------------------------------\n")
    }
    # Calculate summary statistics
    summary_stats <- calculate_summary_statistics(df_long, validation_var)
    if (verbose >= 1) {
      # Add indentation to match other r2e2 sub-messages
      cat("   Summary Statistics:\n")
      stats_output <- utils::capture.output(print(summary_stats))
      cat(paste0("   ", stats_output, collapse = "\n"), "\n")
    }
    
    if (!is.null(save_path)) {
      write.csv(summary_stats, file.path(validation_dir, "summary_statistics.csv"), row.names = FALSE)
    }
    
    # Ridge plot by month (requires ggridges)
    if (temporal_resolution %in% c("daily", "monthly")) {
      if (!requireNamespace("ggridges", quietly = TRUE)) {
        message("Skipping ridge plot (requires 'ggridges' package)")
      } else {
        ridge_df <- process_data(
          dataframe = df_long,
          input_temporal_resolution = temporal_resolution,
          value_columns = validation_var
        )
        
        p5 <- plot_summary_ridge(
          data = ridge_df,
          temporal_resolution = temporal_resolution,
          fill_variable = validation_var,
          environmental_variable_description = validation_var_name
        )
        suppressMessages(print(p5))
        if (!is.null(save_path)) {
          ggplot2::ggsave(file.path(validation_dir, "distribution_by_month.png"), p5, width = 10, height = 8)
        }
      }
    }
    
    results$summary_statistics <- summary_stats
  }
  
  # 4. Grid Cell Alignment ----
  if (grid_cell_alignment) {
    if (verbose >= 2) {
      message("\n---------------------------------------------------",
              "\n   5.4 Grid Cell Alignment",
              "\n---------------------------------------------------\n")
    }
    
    if (is.null(area_weights)) {
      warning("area_weights is required for Grid Cell Alignment. Skipping.")
    } else {
      # Sample 5 random geometries
      set.seed(123)
      sample_geometry <- target_geometry |>
        dplyr::ungroup() |>
        dplyr::slice_sample(n = min(5, nrow(target_geometry))) |>
        dplyr::mutate(geom_id = as.character(geom_id))
      
      # Convert area_weights to sf object
      centroids_sf <- area_weights |>
        sf::st_as_sf(coords = c("x", "y"), crs = 4326) |>
        sf::st_wrap_dateline(options = c("WRAPDATELINE=YES")) |>
        dplyr::filter(geom_id %in% sample_geometry$geom_id)
      
      # Plot (suppress warnings about st_point_on_surface for lon/lat data)
      p6 <- suppressWarnings(
        ggplot2::ggplot() +
          ggplot2::geom_sf(data = centroids_sf, ggplot2::aes(color = factor(geom_id)), size = 0.5, alpha = 0.5) +
          ggplot2::geom_sf(data = sample_geometry, fill = NA, color = "black", size = 0.1, alpha = 0.5) +
          ggrepel::geom_label_repel(
            data = sample_geometry,
            ggplot2::aes(label = geom_id, geometry = geometry, color = factor(geom_id)),
            stat = "sf_coordinates",
            size = 3,
            box.padding = 3.0,
            nudge_y = 0.2,
            show.legend = FALSE
          ) +
          ggplot2::scale_color_discrete(name = "geom_id") +
          ggplot2::labs(
            title = "Geometry Boundaries and Raster Cell Centroids",
            x = "Longitude",
            y = "Latitude"
          ) +
          ggplot2::theme_minimal()
      )
      
      suppressWarnings(print(p6))
      if (!is.null(save_path)) {
        ggplot2::ggsave(file.path(validation_dir, "grid_cell_alignment.png"), p6, width = 10, height = 8)
      }
      
      results$grid_cell_alignment <- list(sample_geometry = sample_geometry, centroids = centroids_sf)
    }
  }
  
  # 5. Grid Cell Count per Geometry ----
  if (cell_count_per_geometry) {
    if (verbose >= 2) {
      message("\n---------------------------------------------------",
              "\n   5.5 Grid Cell Count per Geometry",
              "\n---------------------------------------------------\n")
    }
    
    if (is.null(area_weights)) {
      warning("area_weights is required for Grid Cell Count per Geometry. Skipping.")
    } else {
      # Calculate cell counts
      geometry_cell_counts <- area_weights |>
        dplyr::group_by(geom_id) |>
        dplyr::summarise(cell_count = dplyr::n()) |>
        dplyr::mutate(
          group = dplyr::case_when(
            cell_count <= quantile(cell_count, 0.05) ~ "Lowest 5%",
            cell_count >= quantile(cell_count, 0.95) ~ "Highest 5%",
            TRUE ~ "Middle 90%"
          )
        ) |>
        dplyr::arrange(cell_count)
      
      # Summary statistics
      cell_count_summary <- calculate_summary_statistics(geometry_cell_counts, "cell_count")
      
      # Identify tiny geometries
      cell_count_threshold <- 3
      tiny_geometries <- dplyr::filter(geometry_cell_counts, cell_count <= cell_count_threshold)
      
   
      
      # Density plots
      p7 <- plot_overall_density(geometry_cell_counts)
      print(p7)
      if (!is.null(save_path)) {
        ggplot2::ggsave(file.path(validation_dir, "cell_counts_per_geometry.png"), p7, width = 10, height = 6)
      }
      
      # Detailed plots by group (optional)
      if (cell_count_per_geometry_detailed) {
        for (group_name in c("Lowest 5%", "Middle 90%", "Highest 5%")) {
          p_group <- plot_density_by_group(geometry_cell_counts, group_name)
          print(p_group)
          if (!is.null(save_path)) {
            filename <- paste0("cell_counts_", gsub(" ", "_", tolower(group_name)), ".png")
            ggplot2::ggsave(file.path(validation_dir, filename), p_group, width = 10, height = 6)
          }
        }
      }
      
      results$cell_counts <- list(
        counts = geometry_cell_counts,
        summary = cell_count_summary,
        tiny_geometries = tiny_geometries
      )
    }
  }
  
  # 6. Binned Output ----
  if (binned_output) {
    if (verbose >= 2) {
      message("\n---------------------------------------------------",
              "\n   5.6 Binned Output",
              "\n---------------------------------------------------\n")
    }
    
    # Transform binned output
    binned_dataframe <- transform_binned_output(df_long, temporal_resolution = temporal_resolution)
    
    # Histogram of bin sums
    p8 <- ggplot2::ggplot(binned_dataframe) +
      ggplot2::geom_histogram(ggplot2::aes(x = sum_of_bins), fill = 4) +
      ggplot2::labs(title = "Histogram of Bin Sums", x = "Bin Sums") +
      ggplot2::theme_minimal()
    print(p8)
    if (!is.null(save_path)) {
      ggplot2::ggsave(file.path(validation_dir, "bin_sums_histogram.png"), p8, width = 10, height = 6)
    }
    
    # Spatial averages of bin sums
    bin_sums <- process_data(
      dataframe = binned_dataframe,
      input_temporal_resolution = "daily",
      average_over_time = TRUE,
      value_columns = "sum_of_bins"
    )
    bin_sums$sum_of_bins <- round(bin_sums$sum_of_bins, 5)
    
    p9 <- plot_spatial_averages(
      bin_sums,
      target_geometry,
      title = "Bin sums averaged over time",
      fill_variable = "sum_of_bins",
      fill_label = "Bin Sum"
    )
    print(p9)
    if (!is.null(save_path)) {
      ggplot2::ggsave(file.path(validation_dir, "bin_sums_spatial.png"), p9, width = 10, height = 6)
    }
    
    # Bin distributions over time (daily only)
    if (temporal_resolution == "daily") {
      p10 <- plot_bin_proportion(binned_dataframe, temporal_resolution = "daily")
      print(p10)
      if (!is.null(save_path)) {
        ggplot2::ggsave(file.path(validation_dir, "bin_proportions_over_time.png"), p10, width = 12, height = 6)
      }
    }
    
    results$binned_output <- list(
      binned_data = binned_dataframe,
      bin_sums = bin_sums
    )
  }
  
  invisible(results)
}
