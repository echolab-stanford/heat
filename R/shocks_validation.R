#' 
#' 
#' 
#' 
#' Get basic summaries and NA counts for the shocks measures 
#'  
#' @description This function print basic statistical summaries and NA counts for each measure in the dataframe shocks information
#' 
#' @param results_data A dataframe output by the shocks_wrapper function
#' 
#' @return Null. Prints the summary for each measure and the NA count in the console. 
validate_global_summaries <- function(results_data) {
  
  cat("=== GLOBAL SUMMARIES FOR ALL MEASURES ===\n\n")
  
  # Get unique measures
  unique_measures <- unique(results_data$measure)
  
  for(measure_name in unique_measures) {
    cat("Summary for", measure_name, ":\n")
    cat("=" , rep("=", nchar(measure_name) + 12), "\n", sep="")
    
    measure_data <- results_data[measure == measure_name]$value
    
    # Basic summary
    print(summary(measure_data))
    
    # Additional stats
    cat("Count (total):", length(measure_data), "\n")
    cat("Count (non-NA):", sum(!is.na(measure_data)), "\n")
    cat("Count (NA):", sum(is.na(measure_data)), "\n")
    cat("Count (Zero):", sum(measure_data == 0, na.rm = TRUE), "\n")
    cat("Standard Deviation:", round(sd(measure_data, na.rm = TRUE), 6), "\n")
    
    cat("\n")
  }
}

# #' Access and make the error list more accesible
# #' 
# #' @description This function accesses the error list returned by the shocks_wrapper function and creates a tidier error list
# #' 
# #' @param errors_list List of errors returned by the shocks_wrapper function
# #' 
# #' @return Null if no errors exist. Else, a list with the different types of errors encountered.
# validate_errors <- function(errors_list) {
  
#   cat("=== ERROR ANALYSIS ===\n\n")
  
#   if (is.null(errors_list) || length(errors_list) == 0) {
#     cat("No errors found!\n")
#     return(invisible(NULL))
#   }
  
#   # Initialize results list
#   flattened_errors <- list()
  
#   # Process missing_dates errors
#   if ("missing_dates" %in% names(errors_list) && !is.null(errors_list$missing_dates)) {
    
#     cat("MISSING DATES ERRORS:\n")
#     cat("====================\n")
    
#     missing_dates_flat <- data.frame(
#       error_type = character(0),
#       date = character(0),
#       polygon_id = character(0),
#       intersection_pct = numeric(0),
#       stringsAsFactors = FALSE
#     )
    
#     # Loop through each date in missing_dates
#     for (case in errors_list$missing_dates) {
#       date_info <- names(case)
      
#       # Extract polygons and intersection percentage
#       polygons <- case[[date_info]]$polygons
#       intersection_pct <- case[[date_info]]$intersection_pct
      
#       # Create rows for each polygon
#       for (i in seq_along(polygons)) {
#         missing_dates_flat <- rbind(missing_dates_flat, data.frame(
#           error_type = "missing_dates",
#           date = date_name,
#           polygon_id = polygons[i],
#           intersection_pct = intersection_pct,
#           stringsAsFactors = FALSE
#         ))
#       }
#     }
    
#     # Print summary
#     if (nrow(missing_dates_flat) > 0) {
#       cat("Total affected polygon-date combinations:", nrow(missing_dates_flat), "\n")
#       cat("Unique dates with missing data:", length(unique(missing_dates_flat$date)), "\n")
#       cat("Unique polygons affected:", length(unique(missing_dates_flat$polygon_id)), "\n\n")
      
#       cat("Summary by date:\n")
#       print(table(missing_dates_flat$date))
#       cat("\n")
      
#       cat("Summary by polygon:\n")
#       print(table(missing_dates_flat$polygon_id))
#       cat("\n")
      
#       # Store in results
#       flattened_errors$missing_dates <- missing_dates_flat
#     } else {
#       cat("No missing dates errors found.\n\n")
#     }
#   }
  
#   # Process na_in_data errors (if they exist)
#   if ("na_in_data" %in% names(errors_list) && !is.null(errors_list$na_in_data)) {
    
#     cat("NA IN DATA ERRORS:\n")
#     cat("==================\n")
#     cat("Found NAs in the data for the following polygons and dates:")

#   }

#   # Process out-of-range dates
#   if ("excluded_polygons" %in% names(errors_list) && !is.null(errors_list$excluded_polygons)){
#      print("Excluded polygons: ")
#   }
  
#   # Overall summary - FIXED
#   cat("=== OVERALL ERROR SUMMARY ===\n")
  
#   # Count errors properly
#   total_errors <- 0
#   if (length(flattened_errors) > 0) {
#     for (error_type in names(flattened_errors)) {
#       if (is.data.frame(flattened_errors[[error_type]])) {
#         total_errors <- total_errors + nrow(flattened_errors[[error_type]])
#       }
#     }
#   }
  
#   cat("Total error records:", total_errors, "\n")
  
#   if (total_errors > 0) {
#     cat("\nReturning flattened error data for further analysis...\n")
#     return(flattened_errors)
#   } else {
#     cat("No errors to return.\n")
#     return(invisible(NULL))
#   }
# }

#' Produce plots to visualize the distibution of each measure on the dataframe returned by the shocks_wrapper function
#' 
#' @description This function plots the measures in the dataframe returned by the shocks wrapper function
#' 
#' @param results_data A dataframe returned by the shocks wrapper function
#' @param show_plots A boolean indicating whether to print the plots in the plot console
#' 
#' @return A list with plots for each measure
validate_measure_distributions <- function(results_data, show_plots = TRUE) {
  
  # Load required libraries for plotting
  library(ggplot2)
  
  cat("=== MEASURE DISTRIBUTION PLOTS ===\n\n")
  
  # Get unique measures
  unique_measures <- unique(results_data$measure)
  
  # Create plots for each measure
  plots_list <- list()
  
  for (measure_name in unique_measures) {
    
    cat("Creating plot for:", measure_name, "\n")
    
    # Filter data for this measure
    measure_data <- results_data[measure == measure_name]
    measure_values <- measure_data$value
    
    # Remove NA values for statistics and plotting
    clean_values <- measure_values[!is.na(measure_values)]
    
    if (length(clean_values) == 0) {
      cat("  Warning: No non-NA values found for", measure_name, ". Skipping plot.\n\n")
      next
    }
    
    # Calculate key statistics
    stats <- list(
      mean = mean(clean_values, na.rm = TRUE),
      median = median(clean_values, na.rm = TRUE),
      q05 = quantile(clean_values, 0.05, na.rm = TRUE),
      q95 = quantile(clean_values, 0.95, na.rm = TRUE)
    )
    
    # Round statistics to 2 decimal places
    stats_rounded <- lapply(stats, function(x) round(x, 2))
    
    # Create the density plot
    p <- ggplot(data.frame(values = clean_values), aes(x = values)) +
      geom_density(fill = "lightblue", alpha = 0.7, color = "darkblue") +
      
      # Add vertical lines for key statistics
      geom_vline(aes(xintercept = stats$mean), 
                 color = "red", linetype = "dashed", linewidth = 1) +
      geom_vline(aes(xintercept = stats$median), 
                 color = "green", linetype = "dashed", linewidth = 1) +
      geom_vline(aes(xintercept = stats$q05), 
                 color = "orange", linetype = "dotted", linewidth = 0.8) +
      geom_vline(aes(xintercept = stats$q95), 
                 color = "orange", linetype = "dotted", linewidth = 0.8) +
      
      # Add labels and title
      labs(
        title = paste("Distribution of", measure_name),
        subtitle = paste(
          "Mean (red):", stats_rounded$mean, 
          "| Median (green):", stats_rounded$median,
          "| 5% (orange):", stats_rounded$q05,
          "| 95% (orange):", stats_rounded$q95
        ),
        x = "Value",
        y = "Density"
      ) +
      
      # Clean theme
      theme_minimal() +
      theme(
        plot.title = element_text(hjust = 0.5, size = 14, face = "bold"),
        plot.subtitle = element_text(hjust = 0.5, size = 10),
        axis.title = element_text(size = 12),
        axis.text = element_text(size = 10)
      )
    
    # Store plot in list
    plots_list[[measure_name]] <- p
    
    # Show plot if requested
    if (show_plots) {
      print(p)
    }

  }
  
  cat("=== DISTRIBUTION ANALYSIS COMPLETE ===\n")
  cat("Generated", length(plots_list), "plots for measures with non-NA values.\n")
  if (show_plots) {
    cat("All plots have been displayed above.\n")
  }
  cat("Plots are stored in the returned list for further use.\n\n")
  
  # Return the list of plots for potential further use
  return(plots_list)
}
