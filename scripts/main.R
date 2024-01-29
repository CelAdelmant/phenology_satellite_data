# ──── LIBRARIES AND IMPORTS ──────────────────────────────────────────────────

config <- config::get()
message("Number of cores available: ", config$ncores)
source("R/utils.R")
sf::sf_use_s2(FALSE) # to avoid issues with self-intersections

# ──── FUNCTION DEFINITIONS ───────────────────────────────────────────────────


#' Generate PNG image of cropped raster
#'
#' This function generates a PNG image of a cropped raster for a specific site,
#' variable, and year.
#'
#' @param raster_cropped The cropped raster object.
#' @param contour The contour object used for cropping.
#' @param site_name The name of the site.
#' @param variable The variable being plotted.
#' @param year The year being plotted.
#' @param quadrant_folder The folder name for the quadrant.
#' @param output_dir The output directory where the PNG image will be saved.
#'
#' @return None
#'
#' @examples
#' generate_png(
#'     raster_cropped, contour, "Site A", "Temperature", 2022,
#'     "Quadrant 1", "/path/to/output"
#' )
generate_png <- function(raster_cropped, contour, site_name,
                         variable, year, quadrant_folder, output_dir) {
    # Save a png of the cropped raster for reference
    rviz <- terra::disagg(raster_cropped, 30) |> terra::crop(contour, mask = TRUE)

    title <- paste(site_name, variable, year)
    png_name <-
        file.path(
            output_dir, site_name,
            paste0(gsub(" ", "_", title), ".png")
        )
    dir.create(dirname(png_name), recursive = TRUE, showWarnings = FALSE)
    grDevices::png(png_name)
    terra::plot(rviz, main = title, col = rev(grDevices::terrain.colors(255)))
    dev.off()
}

#' Get site contour from shapefile
#'
#' This function reads a shapefile, extracts the name of the shapefile,
#' converts it to a polygon shape, and returns the name and contour as a list.
#'
#' @param site_shapefile Path to the shapefile.
#' @return A list containing the name and contour of the site.
#' @export
get_site_contour <- function(site_shapefile) {
    name <- tools::file_path_sans_ext(basename(site_shapefile))
    contour <- site_shapefile |>
        sf::st_read(quiet = TRUE) |>
        sf::st_zm(drop = TRUE) |>
        dplyr::summarise(geometry = sf::st_combine(geometry)) |>
        sf::st_cast("POLYGON") |>
        sf::st_make_valid() |>
        terra::vect()
    return(list(name = name, contour = contour))
}

#' Get TIF files from output directory
#'
#' This function retrieves a list of TIF files from the specified output directory
#' for a given quadrant and year.
#'
#' @param output_dir Path to the output directory.
#' @param quad_info Quadrant information.
#' @param year Year for which TIF files are required.
#' @return A character vector containing the paths to the TIF files.
#' @export
get_tif_files <- function(output_dir, quad_info, year) {
    year_dir <- file.path(output_dir, quad_info$folder, year)
    tif_files <- list.files(year_dir,
        pattern = "*proj.tif", full.names = TRUE
    )
    return(tif_files)
}

#' Get site information
#'
#' This function retrieves the site information for a given shapefile data,
#' site quadrant, and output directory.
#'
#' @param sh_data Shapefile data.
#' @param site_quadrant Site quadrant data.
#' @param output_dir Path to the output directory.
#' @return A list containing the site number, folder, and name.
#' @export
get_site_info <- function(sh_data, site_quadrant, output_dir) {
    site_name <- substr(sh_data$name, 1, 3)
    quadrant_number <- site_quadrant |>
        dplyr::filter(site_code == site_name) |>
        dplyr::select(quadrant)
    quadrant_folder <- paste0("quadrant_", quadrant_number)
    return(list(
        number = quadrant_number, folder = quadrant_folder,
        site = site_name
    ))
}

#' Process raster data
#'
#' This function processes a TIF file by cropping it based on a contour,
#' extracting values, and saving a PNG file if required.
#'
#' @param tif_file Path to the TIF file.
#' @param contour Contour shape.
#' @param quad_info Quadrant information.
#' @param year Year of the raster data.
#' @param save_png Logical indicating whether to save a PNG file.
#' @return A data frame containing the processed raster values.
#' @export
process_raster <- function(tif_file, contour, quad_info, year,
                           save_png = TRUE) {
    file_name <- tools::file_path_sans_ext(basename(tif_file))
    variable <- gsub(".*_(.*?)_proj.*", "\\1", file_name)
    raster_data <- terra::rast(tif_file, config$datum)
    raster_data <- raster_data[[1]]
    contour <- terra::project(contour, terra::crs(raster_data))
    raster_cropped <- terra::crop(raster_data, contour,
        mask = TRUE,
        touches = TRUE, ext = TRUE, snap = "out"
    )
    cell_size_km <- terra::cellSize(raster_cropped, unit = "km")
    raster_combined <- c(raster_cropped, cell_size_km)
    values <- terra::extract(raster_combined, contour, exact = TRUE)
    values <- values |>
        dplyr::mutate(
            cell_number = seq_len(nrow(values)),
            year = year,
            variable = variable,
            site = quad_info$site,
            quadrant = quad_info$number
        ) |>
        dplyr::rename_with(.cols = 2, ~"value")
    if (save_png) {
        generate_png(
            raster_cropped, contour, quad_info$site, variable, year,
            quad_info$folder, file.path(output_dir, "png")
        )
    }
    return(values)
}

#' Bind all dataframes together.
#'
#' This requires that all dataframes have the same columns.
#'
#' @param result A list of dataframes to be bound together
#' @return A tibble with all the dataframes bound together
bind_dataframes <- function(result) {
    df <- dplyr::bind_rows(purrr::list_flatten(result), .id = NULL) |>
        dplyr::as_tibble() |>
        dplyr::mutate(quadrant = unlist(quadrant)) # nolint
    return(df)
}

#' Change date columns to day of year instead of day since 1970.
#'
#' Date variables are: Greenup, MidGreenup, MidGreendown,
#' Dormancy, Maturity, Peak, Senescence
#'
#' @param df A dataframe with date columns
#' @return A dataframe with date columns converted to day of year
format_date_columns <- function(df) {
    dayvars <- c(
        "Greenup", "MidGreenup", "MidGreendown",
        "Dormancy", "Maturity", "Peak", "Senescence"
    )

    df <- df |>
        dplyr::mutate(
            value = ifelse(variable %in% dayvars,
                as.numeric(value) - as.numeric(as.Date(paste0(year, "-01-01"))),
                value
            )
        ) |>
        # remove 'ID' column
        dplyr::select(-c("ID"))
    return(df)
}

#' Save the data to a csv file in the derived, 'output' directory
#'
#' @param df A dataframe to be saved
#' @param file_path Path to the file where the dataframe should be saved
save_data <- function(df, file_path) {
    # create the directory if it doesn't exist
    dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
    readr::write_csv(df, file_path)
}

#' Calculate weighted average and missing values
#'
#' This function calculates the weighted average of a variable and the
#' proportion of missing values for each combination of site, year, and variable
#' in a data frame.
#'
#' @param df A data frame containing the variables: value, area, fraction,
#' cell_number, year, variable, site, quadrant
#' @return A summarized data frame with the following columns: site, year,
#' variable, value (weighted average), and missing (average proportion of
#' missing values).
#' @examples df_wa <- calculate_weighted_average(df)
weighted_average <- function(df) {
    df_wa <- df |>
        dplyr::group_by(site, year, variable) |>
        dplyr::mutate(missing = sum(is.na(value)) / dplyr::n()) |>
        dplyr::summarise(
            value = sum(value * fraction, na.rm = TRUE) /
                sum(fraction, na.rm = TRUE),
            missing = mean(missing),
            .groups = "drop"
        )
    return(df_wa)
}

# ──── SETTINGS AND DATA INGEST ───────────────────────────────────────────────

# Define output directory and years of interest
output_dir <- file.path(config$path$derived_data, "satellite")
years <- as.character(seq(2001, 2021))

# shapefile directories
shapefiles_dir <- file.path(config$path$raw_data, "shapefiles")
site_shapefiles <- list.files(shapefiles_dir,
    pattern = "*.kml",
    full.names = TRUE
)

# Read .csv file which contains all forest site codes and the satellite
# quadrant they fall into.
site_quadrant <- read.csv(file.path(
    config$path$metadata,
    "site_name_quadrant.csv"
))


# ──── MAIN ───────────────────────────────────────────────────────────────────


progressr::with_progress({
    p <- progressr::progressor(
        steps = length(site_shapefiles), enable = TRUE, trace = FALSE
    )
    system.time({
        furrr::future_map(site_shapefiles, function(site) {
            data <- list()
            suppressMessages(sf::sf_use_s2(FALSE))
            sh_data <- get_site_contour(site)
            quad_info <- get_site_info(sh_data, site_quadrant, output_dir)
            if (all(!file.exists(
                file.path(output_dir, quad_info$folder, years)
            ))) {
                message(
                    paste0("No data for ", gsub("_", " ", quad_info$folder))
                )
                return(NULL)
            }

            for (year in years) {
                tif_files <- get_tif_files(output_dir, quad_info, year)
                if (length(tif_files) == 0) {
                    message(
                        "No tif files for ", year, " in ", quad_info$folder
                    )
                    next
                }

                for (tif_file in tif_files) {
                    values <- process_raster(
                        tif_file, sh_data$contour, quad_info, year
                    )
                    data[[
                        paste(
                            quad_info$site, year, values$variable[1],
                            sep = "_"
                        )
                    ]] <- values
                }
            }
            return(data)
        },
        .options = furrr::furrr_options(seed = TRUE)
        ) -> result
    }) -> time
    print_time_elapsed(time)
})

# Save raw cell values to csv
df <- bind_dataframes(result) |> format_date_columns()
file_path <- file.path(config$path$derived_data, "output", "cell_values.csv")
save_data(df, file_path)

# Save weighted average values to csv
df_wa <- weighted_average(df)
file_path_wa <- file.path(config$path$derived_data, "output", "population_values.csv")
save_data(df_wa, file_path_wa)
