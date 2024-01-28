# ──── LIBRARIES AND IMPORTS ──────────────────────────────────────────────────

config <- config::get()
sf::sf_use_s2(FALSE) # to avoid issues with self-intersections

# ──── FUNCTION DEFINITIONS ───────────────────────────────────────────────────


#' Generate PNG image of cropped raster
#'
#' This function generates a PNG image of a cropped raster for a specific site, variable, and year.
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
#' generate_png(raster_cropped, contour, "Site A", "Temperature", 2022, "Quadrant 1", "/path/to/output")
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


process_shapefile <- function(shapefile) {
    # Extracting name of shapefile
    name <- tools::file_path_sans_ext(basename(shapefile))
    # Reading in kml and converting to shapefile
    contour <- shapefile |>
        sf::st_read(quiet = TRUE) |>
        sf::st_zm(drop = TRUE) |>
        dplyr::summarise(geometry = sf::st_combine(geometry)) |> # nolint
        sf::st_cast("POLYGON") |>
        sf::st_make_valid() |>
        terra::vect()
    return(list(name = name, contour = contour))
}


get_site_info <- function(sh_data, site_quadrant, output_dir) {
    # Looking which quadrant it falls into
    site_name <- substr(sh_data$name, 1, 3)
    quadrant_number <- site_quadrant |>
        dplyr::filter(site_code == site_name) |>
        dplyr::select(quadrant)
    quadrant_folder <- paste0("quadrant_", quadrant_number)


    return(list(number = quadrant_number, folder = quadrant_folder, site = site_name))
}


process_raster <- function(tif_file, contour, quad_info, year,
                           save_png = TRUE) {
    file_name <- tools::file_path_sans_ext(basename(tif_file))
    variable <- gsub(".*_(.*?)_proj.*", "\\1", file_name)

    raster_data <- terra::rast(tif_file, config$datum)

    # REVIEW: remove duplicate variables
    raster_data <- raster_data[[1]]

    # REVIEW: had to reproject the contour to match the raster?
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

    # Save a png of the cropped raster for reference
    if (save_png) {
        generate_png(
            raster_cropped, contour, quad_info$site, variable, year,
            quad_info$folder, file.path(output_dir, "png")
        )
    }

    return(values)
}

# ──── MAIN ───────────────────────────────────────────────────────────────────

# Here I extract data from the rasters created above for specific areas of forest.
# The output here is a dataset showing the average tree phenology variable values
# for each woodland in my dataset.

# Defining directories of output
# mcd directories

# Define output directory
output_dir <- file.path(config$path$derived_data, "satellite")


# shapefile directories
shapefiles_dir <- file.path(config$path$raw_data, "shapefiles")
shapefiles <- list.files(shapefiles_dir, pattern = "*.kml", full.names = TRUE)


# Read .csv file which contains all forest site codes and the satellite
# quadrant they fall into.
site_quadrant <- read.csv(file.path(
    config$path$metadata,
    "site_name_quadrant.csv"
))

# # read layers list
# layers_file <- file.path(config$path$derived_data, "tmp", "layers.csv")
# layers <- read.csv(layers_file) |> dplyr::as_tibble()


# # add the paths to the derived projected tif files to the layers dataframe
# layers <- layers |>
#     dplyr::mutate(
#         path = file.path(
#             config$path$derived_data, "satellite",
#             quadrant_name, year, paste0(file_name, "_", layer, "_proj.tif")
#         )
#     )

# # add the site_code to the layers dataframe, matching the number after the _ in quadrant_name to 'quadrant' in site_quadrant and extracting the site_code

# # make a new 'quadrant' column in the layers dataframe by extracting the number(s) after the _ in quadrant_name
# layers <- layers |>
#     dplyr::mutate(
#         quadrant = as.integer(gsub(".*_(\\d+)$", "\\1", quadrant_name))
#     )

# # based on the quadrant number, get the site_code from the site_quadrant dataframe
# layers <- layers |>
#     dplyr::left_join(site_quadrant, by = "quadrant") |>
#     dplyr::select(-quadrant)

# # REVIEW: need to iterate over sites, because multiple sites share quadrants.
# # maybe build a list of dataframes, one for all the years*layers for each site,
# # and then combine them at the end? site always will poit to the same shapefile.



# # now we have the site name, we can get the path to the shapefile, which is in the raw/shapefiles directory, with extension .kml, and starts with the site name
# # 1. get the list of shapefiles
# shapefiles

# # 2,. for each file_name, get the path to the corresponding shapefile and add it to that row, based on the site name:
# layers <- layers |>
#     dplyr::mutate(
#         shapefile = sapply(site, function(x) {
#             shapefile <- shapefiles |> grep(paste0("^", x), value = TRUE)
#             if (length(shapefile) == 0) {
#                 stop("No shapefile found for ", x)
#             }
#             if (length(shapefile) > 1) {
#                 stop("Multiple shapefiles found for ", x)
#             }
#             shapefile
#         })
#     )




# # check that all the paths exist
# if (any(!file.exists(layers$path))) {
#     stop("Some files are missing")
# }




# defining list of years
years <- as.character(seq(2001, 2021))


data <- list()

for (shapefile in shapefiles) {
    sh_data <- process_shapefile(shapefile)
    quad_info <- get_site_info(sh_data, site_quadrant, output_dir)

    if (all(!file.exists(file.path(output_dir, quad_info$folder, years)))) {
        message(paste0("No data found for ", gsub("_", " ", quad_info$folder)))
        next
    }

    for (year in years) {
        # listing tif files within the quadrant folder of interest
        year_dir <- file.path(output_dir, quad_info$folder, year)
        tif_files <- list.files(year_dir,
            pattern = "*proj.tif", full.names = TRUE
        )
        # check if there are any tif files, otherwise skip to next year
        if (length(tif_files) == 0) {
            message("No tif files found for ", year, " in ", quad_info$folder)
            next
        }

        for (tif_file in tif_files) {
            values <- process_raster(tif_file, sh_data$contour, quad_info, year)

            # append this to the quadrant dataframe
            data[[paste(quad_info$site, year, values$variable[1], sep = "_")]] <- values
        }
    }
}

all_data <- do.call(rbind, data) |> dplyr::as_tibble()




#         values <- values |>
#             dplyr::mutate(
#                 days_between = as.numeric(difftime(as.Date(paste0(year, "-01-01")),
#                     as.Date("1970-01-01"),
#                     units = "days"
#                 )),
#                 dplyr::across(dplyr::starts_with(layer_names),
#                     ~ . - days_between,
#                     .names = "doy"
#                 )
#             )
#         # remove 'days_between' and variable columns using base R
#         values[, !(names(values) %in% c("days_between", variable))]



#         # Saving dataset to raw list created above
#         all_years_data_list_raw[[year]] <- combined_data_peryear_full

#         # Calculating the average value for the whole woodland by weighting the pixel by the proportion of its area covered in woodland and calculating an average
#         combined_data_peryear_full <- na.omit(combined_data_peryear_full) # removing rows with NA values
#         combined_data_peryear_wa <- combined_data_peryear_full |>
#             group_by(year) |> # calculating weighted average
#             summarise(
#                 Greenup_wa = sum(Greenup_doy * proportion_wood) / sum(proportion_wood),
#                 MidGreenup_wa = sum(MidGreenup_doy * proportion_wood) / sum(proportion_wood),
#                 MidGreendown_wa = sum(MidGreendown_doy * proportion_wood) / sum(proportion_wood),
#                 Dormancy_wa = sum(Dormancy_doy * proportion_wood) / sum(proportion_wood),
#                 Maturity_wa = sum(Maturity_doy * proportion_wood) / sum(proportion_wood),
#                 Peak_wa = sum(Peak_doy * proportion_wood) / sum(proportion_wood),
#                 Senescence_wa = sum(Senescence_doy * proportion_wood) / sum(proportion_wood),
#                 EVI_Amplitude_wa = sum(Amplitude * proportion_wood) / sum(proportion_wood),
#                 EVI_Area_wa = sum(Area * proportion_wood) / sum(proportion_wood),
#                 .groups = "drop"
#             )
#         combined_data_peryear_wa <- as.data.frame(combined_data_peryear_wa)
#         all_years_data_list_wa[[year]] <- combined_data_peryear_wa
#     }
#     # Merging 'per year' rows together for each shapefile
#     # (this code would merge raw data but this is not required) all_years_per_shape <- do.call(rbind, all_years_data_list)
#     all_years_per_shape_wa <- do.call(rbind, all_years_data_list_wa)
#     # Saving dataset to excel file
#     file_path_wa <- file.path(config$path$derived_data, shapefile, "processed_data_wa.csv")
#     write.csv(all_years_per_shape_wa, file.path(file_path_wa))

#     # Saving dataset to list in r
#     all_years_per_shape_list[[shapefile]] <- all_years_per_shape
# }
