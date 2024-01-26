# In this script, I extract the sub-datasets from the rasters downloaded
# from EarthDataSearch. I then perform a large for loop on all of these
# sub-datasets (ONLY RUN ON ARC).
# These for loops take each shapefile, extract which pixels of the raster
# intersect with the shapefile, and extract the values of Greenup, midgreenup
# etc from these pixles. It then forms a dataset for each shapefile and saves
# it to the folder 'Outputs' in "Processed Satellite Images"

# ONLY RUN IN ARC COMPUTERS -


# ──── LIBRARIES AND IMPORTS ──────────────────────────────────────────────────

# loading packages
# library(dplyr)
# library(tidyr)
# library(sf)
# library(terra)
# library(gdalUtilities)
# library(ggplot2)
# library(rasterVis)
# library(lubridate)

config <- config::get()

# ──── GENERAL SETTINGS ───────────────────────────────────────────────────────

# Defining layers to extract
layer_names <- c(
    "Greenup", "MidGreenup", "Peak", "Maturity",
    "Senescence", "MidGreendown", "Dormancy", "EVI_Minimum",
    "EVI_Amplitude", "EVI_Area"
)

# get oly half of the layers for testing #REVIEW
layer_names <- c(
    "Greenup", "MidGreenup", "Peak", "Maturity", "Senescence",
    "MidGreendown"
)

# Defining projection
datum <- "epsg:4326"


# ──── PARALLEL SETUP ─────────────────────────────────────────────────────────

ncores <- future::availableCores()
message("Number of cores available: ", ncores)
future::plan("multisession", workers = ncores)
progressr::handlers("cli")


# ──── FUNCTION DEFINITIONS ───────────────────────────────────────────────────


# Function to extract layers of interest from HDF files
extract_layers <- function(quadrant_name, hdf_file, layer_names) {
    file_name <- gsub("\\..*$", "", basename(hdf_file))
    year <- substr(file_name, 5, 8)

    gdalinfo_output <- gdalUtilities::gdalinfo(hdf_file, quiet = TRUE)

    layer_lines <- sub(
        "^\\s*SUBDATASET_\\d+_NAME=", "",
        grep("^\\s*SUBDATASET_\\d+_NAME",
            strsplit(gdalinfo_output, "\n")[[1]],
            value = TRUE
        )
    )

    layer_lines <- layer_lines[grep(paste0(
        paste0(layer_names, collapse = "|"), "$"
    ), layer_lines)]


    layers <- data.frame(
        file_name = file_name,
        data = layer_lines,
        quadrant_name = quadrant_name,
        year = year,
        name = sapply(
            strsplit(layer_lines, ":"),
            function(x) x[length(x)]
        ),
        layer = sapply(
            strsplit(layer_lines, ":"),
            function(x) x[length(x)]
        ),
        message = paste0(
            sapply(
                strsplit(layer_lines, ":"),
                function(x) x[length(x)]
            ), "(", year, ")",
            "in ", quadrant_name
        )
    )
    return(layers)
}


# Function to extract and save the layer as a projected GeoTIFF.
save_layer <- function(row_d, datum, output_dir) {
    sub <- row_d$name
    quadrant_name <- row_d$quadrant_name
    year <- row_d$year
    file_name <- gsub("\\..*$", "", row_d$file_name)

    gdal_layer <- row_d$data

    output_dir <- file.path(
        output_dir, quadrant_name,
        year
    )
    if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

    dst_name <- paste0(file_name, "_", sub, ".tif")
    out_file <- file.path(output_dir, dst_name)
    gdalUtilities::gdal_translate(gdal_layer, dst_dataset = out_file)
    raster_data <- terra::rast(out_file, datum)

    out_name <- paste0(file_name, "_", sub, "_proj.tif")
    terra::writeRaster(raster_data,
        filename = file.path(output_dir, out_name),
        overwrite = TRUE
    )

    file.remove(out_file)
    file.remove(paste0(out_file, ".aux.xml"))
}


get_hdf_files <- function(mcd_quadrant) {
    # Get the list of HDF files
    hdf_files <- list.files(mcd_quadrant,
        pattern = "*.hdf",
        full.names = TRUE
    )
    # throw an error if there are any .aux.xml files
    if (length(grep("\\.aux\\.xml$", hdf_files)) > 0) {
        stop("Found unwanted .aux.xml files in", mcd_quadrant)
    }
    # Throw an error if no HDF files found
    if (length(hdf_files) == 0) {
        stop("No HDF files found in", mcd_quadrant)
    }
    return(hdf_files)
}

print_time_elapsed <- function(result) {
    time_elapsed <- result[3]
    minutes <- floor(time_elapsed / 60)
    seconds <- round(time_elapsed %% 60, 2)
    message("Finished in ", minutes, " minutes and ", seconds, " seconds")
}


# ──── PREPROCESS HDF FILES ───────────────────────────────────────────────────
# Here, I create rasters from the HDF files downloaded from EarthDataSearch


# Get the list of MCD quadrants
mcd_quadrants <- list.dirs(
    file.path(config$path$raw_data, "satellite"),
    full.names = TRUE, recursive = FALSE
)

# Loop through each MCD quadrant, creating a df of all layers for each quadrant,
# HDF file, year and layer.

dfs <- list()

for (mcd_quadrant in mcd_quadrants) {
    hdf_files <- get_hdf_files(mcd_quadrant)
    quadrant_name <- basename(mcd_quadrant)

    for (hdf_file in hdf_files) {
        layer <- extract_layers(quadrant_name, hdf_file, layer_names)
        dfs[[length(dfs) + 1]] <- layer
    }
}

layers <- dplyr::as_tibble(do.call(rbind, dfs))

# create a test subset of the layers (just 3 random rows)
layers <- layers[sample(nrow(layers), 3), ] # REVIEW

# save layers to a csv file in the derived data folder
layers_file <- file.path(config$path$derived_data, "tmp", "layers.csv")
# create the directory if it doesn't exist
if (!dir.exists(dirname(layers_file))) dir.create(dirname(layers_file))
write.csv(layers, layers_file, row.names = FALSE)


# Now we have a dataframe with all the layers for each HDF file,
# we can extract the layers and save them as projected GeoTIFFs.

# Define output directory
output_dir <- file.path(config$path$derived_data, "satellite")

progressr::with_progress({
    p <- progressr::progressor(
        steps = nrow(layers), enable = TRUE, trace = FALSE
    )
    time <- system.time({
        result <- furrr::future_map(seq_len(nrow(layers)), function(i) {
            row_cont <- layers[i, ]
            p(message = row_cont$message)
            save_layer(row_cont, datum, output_dir = output_dir)
        }, .options = furrr::furrr_options(seed = TRUE))
    })
    # Pretty print time elapsed
    print_time_elapsed(time)
})



# The output is 10 .tif files per hdf file, each depicting a single layer of the multilayer raster hdf files
# These are all saved in their respective folders (mcd Quadrant --> year --> //)
# I then use these layers for the analysis below


# ──── SOURCE FOR BELOW ───────────────────────────────────────────────────────

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
            output_dir, quadrant_folder,
            paste0(gsub(" ", "_", title), ".png")
        )
    grDevices::png(png_name)
    terra::plot(rviz, main = title, col = rev(grDevices::terrain.colors(255)))
    dev.off()
}



###############################################################################
############################ Processing tif files #############################

# Here I extract data from the rasters created above for specific areas of forest.
# The output here is a dataset showing the average tree phenology variable values
# for each woodland in my dataset.

# Defining directories of output
# mcd directories


# shapefile directories

shapefiles_dir <- file.path(config$path$raw_data, "shapefiles")
shapefiles <- list.files(shapefiles_dir, pattern = "*.kml", full.names = TRUE)


# Read .csv file which contains all forest site codes and the satellite
# quadrant they fall into.
site_quadrant <- read.csv(file.path(
    config$path$metadata,
    "site_name_quadrant.csv"
))

# read layers list
layers_file <- file.path(config$path$derived_data, "tmp", "layers.csv")
layers <- read.csv(layers_file) |> dplyr::as_tibble()


# add the paths to the derived projected tif files to the layers dataframe
layers <- layers |>
    dplyr::mutate(
        path = file.path(
            config$path$derived_data, "satellite",
            quadrant_name, year, paste0(file_name, "_", layer, "_proj.tif")
        )
    )

# add the site_code to the layers dataframe, matching the number after the _ in quadrant_name to 'quadrant' in site_quadrant and extracting the site_code

# make a new 'quadrant' column in the layers dataframe by extracting the number(s) after the _ in quadrant_name
layers <- layers |>
    dplyr::mutate(
        quadrant = as.integer(gsub(".*_(\\d+)$", "\\1", quadrant_name))
    )

# based on the quadrant number, get the site_code from the site_quadrant dataframe
layers <- layers |>
    dplyr::left_join(site_quadrant, by = "quadrant") |>
    dplyr::select(-quadrant)

#REVIEW: need to iterate over sites, because multiple sites share quadrants.
# maybe build a list of dataframes, one for all the years*layers for each site,
# and then combine them at the end? site always will poit to the same shapefile.



# now we have the site name, we can get the path to the shapefile, which is in the raw/shapefiles directory, with extension .kml, and starts with the site name
# 1. get the list of shapefiles
shapefiles

# 2,. for each file_name, get the path to the corresponding shapefile and add it to that row, based on the site name:
layers <- layers |>
    dplyr::mutate(
        shapefile = sapply(site, function(x) {
            shapefile <- shapefiles |> grep(paste0("^", x), value = TRUE)
            if (length(shapefile) == 0) {
                stop("No shapefile found for ", x)
            }
            if (length(shapefile) > 1) {
                stop("Multiple shapefiles found for ", x)
            }
            shapefile
        })
    )




# check that all the paths exist
if (any(!file.exists(layers$path))) {
    stop("Some files are missing")
}



for (i in seq_len(nrow(layers))) {
    row <- layers[i, ]
}

    # Extracting the variable (Greenup, MidGreenup, MidGreendown etc)
    variable <- row$layer

    # get the site_name from the site_quadrant dataframe
    # first 3 characters of the file name
    site <- substr(row$file_name, 1, 3)

    # Reading the raster file
    raster_data <- terra::rast(row$path, datum)

    # REVIEW: remove duplicate variables
    raster_data <- raster_data[[1]]

    # Reading in kml and converting to shapefile
    contour <- shapefile |>
        sf::st_read() |>
        sf::st_zm(drop = TRUE) |>
        terra::vect()


    # REVIEW: had to reproject the contour to match the raster?
    # check if destination crs is always the same and remove this step
    contour <- terra::project(contour, terra::crs(raster_data))


    # Cropping and masking the raster with the contour,
    # calculating the cell area and the proportion of the cell
    # covered by the woodland contour
    raster_cropped <- terra::crop(raster_data, contour, mask = TRUE)
    cell_size_km <- terra::cellSize(raster_cropped, unit = "km")
    raster_combined <- c(raster_cropped, cell_size_km)
    values <- terra::extract(raster_combined, contour, exact = TRUE)
    values <- values |>
        dplyr::mutate(
            cell_number = seq_len(nrow(values)),
            year = year,
            variable = variable,
            site = site_name,
            quadrant = quadrant_number
        )
    names(values)[grep(paste0(".*", variable, ".*"), names(values))] <- "value"

    # append this to the quadrant dataframe
    quadrant_dfs[[length(quadrant_dfs) + 1]] <- values

    # Save a png of the cropped raster for reference
    generate_png(
        raster_cropped, contour, site_name, variable, year,
        quadrant_folder, output_dir
    )
}




# defining list of years
years <- as.character(seq(2001, 2021))

# making empty data lists for output data to be saved to
all_years_data_list <- list() # this is for the raw datasets
all_years_data_list_wa <- list() # this is for the weighted average datasets

# TESTING with fewer years
# years <- as.character(seq(2001,2002))

# Nested for loops to extract information from the raster sub-datasets

data = list()

for (shapefile in shapefiles) {
    # Extracting name of shapefile
    shapefile_name <- tools::file_path_sans_ext(basename(shapefile))

    # Reading in kml and converting to shapefile
    contour <- shapefile |>
        sf::st_read() |>
        sf::st_zm(drop = TRUE) |>
        terra::vect()

    # Looking which quadrant it falls into
    site_name <- substr(shapefile_name, 1, 3)
    site_quadrant_row <- site_quadrant |> dplyr::filter(site_code == site_name)
    quadrant_number <- site_quadrant_row$quadrant
    quadrant_folder <- paste0("quadrant_", quadrant_number)

    # check if there are any files in the quadrant folder,
    # otherwise skip to next quadrant
    if (length(list.files(file.path(output_dir, quadrant_folder))) == 0) {
        message("No files found for ", quadrant_folder)
        next
    }

    quadrant_dfs <- list()

    for (year in years) {
        # listing tif files within the quadrant folder of interest
        tif_files <- list.files(file.path(output_dir, quadrant_folder, year),
            pattern = "*proj.tif", full.names = TRUE
        )
        # check if there are any tif files, otherwise skip to next year
        if (length(tif_files) == 0) {
            message("No tif files found for ", year, " in ", quadrant_folder)
            next
        }
        # creating a new empty dataset for each year
        combined_data_peryear <- data.frame()

        for (tif_file in tif_files) {
            # Extracting the file name without extension
            file_name <- tools::file_path_sans_ext(basename(tif_file))
            # Extracting the variable (Greenup, MidGreenup, MidGreendown etc)
            variable <- gsub(".*_(.*?)_proj.*", "\\1", file_name)

            # Reading the raster file
            raster_data <- terra::rast(tif_file, datum)

            # REVIEW: remove duplicate variables
            raster_data <- raster_data[[1]]

            # REVIEW: had to reproject the contour to match the raster?
            contour <- terra::project(contour, terra::crs(raster_data))


            # Cropping and masking the raster with the contour,
            # calculating the cell area and the proportion of the cell
            # covered by the woodland contour
            raster_cropped <- terra::crop(raster_data, contour, mask = TRUE)
            cell_size_km <- terra::cellSize(raster_cropped, unit = "km")
            raster_combined <- c(raster_cropped, cell_size_km)
            values <- terra::extract(raster_combined, contour, exact = TRUE)
            values <- values |>
                dplyr::mutate(
                    cell_number = seq_len(nrow(values)),
                    year = year,
                    variable = variable,
                    site = site_name,
                    quadrant = quadrant_number
                )
            names(values)[grep(paste0(".*", variable, ".*"), names(values))] <- "value"

            # append this to the quadrant dataframe
            quadrant_dfs[[length(quadrant_dfs) + 1]] <- values

            # Save a png of the cropped raster for reference
            generate_png(
                raster_cropped, contour, site_name, variable, year,
                quadrant_folder, output_dir
            )
        }




        values <- values |>
            dplyr::mutate(
                days_between = as.numeric(difftime(as.Date(paste0(year, "-01-01")),
                    as.Date("1970-01-01"),
                    units = "days"
                )),
                dplyr::across(dplyr::starts_with(layer_names),
                    ~ . - days_between,
                    .names = "doy"
                )
            )
        # remove 'days_between' and variable columns using base R
        values[, !(names(values) %in% c("days_between", variable))]



        # Saving dataset to raw list created above
        all_years_data_list_raw[[year]] <- combined_data_peryear_full

        # Calculating the average value for the whole woodland by weighting the pixel by the proportion of its area covered in woodland and calculating an average
        combined_data_peryear_full <- na.omit(combined_data_peryear_full) # removing rows with NA values
        combined_data_peryear_wa <- combined_data_peryear_full |>
            group_by(year) |> # calculating weighted average
            summarise(
                Greenup_wa = sum(Greenup_doy * proportion_wood) / sum(proportion_wood),
                MidGreenup_wa = sum(MidGreenup_doy * proportion_wood) / sum(proportion_wood),
                MidGreendown_wa = sum(MidGreendown_doy * proportion_wood) / sum(proportion_wood),
                Dormancy_wa = sum(Dormancy_doy * proportion_wood) / sum(proportion_wood),
                Maturity_wa = sum(Maturity_doy * proportion_wood) / sum(proportion_wood),
                Peak_wa = sum(Peak_doy * proportion_wood) / sum(proportion_wood),
                Senescence_wa = sum(Senescence_doy * proportion_wood) / sum(proportion_wood),
                EVI_Amplitude_wa = sum(Amplitude * proportion_wood) / sum(proportion_wood),
                EVI_Area_wa = sum(Area * proportion_wood) / sum(proportion_wood),
                .groups = "drop"
            )
        combined_data_peryear_wa <- as.data.frame(combined_data_peryear_wa)
        all_years_data_list_wa[[year]] <- combined_data_peryear_wa
    }
    # Merging 'per year' rows together for each shapefile
    # (this code would merge raw data but this is not required) all_years_per_shape <- do.call(rbind, all_years_data_list)
    all_years_per_shape_wa <- do.call(rbind, all_years_data_list_wa)
    # Saving dataset to excel file
    file_path_wa <- file.path(config$path$derived_data, shapefile, "processed_data_wa.csv")
    write.csv(all_years_per_shape_wa, file.path(file_path_wa))

    # Saving dataset to list in r
    all_years_per_shape_list[[shapefile]] <- all_years_per_shape
}
