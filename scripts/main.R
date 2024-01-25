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
    # Throw an error if no HDF files found
    if (length(hdf_files) == 0) {
        stop("No HDF files found in", mcd_quadrant)
    }
    return(hdf_files)
}



# ──── PREPROCESS HDF FILES ───────────────────────────────────────────────────
# Here, I create rasters from the HDF files downloaded from EarthDataSearch

# Get the list of MCD quadrants
mcd_quadrants <- list.dirs(
    file.path(config$path$raw_data, "satellite"),
    full.names = TRUE, recursive = FALSE
)

# Main processing step

# Loop through each MCD quadrant, creating a df of all layers for each quadrant,
# HDF file, year and layer

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



# Now we have a dataframe with all the layers for each HDF file,
# we can extract the layers and save them as projected GeoTIFFs.

# Define output directory
output_dir <- file.path(config$path$derived_data, "satellite")

progressr::with_progress({
    p <- progressr::progressor(
        steps = nrow(layers), enable = TRUE, trace = FALSE
    )
    furrr::future_map(seq_len(nrow(layers)), function(i) {
        row_cont <- layers[i, ]
        p(message = row_cont$message)
        save_layer(row_cont, datum, output_dir = output_dir)
    }, .options = furrr::furrr_options(seed = TRUE))
})



# The output is 10 .tif files per hdf file, each depicting a single layer of the multilayer raster hdf files
# These are all saved in their respective folders (mcd Quadrant --> year --> //)
# I then use these layers for the analysis below



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

# defining list of years
years <- as.character(seq(2001, 2021))

# making empty data lists for output data to be saved to
all_years_data_list <- list() # this is for the raw datasets
all_years_data_list_wa <- list() # this is for the weighted average datasets

# TESTING with fewer years
# years <- as.character(seq(2001,2002))

# Nested for loops to extract information from the raster sub-datasets

for (shapefile in shapefiles) {
    # Extracting name of shapefile
    shapefile_name <- tools::file_path_sans_ext(basename(shapefile))
    # Looking which quadrant it falls into
    site_name <- substr(shapefile_name, 1, 3)
    site_quadrant_row <- site_quadrant |> dplyr::filter(site_code == site_name)
    quadrant_number <- site_quadrant_row$quadrant
    quadrant_folder <- paste0("quadrant_", quadrant_number)


    for (year in years) {
        # listing tif files within the quadrant folder of interest
        tif_files <- list.files(file.path(output_dir, quadrant_folder, year), pattern = "*proj.tif", full.names = TRUE)
        # creating a new empty dataset for each year
        combined_data_peryear <- data.frame()

        for (tif_file in tif_files) {
            # Extracting the file name without extension
            file_name <- tools::file_path_sans_ext(basename(tif_file))
            # Extracting the variable (Greenup, MidGreenup, MidGreendown etc)
            variable <- gsub(".*_(.*?)_proj.*", "\\1", file_name)

            # Reading the raster file
            raster_data <- terra::rast(tif_file, datum)

            # Reading in kml and converting to shapefile
            contour <- sf::st_read(shapefile)
            contour <- sf::st_zm(contour, drop = TRUE) # removing the z dimension
            contour <- terra::vect(contour) # making this a spatial object

            # REVIEW: had to reproject the contour to match the raster?
            contour <- terra::project(contour, terra::crs(raster_data))

            # draw the contour over the raster_data
            # crop raster data to the bounding box of the contour + 20 km
            # (to make sure the contour is fully covered)

            # Cropping and masking the raster with the contour
            raster_cropped <- terra::crop(raster_data, contour, mask = TRUE)
            raster_polygons <- terra::as.polygons(raster_cropped)
            terra::plot(raster_cropped)
            terra::plot(raster_polygons)


            raster_polygons$cell_number <- seq_len(nrow(raster_polygons)) # assigning cell number


            # Making spatial data frame
            raster_polygons <- SpatialPolygonsDataFrame(raster_polygons, data.frame(raster_polygons), match.ID = TRUE)


            # REVIEW: NMR - Not sure what this does

            # Calculating intersect and adding to new dataset
            intersect <- sf::st_intersection(raster_polygons, contour) # calculating intersect


            intersect_data <- intersect@data # making a new dataset of only intersected polygons
            intersected_rows <- intersect$cell_number # finding intersected rows
            area_overall <- area(raster_polygons) # assigning area to matrix
            area_overall <- area_overall[intersected_rows] # using only intersected rows
            intersect_data$area_overall <- area_overall # assigning to dataset
            intersect_data$area_overlap <- area(intersect) # finding area of intersection
            intersect_data$proportion_wood <- intersect_data$area_overlap / intersect_data$area_overall # calculating proportion of area covered in 'site'
            intersect_data$year <- year # assigning year value in column from file name
            colnames(intersect_data)[3] <- variable # changing the name of the value column to the variable name (so I can merge columns later)

            # Assigning this data to a new temporary dataframe
            temporary_data <- intersect_data[, c("cell_number", variable)]

            # Creating or merging dataframe/s
            if (nrow(combined_data_peryear) == 0) {
                combined_data_peryear <- temporary_data # Initialize combined_data_peryear with the first 'temp_data' set
            } else {
                # Merge 'temp_data' into 'combined_data_peryear' based on 'cell_number'
                combined_data_peryear <- merge(combined_data_peryear, temporary_data, by = "cell_number", all = TRUE)
            }

            # Merging with description data
            description_data <- intersect_data[, c("Name", "Description", "cell_number", "area_overall", "area_overlap", "proportion_wood", "year")]
            combined_data_peryear_full <- merge(combined_data_peryear, description_data, by = "cell_number", all = TRUE)

            # This creates a dataset with all information about the pixels which intersect with the contour.
            # This is raw data and therefore must be cleaned and re-formatted below
        }
        # Doing some manipulations to transform "day since 1970" to "day of year"
        combined_data_peryear_full <- combined_data_peryear_full |>
            mutate(days_between = as.numeric(difftime(
                as.Date(paste0(year, "-01-01"), format = "%Y-%m-%d"),
                as.Date("1970-01-01"),
                units = "days"
            ))) |>
            mutate(
                Greenup_doy = Greenup - days_between,
                MidGreenup_doy = MidGreenup - days_between,
                MidGreendown_doy = MidGreendown - days_between,
                Dormancy_doy = Dormancy - days_between,
                Maturity_doy = Maturity - days_between,
                Peak_doy = Peak - days_between,
                Senescence_doy = Senescence - days_between
            )

        # Removing unwated columns
        combined_data_peryear_full <- combined_data_peryear_full |>
            select(
                -Dormancy, -MidGreenup, -Greenup, -MidGreendown, -Peak,
                -Senescence, -Maturity, -Description, -area_overall,
                -area_overlap, -days_between
            )

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
