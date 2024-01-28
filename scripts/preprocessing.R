# In this script, I extract the sub-datasets from the rasters downloaded
# from EarthDataSearch. I then perform a large for loop on all of these
# sub-datasets (ONLY RUN ON ARC).
# These for loops take each shapefile, extract which pixels of the raster
# intersect with the shapefile, and extract the values of Greenup, midgreenup
# etc from these pixles. It then forms a dataset for each shapefile and saves
# it to the folder 'Outputs' in "Processed Satellite Images"


# ──── LIBRARIES AND IMPORTS ──────────────────────────────────────────────────

config <- config::get()
message("Number of cores available: ", config$ncores)
source("R/utils.R")


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
        layer <- extract_layers(quadrant_name, hdf_file, config$layer_names)
        dfs[[length(dfs) + 1]] <- layer
    }
}

layers <- dplyr::as_tibble(do.call(rbind, dfs))

# create a test subset of the layers (just 3 random rows)
# layers <- layers[sample(nrow(layers), 3), ] # REVIEW

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
            save_layer(row_cont, config$datum, output_dir = output_dir)
        }, .options = furrr::furrr_options(seed = TRUE))
    })
    # Pretty print time elapsed
    print_time_elapsed(time)
})
