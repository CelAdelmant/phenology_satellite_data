# In this script, I extract the sub-datasets from the rasters downloaded
# from EarthDataSearch. I then perform a large for loop on all of these
# sub-datasets (ONLY RUN ON ARC).
# These for loops take each shapefile, extract which pixels of the raster
# intersect with the shapefile, and extract the values of Greenup, midgreenup
# etc from these pixles. It then forms a dataset for each shapefile and saves
# it to the folder 'Outputs' in "Processed Satellite Images"

# ONLY RUN IN ARC COMPUTERS -

# loading packages
library(dplyr)
library(tidyr)
library(sf)
library(raster)
library(terra)
library(gdalUtilities)
library(ggplot2)
library(rasterVis)
library(lubridate)

# setting working directory
setwd("C:\\Users\\newc6032\\OneDrive - Nexus365\\Documents\\ArcGIS\\Projects\\Tree_Phenology\\Satellite images\\")

########################## Preparing rasters ############################################
# Here, I create rasters from the HDF files downloaded from EarthDataSearch

# Defining subdatasets to run for-loop through (I do not include NumCycles as I am not interested in this dataset)
subdatasets <- c(
    "Greenup", "MidGreenup", "Peak", "Maturity",
    "Senescence", "MidGreendown", "Dormancy", "EVI_Minimum",
    "EVI_Amplitude", "EVI_Area"
)

# Defining projection as wgs1984
wgs1984 <- "+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"

# Defining directories of input files
input_MCD_dir <- "C:\\Users\\newc6032\\OneDrive - Nexus365\\Documents\\ArcGIS\\Projects\\Tree_Phenology\\Satellite images" # this is where all of the original hdf files are, but they are in seperate folders for each quadrant
MCD_quadrants <- list.dirs(input_MCD_dir, full.names = TRUE, recursive = FALSE) # this output shows all of the different quadrant folders

# For loop to extract raster subdatasets of interest from the hdf files
for (MCD_quadrant in MCD_quadrants) {
    # Extracting the name of the folder for output
    MCD_quadrant_name <- tools::file_path_sans_ext(basename(MCD_quadrant))
    # Listing files in MCD quadrant folder
    hdf_files <- list.files(MCD_quadrant, pattern = "*.hdf", full.names = TRUE)

    # Loop through each HDF file
    for (hdf_file in hdf_files) {
        # Extracting the filename without extension
        file_name <- tools::file_path_sans_ext(basename(hdf_file))
        year <- substr(file_name, 5, 8)
        # Extracting sub-dataset name
        gdalinfo_output <- gdalinfo(paste0(file_name, ".hdf"))
        gdalinfo_lines <- strsplit(gdalinfo_output, "\n")[[1]] # splitting the text by lines
        subdataset_lines <- grep("^\\s*SUBDATASET_\\d+_NAME", gdalinfo_lines, value = TRUE) # extracting lines containing subdataset names
        subdataset_lines <- gsub("^\\s*SUBDATASET_\\d+_NAME=", "", subdataset_lines) # extract names of subdatasets

        # Creating for-loop to run through each subdataset and translate to .tif, project to WGS1984 and write as a projected GeoTiFF
        for (sub in subdatasets) {
            gdal_subdataset <- paste0(subdataset_lines)
            dst_name <- paste0(file_name, "_", sub, ".tif")
            gdal_translate(gdal_subdataset, dst_dataset = dst_name) # converting subdataset to .tif
            raster_data <- raster(dst_name) # creating a raster object from the .tif file
            raster_proj <- projectRaster(raster_data, crs = wgs1984) # resampling the raster using the projection string
            writeRaster(raster_proj, filename = paste0("C:\\Users\\newc6032\\OneDrive - Nexus365\\Documents\\ArcGIS\\Projects\\Tree_Phenology\\Processed Satellite images\\", MCD_quadrant_name, "\\", year, "\\", file_name, "_", sub, "_proj.tif")) # writing the projected raster to a new .tif file
        }
    }
}
# The output is 10 .tif files per hdf file, each depicting a single layer of the multilayer raster hdf files
# These are all saved in their respective folders (MCD Quadrant --> year --> //)
# I then use these subdatasets for the analysis below

###############################################################################
############################ Processing tif files #############################

# Here I extract data from the rasters created above for specific areas of forest.
# The output here is a dataset showing the average tree phenology variable values
# for each woodland in my dataset.

# Defining directories of output
# MCD directories
processed_MCD_dir <- "C:\\Users\\newc6032\\OneDrive - Nexus365\\Documents\\ArcGIS\\Projects\\Tree_Phenology\\Processed Satellite Images test\\" # thi is where the processed raster subdatasets are stored
setwd("C:\\Users\\newc6032\\OneDrive - Nexus365\\Documents\\ArcGIS\\Projects\\Tree_Phenology\\Processed Satellite images test") # here I set te working directory
# shapefile directories
shapefiles_dir <- "C:\\Users\\newc6032\\OneDrive - Nexus365\\Documents\\ArcGIS\\Projects\\Tree_Phenology\\Shapefiles extracted test" # this is where the shapefiles for each forest are stored
shapefiles <- list.files(shapefiles_dir, pattern = "*.kml", full.names = TRUE) # listing all of these shapefiles as inputs for the for loop
site_quadrant <- read.csv("C:\\Users\\newc6032\\OneDrive - Nexus365\\site_name_quadrant.csv") # downloading a csv file which contains all forest site codes and the satellite quadrant they fall into. This is because the satellite product is split into different areas of the globe.

# redefining the projection (as above)
wgs1984 <- "+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"

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
    site_quadrant_row <- site_quadrant %>% filter(site_code == site_name)
    quadrant_number <- site_quadrant_row$quadrant
    quadrant_folder <- paste0("Quadrant ", quadrant_number, sep = "")


    for (year in years) {
        # listing tif files within the quadrant folder of interest
        tif_files <- list.files(paste0(processed_MCD_dir, quadrant_folder, "\\", year, "\\", sep = ""), pattern = "*proj.tif", full.names = TRUE)
        # creating a new empty dataset for each year
        combined_data_peryear <- data.frame()

        for (tif_file in tif_files) {
            # Extracting the file name without extension
            file_name <- tools::file_path_sans_ext(basename(tif_file))
            # Extracting the variable (Greenup, MidGreenup, MidGreendown etc)
            variable <- gsub(".*_(.*?)_proj.*", "\\1", file_name)

            # Reading the raster file
            raster_data <- raster::raster(tif_file)
            # Reading in shapefile and cleaning
            shapefile <- st_read(shapefile)
            shapefile <- st_zm(shapefile, drop = TRUE) # removing the z dimension
            shapefile <- as_Spatial(shapefile) # making this a spatial object

            # Cropping the raster with the shapefile
            raster_cropped <- crop(raster_data, extent(shapefile))
            raster_polygons <- rasterToPolygons(raster_cropped, dissolve = TRUE)
            raster_polygons$cell_number <- seq_len(nrow(raster_polygons)) # assigning cell number

            # Making spatial data frame
            raster_polygons <- SpatialPolygonsDataFrame(raster_polygons, data.frame(raster_polygons), match.ID = TRUE)

            # Ensuring same projection (not necessary but just in case!)
            projection(shapefile) <- wgs1984
            projection(raster_polygons) <- wgs1984

            # Calculating intersect and adding to new dataset
            intersect <- raster::intersect(shapefile, raster_polygons) # calculating intersect
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

            # This creates a dataset with all information about the pixels which intersect with theshapefile.
            # This is raw data and therefore must be cleaned and re-formatted below
        }
        # Doing some manipulations to transform "day since 1970" to "day of year"
        combined_data_peryear_full <- combined_data_peryear_full %>%
            mutate(days_between = as.numeric(difftime(
                as.Date(paste0(year, "-01-01"), format = "%Y-%m-%d"),
                as.Date("1970-01-01"),
                units = "days"
            ))) %>%
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
        combined_data_peryear_full <- combined_data_peryear_full %>%
            select(
                -Dormancy, -MidGreenup, -Greenup, -MidGreendown, -Peak,
                -Senescence, -Maturity, -Description, -area_overall,
                -area_overlap, -days_between
            )

        # Saving dataset to raw list created above
        all_years_data_list_raw[[year]] <- combined_data_peryear_full

        # Calculating the average value for the whole woodland by weighting the pixel by the proportion of its area covered in woodland and calculating an average
        combined_data_peryear_full <- na.omit(combined_data_peryear_full) # removing rows with NA values
        combined_data_peryear_wa <- combined_data_peryear_full %>%
            group_by(year) %>% # calculating weighted average
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
    file_path_wa <- paste0("C:\\Users\\newc6032\\OneDrive - Nexus365\\Documents\\ArcGIS\\Projects\\Tree_Phenology\\Processed Satellite images\\Outputs\\WeightedAverage\\", shapefile, "processed_data_wa.csv")
    write.csv(all_years_per_shape_wa, file.path(file_path))

    # Saving dataset to list in r
    all_years_per_shape_list[[shapefile]] <- all_years_per_shape
}
