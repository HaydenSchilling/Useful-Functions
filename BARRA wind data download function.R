# Function to download and process wind data for specified locations and time periods
download_process_wind_data <- function(
    locations,
    start_year = 2009,
    end_year = 2024,
    output_dir = "Data/Wind Data",
    save_interim = TRUE,
    download_u = TRUE,
    download_v = TRUE,
    overwrite_existing = FALSE) {
  
  # Load required packages
  required_packages <- c("tidyverse", "raster", "lubridate", "data.table")
  for(pkg in required_packages) {
    if(!require(pkg, character.only = TRUE)) {
      install.packages(pkg)
      library(pkg, character.only = TRUE)
    }
  }
  
  # Install rWind if not available
  if(!require("rWind")) {
    if(!require("devtools")) {
      install.packages("devtools")
      library(devtools)
    }
    devtools::install_github("jabiologo/rWind")
    library(rWind)
  }
  
  # Create directories if they don't exist
  dir.create(file.path(output_dir), showWarnings = FALSE, recursive = TRUE)
  dir.create(file.path(output_dir, "Combined files"), showWarnings = FALSE)
  
  # Validate locations dataframe
  if(!is.data.frame(locations)) {
    stop("'locations' must be a dataframe")
  }
  
  required_cols <- c("Lat", "Lon")
  location_name_col <- NULL
  
  # Check for required columns
  if(!all(required_cols %in% colnames(locations))) {
    stop("'locations' dataframe must contain columns: 'Lat' and 'Lon'")
  }
  
  # Check for location name column (Estuary or location_name)
  if("Estuary" %in% colnames(locations)) {
    location_name_col <- "Estuary"
  } else if("location_name" %in% colnames(locations)) {
    location_name_col <- "location_name"
    colnames(locations)[colnames(locations) == "location_name"] <- "Estuary"
  } else {
    # Create location names if none provided
    message("No location names found in dataframe. Creating default names.")
    locations$Estuary <- paste0("Location_", 1:nrow(locations))
    location_name_col <- "location_name"
  }
  
  # Check for duplicate locations
  locations_check <- locations[, c("Lat", "Lon")]
  duplicates <- duplicated(locations_check)
  
  if(any(duplicates)) {
    dup_count <- sum(duplicates)
    message(paste0("Found ", dup_count, " duplicate location(s). Removing duplicates."))
    
    # Get indices of duplicate rows
    dup_indices <- which(duplicates)
    dup_locations <- locations[dup_indices, ]
    
    # Print message 
    message("Removed duplicate locations: ")
    
    # Remove duplicates
    locations <- locations[!duplicates, ]
  }
  
  # Create spatial points data frame for locations
  xy <- locations[, c("Lon", "Lat")]
  locations_sp <- SpatialPointsDataFrame(
    coords = xy, 
    data = locations,
    proj4string = CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
  )
  
  # Check if combined data files already exist
  u_combined_file <- paste0(output_dir, "/Combined files/Full u wind.csv")
  v_combined_file <- paste0(output_dir, "/Combined files/Full v wind.csv")
  uv_combined_file <- paste0(output_dir, "/Combined files/Combined U V data.csv")
  final_file <- paste0(output_dir, "/Combined files/Combined U V speed direction data.csv")
  
  # Check if final file exists and we're not overwriting
  if(file.exists(final_file) && !overwrite_existing) {
    message("Final combined file already exists and overwrite_existing=FALSE. Loading existing data.")
    return(fread(final_file))
  }
  
  # Generate year-month combinations
  months <- seq(1:12)
  years <- seq(start_year, end_year, 1)
  
  year_month <- crossing(years, months)
  
  # Remove December only for end_year=2024 (BARRA ends in Nov 2024)
  if(end_year == 2024) {
    duds <- year_month %>% filter(years == 2024 & months == 12)
    year_month <- year_month %>% anti_join(duds)
  }
  
  year_month <- year_month %>% 
    mutate(
      Month_pad = str_pad(as.character(months), width=2, pad="0"),
      year_month = paste0(years, Month_pad)
    )
  
  # Base URLs for wind data
  u_url_base <- "https://dapds00.nci.org.au/thredds/dodsC/ob53/output/reanalysis/AUS-11/BOM/ERA5/historical/hres/BARRA-R2/v1/day/uas/latest/uas_AUS-11_ERA5_historical_hres_BOM_BARRA-R2_v1_day_"
  v_url_base <- "https://dapds00.nci.org.au/thredds/dodsC/ob53/output/reanalysis/AUS-11/BOM/ERA5/historical/hres/BARRA-R2/v1/day/vas/latest/vas_AUS-11_ERA5_historical_hres_BOM_BARRA-R2_v1_day_"
  url_suffix <- ".nc"
  
  # Download U wind data
  if(download_u) {
    message("Processing U wind data...")
    pb <- txtProgressBar(min = 0, max = nrow(year_month), style = 3)
    
    for(i in 1:nrow(year_month)) {
      output_file <- paste0(output_dir, "/u_wind_", year_month$year_month[i], ".csv")
      
      # Check if file exists and skip if not overwriting
      if(file.exists(output_file) && !overwrite_existing) {
        message(paste0("\nSkipping existing file: ", basename(output_file)))
        setTxtProgressBar(pb, i)
        next
      }
      
      full_url <- paste0(u_url_base, year_month$year_month[i], "-", year_month$year_month[i], url_suffix)
      
      tryCatch({
        mydata <- raster::stack(full_url)
        extracted <- raster::extract(mydata, locations_sp, method = "bilinear")
        
        et <- t(extracted)
        et2 <- as.data.frame(et)
        et2$Date <- row.names(et2)
        
        # Dynamically rename columns based on locations
        colnames(et2)[1:nrow(locations)] <- locations$Estuary
        
        # Use fwrite instead of write_csv
        fwrite(et2, output_file)
      }, error = function(e) {
        warning(paste("Error downloading U wind data for", year_month$year_month[i], ":", e$message))
      })
      
      setTxtProgressBar(pb, i)
    }
    close(pb)
  }
  
  # Download V wind data
  if(download_v) {
    message("Processing V wind data...")
    pb <- txtProgressBar(min = 0, max = nrow(year_month), style = 3)
    
    for(i in 1:nrow(year_month)) {
      output_file <- paste0(output_dir, "/v_wind_", year_month$year_month[i], ".csv")
      
      # Check if file exists and skip if not overwriting
      if(file.exists(output_file) && !overwrite_existing) {
        message(paste0("\nSkipping existing file: ", basename(output_file)))
        setTxtProgressBar(pb, i)
        next
      }
      
      full_url <- paste0(v_url_base, year_month$year_month[i], "-", year_month$year_month[i], url_suffix)
      
      tryCatch({
        mydata <- raster::stack(full_url)
        extracted <- raster::extract(mydata, locations_sp, method = "bilinear")
        
        et <- t(extracted)
        et2 <- as.data.frame(et)
        et2$Date <- row.names(et2)
        
        # Dynamically rename columns based on locations
        colnames(et2)[1:nrow(locations)] <- locations$Estuary
        
        # Use fwrite instead of write_csv
        fwrite(et2, output_file)
      }, error = function(e) {
        warning(paste("Error downloading V wind data for", year_month$year_month[i], ":", e$message))
      })
      
      setTxtProgressBar(pb, i)
    }
    close(pb)
  }
  
  # Combine U wind files
  message("Combining U wind files...")
  u_files <- list.files(output_dir, pattern = "u_wind", full.names = TRUE)
  
  if(length(u_files) > 0) {
    u_data_list <- list()
    for(i in 1:length(u_files)) {
      u_data_list[[i]] <- fread(u_files[i])
    }
    
    u_combined <- bind_rows(u_data_list)
    if(save_interim) {
      fwrite(u_combined, paste0(output_dir, "/Combined files/Full u wind.csv"))
    }
    
    # Convert to long format
    u_dat <- u_combined %>% 
      pivot_longer(cols = locations$Estuary, names_to = "Estuary", values_to = "u")
  } else {
    stop("No U wind files found.")
  }
  
  # Combine V wind files
  message("Combining V wind files...")
  v_files <- list.files(output_dir, pattern = "v_wind", full.names = TRUE)
  
  if(length(v_files) > 0) {
    v_data_list <- list()
    for(i in 1:length(v_files)) {
      v_data_list[[i]] <- fread(v_files[i])
    }
    
    v_combined <- bind_rows(v_data_list)
    if(save_interim) {
      fwrite(v_combined, paste0(output_dir, "/Combined files/Full v wind.csv"))
    }
    
    # Convert to long format
    v_dat <- v_combined %>% 
      pivot_longer(cols = locations$Estuary, names_to = "Estuary", values_to = "v")
  } else {
    stop("No V wind files found.")
  }
  
  # Merge U and V data and calculate wind speed and direction
  message("Calculating wind speed and direction...")
  uv_dat <- u_dat %>% 
    left_join(v_dat) %>% 
    mutate(Date = ymd(str_remove(Date, pattern = "X")))
  
  # Calculate speed from vectors
  message("Calculating speed and direction...")
  sp_dir <- uv2ds(uv_dat$u, uv_dat$v)
  
  # Add results to dataframe
  uv_dat$Direction <- sp_dir[, 1]
  uv_dat$Speed_m_s <- sp_dir[, 2]
  uv_dat$Speed_km_hr <- uv_dat$Speed_m_s * 3.6
  
  # Add date components
  uv_dat$Year <- year(uv_dat$Date)
  uv_dat$Month <- month(uv_dat$Date)
  uv_dat$Day <- day(uv_dat$Date)
  
  # Add Lat and Lon to the final dataframe by joining with locations dataframe
  locations_lookup <- locations[, c("Estuary", "Lat", "Lon")]
  uv_dat <- uv_dat %>% 
    left_join(locations_lookup, by = "Estuary")
  
  # Write combined data
  message("Writing final data files...")
  if(save_interim) {
    fwrite(uv_dat, paste0(output_dir, "/Combined files/Combined U V data.csv"))
  }
  
  # Write final data with speed and direction
  fwrite(uv_dat, paste0(output_dir, "/Combined files/Combined U V speed direction data.csv"))
  
  message("Wind data processing complete!")
  return(uv_dat)
}

# #Example usage:
# sample_locations <- data.frame(
#   Lat = c(-32.27560, -33.31052, -34.52192, -34.52192),
#   Lon = c(152.49460, 151.48528, 150.84102, 150.84102),
#   Estuary = c("Wallis Lake", "Tuggerah Lake", "Lake Illawarra", "Lake Illawarra")
# )
# 
# result <- download_process_wind_data(
#   locations = sample_locations,  # Required dataframe with Lat, Lon, and location names
#   start_year = 2011,             # Start year for data (default: 2009)
#   end_year = 2012,               # End year for data (default: 2024)
#   output_dir = "Wind_Data",      # Directory for output files
#   save_interim = TRUE,           # Save intermediate files
#   download_u = TRUE,             # Download U wind data
#   download_v = TRUE,             # Download V wind data
#   overwrite_existing = T     # Skip existing files (default: FALSE)
# )

## Downloads from https://dapds00.nci.org.au/thredds/catalog/ob53/output/reanalysis/AUS-11/BOM/ERA5/historical/hres/BARRA-R2/v1/day/uas/latest/catalog.html
## Check this link to see the latest date available then might need to edit the code