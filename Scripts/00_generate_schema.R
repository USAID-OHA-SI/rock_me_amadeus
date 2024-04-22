# PROJECT: rock_me_amadeus
# PURPOSE: Analysis of mozART data -- setting up a schema map
# AUTHOR: Tim Essam | SI
# REF ID:   9214e28b
# LICENSE: MIT
# DATE: 2023-03-24
# NOTES: Tim Essam | SI

# LOCALS & SETUP ============================================================================

  # Libraries
    library(gagglr)
    library(tidyverse)
    library(scales)
    library(sf)
    library(extrafont)
    library(tidytext)
    library(patchwork)
    library(ggtext)
    library(glue)
    library(datamodelr)
    library(googledrive)
    
  # Download data from link provided
  # See JL chat
    
  # SI specific paths/functions  
    load_secrets()
    
    mozART <- file.path("Data/MozART 2.0/")
    df_list <- list.files(mozART)
  
  # REF ID for plots
    ref_id <- "9214e28b"
    
# LOAD DATA ============================================================================  

  db <- map(setNames(df_list, df_list %>% str_remove_all(., ".gz")), 
            ~readRDS(file.path(mozART, .x)))
    
  db$df_type_id_lookup
  db$df_form %>% glimpse()
  db$df_patient %>% glimpse()
  
  db$df_form %>% 
    left_join(db$df_patient, by = "patient_uid")
  
  
  # Couldn't figure out how to purrr the list df into the dm_from_data_frames function
  # paste output from chunk below into the dm_... function
  str_c("db$", names(db)) %>% cat(., sep = ", ")
  
  #str_c("db$", names(db)) %>% cat(., sep = ", ") %>% 
  dm_from_data_frames(!!!db)
    
  dm_f <- dm_from_data_frames(db$df_clinical_consultation, db$df_dsd, db$df_form, db$df_identifier, 
                              db$df_key_vul_pop, db$df_laboratory, db$df_location, db$df_medication,
                              db$df_observation, db$df_observation_lookup, db$df_pat_state, 
                              db$df_patient, db$df_type_id_lookup)
  
  # Setup a graphic depiction so we can plot and export
  graph <- dm_create_graph(dm_f, col_attr = c("column", "type"))
  
  dm_render_graph(graph)
  
  # Export to a PDF for pushing around in AI
  dm_export_graph(graph, file_name = "Graphics/all_tables.pdf")

# MUNGE ============================================================================
  
  # Explore the names and common elements across data sets
    map(mozART_db, ~names(.x))
  
# VIZ ============================================================================

  #  

# SPINDOWN ============================================================================

