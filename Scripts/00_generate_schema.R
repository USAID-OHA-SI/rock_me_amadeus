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
    library(dbplyr)
    library(scales)
    library(sf)
    library(tidytext)
    library(patchwork)
    library(ggtext)
    library(glue)
    library(datamodelr) # Switch to dm package
    library(dm)
    library(googledrive)

  source("./Scripts/00_utilities.R")
    
  # Download data from link provided
  # See JL chat
    
  # SI specific paths/functions  
    #load_secrets()
    
    mozART <- file.path("Data/MozART 2.0/")
    df_list <- list.files(mozART)
  
  # REF ID for plots
    ref_id <- "9214e28b"
    
# LOAD DATA ============================================================================  

  # Bind all tables into a named list
  db <- map(setNames(df_list, df_list %>% str_remove_all(., "df_|.gz")), 
            ~readRDS(file.path(mozART, .x)))
    
  # Take a pick at all tables
  # db$type_id_lookup %>% glimpse()
  # db$form %>% glimpse()
  # db$patient %>% glimpse()
  
  db %>% glimpse_all()
  
  # List table / columns structure
  
  ## YAML Format for DM -> 
  names(db) %>% 
    walk(function(.x) {
      cat(glue("\n\n- table: {.x}\n\n"),
        "\tsegment: *dim\n",
        "\tcolumns:\n",
        glue("{paste0('\t\t', names(db[[.x]]), collapse = ':\n')}:\n\n"),
        set = "\n"
      )
    }) 
  
  yaml::write_yaml(NULL, file = "./Documents/data-model.yml")
  
  # Copy console output to yaml file
  
  # names(db) %>% 
  #   map_chr(function(.x) {
  #     paste0(glue("\ntable: {.x}\n\n"),
  #         "\tsegment: *dim\n",
  #         "\tcolumns:\n",
  #         glue("{paste0('\t\t', names(db[[.x]]), collapse = ':\n')}:\n\n"),
  #         set = "\n"
  #     )
  #   }) %>% 
  #   yaml::write_yaml(file = "./Documents/data-model.yml")

  # List table / columns structure
  
  names(db) %>% 
    walk(function(.x) {
      cat(glue("\n\n{which(names(db) == .x)} - {.x}\n\n\n"))
      tbl <- get_structure(db[[.x]])
      print(tbl, n = Inf)
    })
  
  db_tables <- df_list %>% 
    map_chr(~str_remove_all(.x, "df_|.gz")) 
  
  #db_tables %>% clipr::write_clip()
  
  # Track down all tables from data dict ----
  
  df_metadata <- tibble(
    id = 1:length(df_list),
    source = c(
      "clinical_consultation",
      "dsd",
      "form",
      "identifier",
      "key_vul_pop",
      "laboratory",
      "location",
      "medication",
      "observation",
      "observation_lookup",
      "pat_state",
      "patient",
      "type_id_lookup"
    ),
    primary_key = "id",
    foreign_key = c(
      "observation::encounter_uuid",
      "observation::encounter_uuid, patient::patient_uuid, location::location_uuid",
      "patient::patient_uuid, location::location_uuid",
      "patient::patient_uuid",
      "observation::encounter_uuid",
      "observation::encounter_uuid, labtest_uuid",
      "orgs::datim_id",
      "observation::encounter_uuid",
      "observation::encounter_uuid",
      "",
      "patient::patient_uuid, location::location_uuid, observation::encounter_uuid, location::state_uuid",
      "",
      ""
    ),
    description = c(
      "Contains consultation dates only. Join this to observation table based on encounters uuid for more details.",
      "Contains differentiated service delivery models a person might be enrolled in.",
      "Forms",
      "Identifications",
      "Contains key population and vulnerable population information captured in the Ficha Clinica, Ficha de APSS, PreP forms. Key population captured in
the Ficha Clinica, FFicha de APSS differ from the PreP forms. PreP forms currently aren’t transferred into Mozart.",
      "Includes both lab orders and results from multiple data sources, including clinical (ficha resumo, ficha clinica), manually entered laboratory
results (laboratoro general), and electronically submitted results via interoperability (FSR)",
      "Location",
      "Contains HIV medications prescribed and dispensed for a patient in SESP. The columns are populated based on the form data source and will change
overtime. In some cases, the encounter_type will need to be used in conjunction with the form_id to specify the source",
      "Contains data not captured in other tables, such as pregnancy and breastfeeding. A significant majority of these The
OBSERVATION_LOOKUP table provides the descriptions for every observation id, which is called a concept id in Open MRS SESP. To minimize
the transformation from Open MRS SESP to Mozart, Open MRS SESP names and data structures were maintained",
      "Observation types",
      "Captures the patient state information that is collected in the program enrollment module and on fichas. The Program Enrollment Module in SESP is
an independent module used when a patient is registered for a service and is considered the starting point for registration in SESP.",
      "Patient",
      "ID type lookup"
    )
  )
  
  df_metadata %>% glimpse()
  
  df_metadata <- df_metadata %>% 
    mutate(
      primary_key = case_when(
        source == "observation_lookup" ~ "concept_id", 
        TRUE ~ primary_key
        )
      ) 
  
  # Explore and join tables ----
  
  db$form %>% 
    left_join(db$patient, by = "patient_uuid")
  
  db$clinical_consultation %>% 
    left_join(db$observation, by = "encounter_uuid")
  
  
  # Build the datamodel from yaml file
  
  file_model <- return_latest("./Documents", "m.*data-model.yml")
  
  dm_y <- dm_read_yaml(file_model)
  
  #dm_y <- dm_add_references(dm = dm_y)
  
  # dm_y <- dm_add_references(
  #   dm_y,
  #   
  #   clinical_consultation$encounter_uuid == observation$encounter_uuid,
  #   clinical_consultation$encounter_uuid == medication$encounter_uuid,
  #   clinical_consultation$encounter_uuid == dsd$encounter_uuid,
  #   clinical_consultation$encounter_uuid == form$encounter_uuid,
  #   clinical_consultation$encounter_uuid == laboratory$encounter_uuid,
  #   clinical_consultation$encounter_uuid == key_vul_pop$encounter_uuid,
  #   
  #   patient$patient_uuid == identifier$patient_uuid,
  #   patient$patient_uuid == form$patient_uuid,
  #   patient$patient_uuid == pat_state$patient_uuid,
  #   pat_state$location_uuid == location$location_uuid,
  #   
  #   observation$concept_id == observation_lookup$concept_id,
  #   
  #   form$location_uuid == location$location_uuid
  #   
  # )
  
  dm_y %>% dm_draw()
  
  graph_y <- dm_create_graph(
    dm = dm_y,
    rankdir = "BT",
    graph_name = "MozART 2.0 Data Model",
    columnArrows = T
  )
  
  dm_render_graph(graph_y)
  
  ## 
  
  dm_x <- dm_from_con(file_model)
  
  dm_x <- dm(db$clinical_consultation, 
             db$dsd, 
             db$form, 
             db$identifier, 
             db$key_vul_pop, 
             db$laboratory, 
             db$location, 
             db$medication,
             db$observation, 
             db$observation_lookup, 
             db$pat_state, 
             db$patient, 
             db$type_id_lookup)
  
  dm_x %>% dm_draw(rankdir = "TB", view_type = "all")
  
  dm(dsd = db$dsd)
  
  db %>% as_dm()
  db %>% as_dm() %>% dm_examine_constraints()
  
  # Build the core Data Model
  dm_mozart <- dm() %>% 
    dm(db$clinical_consultation,
       db$dsd, 
       db$form, 
       db$identifier, 
       db$key_vul_pop, 
       db$laboratory, 
       db$location, 
       db$medication,
       db$observation, 
       db$observation_lookup, 
       db$pat_state, 
       db$patient, 
       db$type_id_lookup
    ) 
  
  # tables
  dm_mozart %>% names()
  
  # Add contraints: PK, UK, and FK
  dm_mozart <- dm_mozart %>% 
    # Table metadata
    dm_add_pk(`db$type_id_lookup`, 'id') %>% 
    dm_add_uk(`db$type_id_lookup`, c('table_name', 'column_name', 'id_type_lookup')) %>% 
    # Location
    dm_add_pk(`db$location`, 'id') %>% 
    dm_add_uk(`db$location`, 'location_uuid') %>% 
    # Forms
    dm_add_pk(`db$form`, 'id') %>% 
    dm_add_fk(`db$form`, 'form_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$form`, 'encounter_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$form`, 'encounter_type', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$form`, 'location_uuid', `db$location`, 'location_uuid') %>% 
    dm_add_fk(`db$form`, 'patient_uuid', `db$patient`, 'patient_uuid') %>% 
    # Clinical Consultations
    dm_add_pk(`db$clinical_consultation`, 'id') %>% 
    dm_add_uk(`db$clinical_consultation`, 'encounter_uuid') %>% 
    dm_add_fk(`db$clinical_consultation`, 'encounter_uuid', `db$form`, 'encounter_uuid') %>% 
    # DSD
    dm_add_pk(`db$dsd`, 'id') %>% 
    dm_add_uk(`db$dsd`, 'dsd_uuid') %>% 
    dm_add_fk(`db$dsd`, 'encounter_uuid', `db$form`, 'encounter_uuid') %>% 
    dm_add_fk(`db$dsd`, 'dsd_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$dsd`, 'dsd_state_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    # Clinical Observations
    dm_add_pk(`db$observation`, 'id') %>% 
    dm_add_uk(`db$observation`, 'obs_uuid') %>% 
    dm_add_pk(`db$observation_lookup`, 'concept_id') %>% 
    dm_add_fk(`db$observation`, 'concept_id', `db$observation_lookup`, 'concept_id') %>% 
    dm_add_fk(`db$observation`, 'value_concept_id', `db$observation_lookup`, 'concept_id') %>%
    dm_add_fk(`db$observation`, 'encounter_uuid', `db$clinical_consultation`, 'encounter_uuid') %>%
    # KP, OVC and GBV
    dm_add_pk(`db$key_vul_pop`, 'id') %>% 
    dm_add_fk(`db$key_vul_pop`, 'encounter_uuid', `db$form`, 'encounter_uuid') %>% 
    dm_add_fk(`db$key_vul_pop`, 'pop_type', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$key_vul_pop`, 'pop_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    # LAB
    dm_add_pk(`db$laboratory`, 'id') %>% 
    dm_add_fk(`db$laboratory`, 'encounter_uuid', `db$form`, 'encounter_uuid') %>% 
    dm_add_fk(`db$laboratory`, 'lab_test_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$laboratory`, 'result_qualitative_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$laboratory`, 'specimen_type_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    # Medications
    dm_add_pk(`db$medication`, 'id') %>% 
    dm_add_uk(`db$medication`, 'medication_uuid') %>% 
    dm_add_fk(`db$medication`, 'encounter_uuid', `db$form`, 'encounter_uuid') %>% 
    dm_add_fk(`db$medication`, 'regimen_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$medication`, 'reason_change_regimen_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$medication`, 'formulation_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$medication`, 'mode_dispensation_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$medication`, 'type_dispensation_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$medication`, 'med_line_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$medication`, 'arv_side_effects_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$medication`, 'adherence_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    # Identification
    dm_add_pk(`db$identifier`, 'identifier_seq') %>% 
    dm_add_fk(`db$identifier`, 'identifier_type', `db$type_id_lookup`, 'id_type_lookup') %>% 
    # Patients
    dm_add_pk(`db$patient`, 'id') %>% 
    dm_add_uk(`db$patient`, 'patient_uuid') %>% 
    dm_add_fk(`db$patient`, 'patient_uuid', `db$identifier`, 'patient_uuid') %>% 
    dm_add_fk(`db$patient`, 'patient_uuid', `db$pat_state`, 'patient_uuid') %>% 
    dm_add_fk(`db$patient`, 'gender', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$patient`, 'birthdate_estimated', `db$type_id_lookup`, 'id_type_lookup') %>% 
    # Patient Status
    dm_add_pk(`db$pat_state`, 'id') %>% 
    dm_add_uk(`db$pat_state`, 'state_uuid') %>% 
    dm_add_fk(`db$pat_state`, 'location_uuid', `db$location`, 'location_uuid') %>% 
    dm_add_fk(`db$pat_state`, 'program_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$pat_state`, 'source_id', `db$type_id_lookup`, 'id_type_lookup') %>% 
    dm_add_fk(`db$pat_state`, 'state_id', `db$type_id_lookup`, 'id_type_lookup') 
  
  # Check constraints
  #dm_mozart %>% dm_get_all_pks()
  #dm_mozart %>% dm_get_all_uks()
  #dm_mozart %>% dm_examine_constraints() %>% as_tibble()
  
  # View model through Shiny App
  #dm_gui(dm = dm_mozart)
  
  # Set colors
  dm_mozart <- dm_mozart %>% 
    dm_set_colors(
      darkgreen = c(`db$dsd`, `db$clinical_consultation`, `db$key_vul_pop`, 
              `db$laboratory`, `db$medication`, `db$observation`),
      lightgreen = c(`db$observation_lookup`, `db$clinical_consultation`), 
      blue = `db$form`,
      darkgray = c(`db$type_id_lookup`, `db$location`)
      )
  
  # Draw
  dm_mozart %>% dm_draw(rankdir = "LR", view_type = "all")

  
  
  
  
  
  # Couldn't figure out how to purrr the list df into the dm_from_data_frames function
  # paste output from chunk below into the dm_... function
  str_c("db$", names(db)) %>% cat(., sep = ", ")
  
  #str_c("db$", names(db)) %>% cat(., sep = ", ") %>% 
  #dm_from_data_frames(!!!db)
  
  # NOT SURE why it didn't work before but `dm_from_data_frames()` also takes a list
  dm_f <- dm_from_data_frames(db)
    
  dm_f <- dm_from_data_frames(db$clinical_consultation, 
                              db$dsd, 
                              db$form, 
                              db$identifier, 
                              db$key_vul_pop, 
                              db$laboratory, 
                              db$location, 
                              db$medication,
                              db$observation, 
                              db$observation_lookup, 
                              db$pat_state, 
                              db$patient, 
                              db$type_id_lookup)
  
  db$clinical_consultation %>% 
    tbl()
  
  #df_metadata %>% glimpse()
  
  dm_f <- dm_add_references(
    dm_f,
    
    clinical_consultation$encounter_uuid == observation$encounter_uuid,
    clinical_consultation$encounter_uuid == medication$encounter_uuid,
    clinical_consultation$encounter_uuid == dsd$encounter_uuid,
    clinical_consultation$encounter_uuid == form$encounter_uuid,
    clinical_consultation$encounter_uuid == laboratory$encounter_uuid,
    clinical_consultation$encounter_uuid == key_vul_pop$encounter_uuid,
    
    patient$patient_uuid == identifier$patient_uuid,
    patient$patient_uuid == form$patient_uuid,
    patient$patient_uuid == pat_state$patient_uuid,
    pat_state$location_uuid == location$location_uuid,
    
    observation$concept_id == observation_lookup$concept_id
    
  )
  
  # Setup a graphic depiction so we can plot and export
  graph <- dm_create_graph(dm_f, rankdir = "BT", col_attr = c("column", "type"))
  
  dm_render_graph(graph)
  
  # Export to a PDF for pushing around in AI
  dm_export_graph(graph, file_name = "Graphics/all_tables.pdf")

# MUNGE ============================================================================
  
  # Explore the names and common elements across data sets
    map(db, ~names(.x))
  
# VIZ ============================================================================

  #  

# SPINDOWN ============================================================================

