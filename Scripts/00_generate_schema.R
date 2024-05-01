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
    
# Connection ====
    
  glamr::get_services()
  
  acct_name <- "mysql-local-dev"
  
  dbname <- "mozart20"
    
  acct <- get_account(name = acct_name)
  
  # MySQL DB
  conn <- mysql_connection(
    db_host = acct$host,
    db_port = acct$port,
    db_name = dbname, # or use default db at acct$dbname
    db_user = acct$username,
    db_pass = acct$password
  )
  
  # SQLite
  #conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")
  
  # Get detailled connection info
  DBI::dbGetInfo(conn)
  
  # List tables
  DBI::dbListTables(conn)

    
# LOAD DATA ============================================================================  

  # Bind all tables into a named list
  db <- map(setNames(df_list, df_list %>% str_remove_all(., "df_|.gz")), 
            ~readRDS(file.path(mozART, .x)))
    
  # Take a pick at all tables
  db %>% glimpse_all()
  
  # OPTION #1 - use of datamodelr ----
  
  # Couldn't figure out how to purrr the list df into the dm_from_data_frames function
  # paste output from chunk below into the dm_... function
  str_c("db$", names(db)) %>% cat(., sep = ", ")
  
  # dm_from_data_frames() also takes a list
  #dm_f <- dm_from_data_frames(db)
  
  dm_f <- dm_from_data_frames(db$clinical_consultation, db$dsd, db$form, db$identifier, 
                              db$key_vul_pop, db$laboratory, db$location, db$medication, 
                              db$observation, db$observation_lookup, db$pat_state, 
                              db$patient, db$type_id_lookup)
  
  # Setup a graphic depiction so we can plot and export
  graph <- dm_create_graph(dm_f, col_attr = c("column", "type"))
  dm_render_graph(graph)
  
  # Export to a PDF for pushing around in AI
  dm_export_graph(graph, file_name = "Graphics/all_tables.pdf")
  
  # OPTION #2 - use of datamodelr and load model from yaml file ----
  
  # Track down all tables from data dict ----
  
  # List DM tables 
  db_tables <- df_list %>% 
    map_chr(~str_remove_all(.x, "df_|.gz")) 
  
  #db_tables %>% clipr::write_clip()
  
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
      "Location data",
      "Contains HIV medications prescribed and dispensed for a patient in SESP. The columns are populated based on the form data source and will change
overtime. In some cases, the encounter_type will need to be used in conjunction with the form_id to specify the source",
      "Contains data not captured in other tables, such as pregnancy and breastfeeding. A significant majority of these The
OBSERVATION_LOOKUP table provides the descriptions for every observation id, which is called a concept id in Open MRS SESP. To minimize
the transformation from Open MRS SESP to Mozart, Open MRS SESP names and data structures were maintained",
      "Observation types",
      "Captures the patient state information that is collected in the program enrollment module and on fichas. The Program Enrollment Module in SESP is
an independent module used when a patient is registered for a service and is considered the starting point for registration in SESP.",
      "Patient data",
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
  
  
  ## Output YAML Format for DM ----
  names(db) %>% 
    walk(function(.x) {
      cat(glue("\n\n- table: {.x}\n\n"),
        "\tsegment: *dim\n",
        "\tcolumns:\n",
        glue("{paste0('\t\t', names(db[[.x]]), collapse = ':\n')}:\n\n"),
        set = "\n"
      )
    }) 
  
  # Create empty yaml file and copy console output to it
  yaml::write_yaml(NULL, file = "./Documents/data-model.yml")
  
  file.edit("./Documents/data-model.yml")

  # Build the data model from yaml file
  
  file_model <- return_latest("./Documents", "m.*data-model.yml")
  
  dm_y <- dm_read_yaml(file_model)
  
  graph_y <- dm_create_graph(
    dm = dm_y,
    rankdir = "BT",
    graph_name = "MozART 2.0 Data Model",
    columnArrows = T
  )
  
  dm_render_graph(graph_y)
  
  # OPTION #3- use of dm package
  
  # List table / columns structure ----
  
  names(db) %>% 
    walk(function(.x) {
      cat(glue("\n\n{which(names(db) == .x)} - {.x}\n\n\n"))
      tbl <- get_structure(db[[.x]])
      print(tbl, n = Inf)
    })
  
  db$form %>% glimpse()
  
  # Build the core Data Model
  dm_mozart <- dm(
    "clinical_consultation" = db$clinical_consultation,
    "dsd" = db$dsd, 
    "form" = db$form, 
    "identifier" = db$identifier, 
    "key_vul_pop" = db$key_vul_pop, 
    "laboratory" = db$laboratory, 
    "location" = db$location, 
    "medication" = db$medication,
    "observation" = db$observation, 
    "observation_lookup" = db$observation_lookup, 
    "pat_state" = db$pat_state, 
    "patient" = db$patient, 
    "type_id_lookup" = db$type_id_lookup
  ) 
  
  # tables
  dm_mozart %>% names()
  #dm_mozart %>% dm_get_tables()
  
  # Add contraints: PK, UK, and FK
  dm_mozart <- dm_mozart %>% 
    # Table metadata
    dm_add_pk(type_id_lookup, 'id') %>% 
    dm_add_uk(type_id_lookup, c('table_name', 'column_name', 'id_type_lookup')) %>% 
    # Location
    dm_add_pk(location, 'id') %>% 
    dm_add_uk(location, 'location_uuid') %>% 
    # Forms
    dm_add_pk(form, 'id') %>% 
    dm_add_fk(form, 'form_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(form, 'encounter_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(form, 'encounter_type', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(form, 'location_uuid', location, 'location_uuid') %>% 
    dm_add_fk(form, 'patient_uuid', patient, 'patient_uuid') %>% 
    # Clinical Consultations
    dm_add_pk(clinical_consultation, 'id') %>% 
    dm_add_uk(clinical_consultation, 'encounter_uuid') %>% 
    dm_add_fk(clinical_consultation, 'encounter_uuid', form, 'encounter_uuid') %>% 
    # DSD
    dm_add_pk(dsd, 'id') %>% 
    dm_add_uk(dsd, 'dsd_uuid') %>% 
    dm_add_fk(dsd, 'encounter_uuid', form, 'encounter_uuid') %>% 
    dm_add_fk(dsd, 'dsd_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(dsd, 'dsd_state_id', type_id_lookup, 'id_type_lookup') %>% 
    # Clinical Observations
    dm_add_pk(observation, 'id') %>% 
    dm_add_uk(observation, 'obs_uuid') %>% 
    dm_add_pk(observation_lookup, 'concept_id') %>% 
    dm_add_fk(observation, 'concept_id', observation_lookup, 'concept_id') %>% 
    dm_add_fk(observation, 'value_concept_id', observation_lookup, 'concept_id') %>%
    dm_add_fk(observation, 'encounter_uuid', clinical_consultation, 'encounter_uuid') %>%
    # KP, OVC and GBV
    dm_add_pk(key_vul_pop, 'id') %>% 
    dm_add_fk(key_vul_pop, 'encounter_uuid', form, 'encounter_uuid') %>% 
    dm_add_fk(key_vul_pop, 'pop_type', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(key_vul_pop, 'pop_id', type_id_lookup, 'id_type_lookup') %>% 
    # LAB
    dm_add_pk(laboratory, 'id') %>% 
    dm_add_fk(laboratory, 'encounter_uuid', form, 'encounter_uuid') %>% 
    dm_add_fk(laboratory, 'lab_test_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(laboratory, 'result_qualitative_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(laboratory, 'specimen_type_id', type_id_lookup, 'id_type_lookup') %>% 
    # Medications
    dm_add_pk(medication, 'id') %>% 
    dm_add_uk(medication, 'medication_uuid') %>% 
    dm_add_fk(medication, 'encounter_uuid', form, 'encounter_uuid') %>% 
    dm_add_fk(medication, 'regimen_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(medication, 'reason_change_regimen_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(medication, 'formulation_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(medication, 'mode_dispensation_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(medication, 'type_dispensation_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(medication, 'med_line_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(medication, 'arv_side_effects_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(medication, 'adherence_id', type_id_lookup, 'id_type_lookup') %>% 
    # Identification
    dm_add_pk(identifier, 'identifier_seq') %>% 
    dm_add_fk(identifier, 'identifier_type', type_id_lookup, 'id_type_lookup') %>% 
    # Patients
    dm_add_pk(patient, 'id') %>% 
    dm_add_uk(patient, 'patient_uuid') %>% 
    dm_add_fk(patient, 'patient_uuid', identifier, 'patient_uuid') %>% 
    dm_add_fk(patient, 'patient_uuid', pat_state, 'patient_uuid') %>% 
    dm_add_fk(patient, 'gender', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(patient, 'birthdate_estimated', type_id_lookup, 'id_type_lookup') %>% 
    # Patient Status
    dm_add_pk(pat_state, 'id') %>% 
    dm_add_uk(pat_state, 'state_uuid') %>% 
    dm_add_fk(pat_state, 'location_uuid', location, 'location_uuid') %>% 
    dm_add_fk(pat_state, 'program_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(pat_state, 'source_id', type_id_lookup, 'id_type_lookup') %>% 
    dm_add_fk(pat_state, 'state_id', type_id_lookup, 'id_type_lookup') 
  
  # Check constraints
  dm_mozart %>% dm_get_all_pks()
  dm_mozart %>% dm_get_all_uks()
  dm_mozart %>% dm_get_all_fks()
  
  #dm_mozart %>% dm_examine_constraints() %>% as_tibble()
  
  # View model through Shiny App
  #dm_gui(dm = dm_mozart)
  
  # Set colors
  dm_mozart <- dm_mozart %>% 
    dm_set_colors(
      darkgreen = c(dsd, clinical_consultation, key_vul_pop, 
              laboratory, medication, observation),
      lightgreen = c(observation_lookup, clinical_consultation), 
      blue = form,
      darkgray = c(type_id_lookup, location)
    )
  
  # Validate
  dm_mozart %>% dm_validate()
  
  # Draw
  dm_graph <- dm_mozart %>% dm_draw(rankdir = "LR", view_type = "all")
  
  dm_graph
  
  # Flatten - use join to flatten related tables
  dm_mozart %>% dm_flatten_to_tbl(.start = dsd, form)
  dm_mozart %>% dm_flatten_to_tbl(.start = clinical_consultation, form)
  
  # DB Schema
  
  DBI::dbListTables(conn)
  
  # DB Build tables
  
  pks <- dm_mozart %>% dm_get_all_pks() 
  fks <- dm_mozart %>% dm_get_all_fks()
  
  names(db) %>% 
    walk(function(.x) {
      
      cat(glue("\n\n{which(names(db) == .x)} - {.x}\n\n\n"))
      
      # Get table structure
      tbl <- get_structure(.df = db[[.x]], pks = pks, tblname = .x)
      
      tbl_fields <- tbl %>% 
        pull(definition) %>% 
        rlang::set_names(nm = tbl$var)
      
      # If table exist drop it
      if (DBI::dbExistsTable(conn = conn, name = .x)) {
        print(glue("Dropping table: {.x}"))
        DBI::dbRemoveTable(conn = conn, name = .x)
      }
      
      # Create table only
      
      # DBI::dbCreateTable(
      #   conn = conn,
      #   name = .x,
      #   fields = tbl_fields,
      #   value = db[[.x]],
      #   row.names = F,
      #   overwrite = TRUE
      # )
      
      # Create table and load data
      
      DBI::dbWriteTable(
        conn = conn,
        name = .x,
        fields = tbl_fields,
        value = db[[.x]],
        row.names = F,
        overwrite = T
      )
    })
  
  # Apply all foreign keys to physical database
  
  fks %>% glimpse()
  
  # fks %>% 
  #   distinct(child_table) %>% 
  #   pull() %>% 
  #   walk(function(.x){
  #     print(.x)
  #   })
  
  # Generate SQL Statements and load data
  # dm_mozart %>% dm_ddl_pre(dest = conn)
  # dm_mozart %>% dm_ddl_load(dest = conn)
  # dm_mozart %>% dm_ddl_post(dest = conn)
  
  # or this
  #dm_mozart %>% dm_sql(dest = conn)
  
  conn %>% dplyr::tbl('form')
  

# MUNGE ============================================================================
  
  # Explore the names and common elements across data sets
    map(db, ~names(.x))
  
# VIZ ============================================================================

  #  

# SPINDOWN ============================================================================

