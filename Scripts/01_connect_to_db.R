# AUTHOR:   K. Srikanth | USAID
# PURPOSE:  set up script to intiate SQL connection
# REF ID:   140660ed 
# LICENSE:  MIT
# DATE:     2023-05-02
# UPDATED: 

# DEPENDENCIES ------------------------------------------------------------
  
library(tidyverse)
library(glamr)
library(gophr)
library(DBI)

# GLOBAL VARIABLES --------------------------------------------------------
  
  ref_id <- "140660ed"
  
  source("Scripts/00_utilities.R")
  
  mysql_connect <- function(db_name, db_user, db_pass,
                            db_host,db_port) {
    ## Connections
    DBI::dbConnect(
      drv = RMySQL::MySQL(),
      host = db_host,
      port = as.integer(db_port),
      dbname = db_name,
      username = db_user,
      password = db_pass
    )
  }
  

# STORE KEYS --------------------------------------------------------------
  
  #ONE TIME ONLY
  set_key(service = pg_service("mozart2"), "host")
  set_key(service = pg_service("mozart2"), "port")
  set_key(service = pg_service("mozart2"), "database")
  set_key(service = pg_service("mozart2"), "username")
  set_key(service = pg_service("mozart2"), "password")
  
  #store keys
  db_host <-pg_host("mozart2")
  db_port <- pg_port("mozart2")
  db_name <- pg_database("mozart2")
  db_user <- pg_user("mozart2")
  db_pwd <- pg_pwd("mozart2")
  
  #establish connection
  conn <- mysql_connect(db_name = "mozart2", db_user = db_user,
                        db_pass = db_pwd, db_host = db_host, db_port = db_port)
  
  
  # Get detailled connection info
  DBI::dbGetInfo(conn)
  
  # List tables
  DBI::dbListTables(conn)
  
  
  dplyr::tbl(conn, "dsd") %>%
    tibble::as_tibble()
  

  
  

