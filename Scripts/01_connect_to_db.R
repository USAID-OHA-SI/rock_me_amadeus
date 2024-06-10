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

# STORE KEYS --------------------------------------------------------------
  
  acct_dev <- "mysql-local-dev"
  
  acct_db <- get_account(name = acct_dev)
  
  mozart2 <- "mozart_fgh_zam_test"
  
  #establish connection
  conn <- mysql_connection(db_name = mozart2, 
                           db_user = acct_db$username,
                           db_pass = acct_db$password, 
                           db_host = acct_db$host, 
                           db_port = acct_db$port)
  
  
  # Get detailed connection info
  DBI::dbGetInfo(conn)
  
  # List all tables
  tbls <- DBI::dbListTables(conn)
  
  tbls
  

  
  

