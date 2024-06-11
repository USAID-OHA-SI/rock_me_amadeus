# PROJECT: rock_me_amadeus
# AUTHOR:  Baboyma Kagniniwa | USAID/OHA/SIEI/SI
# PURPOSE: Access Configuration
# REF ID:  3b784ece 
# LICENSE: MIT
# DATE:    2024-04-24
# UPDATE:  2024-04-24
# NOTES:   Make sure you have access to a MySQL DB

# Libraries ====
  
  library(tidyverse)
  
# LOCALS & SETUP ====

  # Set Params

  ref_id <- "b8140e7e"
  
  # RDBMS Credentials 
  
  ## Admin access - set username to `admin`
  
  acct_admin <- "mysql-local-admin"
  
  ## TODO - Run this once only
  #glamr::set_account(name = acct_admin, keys = c("host", "port", "username", "password", "dbname"))
  
  acct <- glamr::get_account(name = acct_admin)
  
  names(acct)
  
  ## Developer access - set username to `developer`
  
  acct_dev <- "mysql-local-dev"
  
  ## TODO - Run this once only
  #glamr::set_account(name = acct_dev, keys = c("host", "port", "username", "password", "dbname"))
  
  acct <- glamr::get_account(name = acct_dev)
  
  names(acct)
  

  

# OUTPUTS =====

