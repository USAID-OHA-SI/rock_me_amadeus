library(magrittr)

#' @title Check host availability
#'
#' @param host Hostname
#'
check_host <- function(host) {
  pingr::is_online(host)
}


#' @title Postgres Environment
#'
#' @param env Name of environment service
#'
pg_service <- function(env = "local") {
  base::paste0(env, "-postgres")
}


#' @title Postgres Database's host
#'
#' @param env Name of the environment, default set to local
#'
pg_host <- function(env = "local") {
  svc <- pg_service(env)
  get_key(service = svc, name = "host")
}


#' @title Postgres Database's port
#'
pg_port <- function(env = "local") {
  svc <- pg_service(env)
  get_key(service = svc, name = "port")
}


#' @title Postgres Database's name
#'
pg_database <- function(env = "local") {
  svc <- pg_service(env)
  get_key(service = svc, name = "database")
}


#' @title Postgres user's name
#'
pg_user <- function(env = "local") {
  svc <- pg_service(env)
  get_key(service = svc, name = "username")
}


#' @title Postgres user's password
#'
pg_pwd <- function(env = "local") {
  svc <- pg_service(env)
  get_key(service = svc, name = "password")
}

#' @title Postgres Connection
#'
#' @param db_host
#' @param db_port
#' @param db_database
#' @param db_user
#' @param db_pwd
#'
pg_connection <- function(db_driver = RPostgres::Postgres(),
                          db_host = pg_host(),
                          db_port = pg_port(),
                          db_name = pg_database(),
                          db_user = pg_user(),
                          db_pwd = pg_pwd()) {
  
  DBI::dbConnect(drv = db_driver,
                 host = db_host,
                 port = db_port,
                 dbname = db_name,
                 user = db_user,
                 password = db_pwd)
  
}


#' @title Establish a connection
#'
#' @param db_host
#' @param db_port
#' @param db_database
#' @param db_user
#' @param db_pwd
#' @param db_file
#'
db_connection <- function(db_driver = RPostgres::Postgres(),
                          db_host = pg_host(),
                          db_port = pg_port(),
                          db_name = pg_database(),
                          db_user = pg_user(),
                          db_pwd = pg_pwd(),
                          db_file = NULL) {
  
  conn <- NULL
  
  if (str_detect(db_name, ".*[.]db$|.*[.]sqlite$|.*[.]sqlite3$")) {
    conn <- RSQLite::dbConnect(RSQLite::SQLite(), db_name)
  } else if("MySQLDriver" %in% class(db_driver)) {
    conn <- RMySQL::dbConnect(db_driver,
                              host = db_host,
                              port = db_port,
                              dbname = db_name,
                              user = db_user,
                              password = db_pwd)
  } else {
    conn <- pg_connection(db_driver, db_host, db_port, db_name, db_user, db_pwd)
  }
  
  return(conn)
}

#' @title Connect to MySQL DB
#' 
mysql_connection <- function(db_name, db_user, db_pass,
                          db_host = "localhost",
                          db_port = 3306) {
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


#' @title Retrieve database name from a connection object
#'
#' @param conn DBI::dbConnect instance
#'
db_name <- function(conn) {
  
  conn_info <- DBI::dbGetInfo(conn)
  
  name <- conn_info$dbname
  
  return(name)
}


#' @title Connect to database
#'
#' @description TODO - Switch to a new database
#'
db_connect <- function(name, conn) {
  DBI::dbGetInfo(conn)$dbname <- name
  return(name)
}


#' @title List of Databases
#'
db_list <- function(conn) {
  
  sql_cmd <- '
    SELECT * FROM pg_database
    WHERE datallowconn = true
    AND datistemplate = false;
  '
  
  query <- DBI::dbGetQuery(conn, sql_cmd)
  
  query %>%
    tibble::as_tibble() %>%
    dplyr::pull(datname) %>%
    base::sort()
}


#' @title Get current database
#'
#'
db_in_use <- function(conn) {
  conn_info <- DBI::dbGetInfo(conn)
  db_name <- conn_info$dbname
  
  return(db_name)
}


#' @title List database schemas
#'
db_schemas <- function(conn) {
  
  db_name <- db_in_use(conn)
  
  usethis::ui_info(glue::glue("Current Database [{db_name}]"))
  
  sql_cmd <- "
    SELECT DISTINCT table_catalog, table_schema
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    AND table_catalog = $1
    ORDER BY table_schema;
  "
  
  query <- DBI::dbGetQuery(conn, sql_cmd, params = base::list(db_name))
  
  schemas <- query %>%
    tibble::as_tibble() %>%
    dplyr::pull(table_schema) %>%
    base::sort()
  
  return(schemas)
}


#' @title Create new database schema
#'
db_schema <- function(conn, name) {
  
  db_name <- db_in_use(conn)
  
  usethis::ui_info(glue::glue("Current Database [{db_name}]"))
  usethis::ui_info(glue::glue("Adding new schema [{name}]"))
  
  sql_cmd <- glue::glue("CREATE SCHEMA IF NOT EXISTS {name};")
  
  tryCatch(
    expr = {
      DBI::dbSendQuery(conn, sql_cmd)
    },
    error = function(err) {
      print(err)
      usethis::ui_stop("ERROR")
    }
  )
}


#' @title List database and schema tables
#'
db_tables <- function(conn,
                      schema = NULL,
                      details = FALSE) {
  
  db_name <- db_in_use(conn)
  
  #usethis::ui_info(glue::glue("Current Database [{db_name}]"))
  
  sql_cmd <- "
    SELECT DISTINCT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    AND table_catalog = $1
    ORDER BY table_name;
  "
  
  query <- DBI::dbGetQuery(conn, sql_cmd, params = list(db_name))
  
  tbls <- query %>% tibble::as_tibble()
  
  if (!base::is.null(schema)) {
    tbls <- tbls %>% dplyr::filter(table_schema == schema)
  }
  
  if (details) {
    return(tbls)
  }
  
  tbls <- tbls %>%
    dplyr::mutate(
      name = dplyr::case_when(
        table_schema == "public" ~ table_name,
        TRUE ~ paste0(table_schema, ".", table_name)
      )
    ) %>%
    dplyr::pull(name)
  
  return(tbls)
}

#' @title Check if table exists
#'
#'
db_table_exists <- function(conn, tbl_name) {
  
  tbls <- db_tables(conn)
  
  tbl_name %in% tbls
}


#' @title Get Table full name
#'
#'
db_table_id <- function(tbl_name) {
  
  # Extract schema
  if(str_detect(tbl_name, ".*[.].*")) {
    schema <- str_extract(tbl_name, ".*(?=\\.)")
    name <- str_extract(tbl_name, "(?<=\\.).*")
  } else {
    schema <- "public"
    name <- tbl_name
  }
  
  DBI::Id(schema = schema, table = name)
}


#' @title
#'
db_create_table <- function(tbl_name, fields,
                            conn = NULL,
                            meta = NULL,
                            pkeys = NULL,
                            overwrite = FALSE,
                            load_data = TRUE) {
  # Check Connection
  if (is.null(conn))
    conn <- db_connection()
  
  db_name <- db_in_use(conn)
  
  usethis::ui_info(glue::glue("Current Database [{db_name}]"))
  
  # Extract schema
  if(str_detect(tbl_name, ".*[.].*")) {
    schema <- str_extract(tbl_name, ".*(?=\\.)")
    name <- str_extract(tbl_name, "(?<=\\.).*")
  } else {
    schema <- "public"
    name <- tbl_name
  }
  
  tbl_full_name <- db_table_id(tbl_name)
  
  # Check existing tables
  usethis::ui_info(glue::glue("Schema: {schema}"))
  usethis::ui_info(glue::glue("Table: {name}"))
  
  tbls <- db_tables(conn, schema)
  
  tbl_exists <- tbl_name %in% tbls
  
  usethis::ui_info(glue::glue("Exists: {tbl_exists}"))
  
  # Check / Set columns data types
  if (!base::is.null(meta)) {
    if (!all(names(fields) %in% names(meta))) {
      cols_missing <- setdiff(
        names(fields), names(meta[names(fields) %in% names(meta)])) %>%
        base::paste(collapse = ", ")
      
      usethis::ui_stop(glue::glue("Missing column(s) from {tbl_name}: {cols_missing}"))
    }
    
    cols <- meta[names(meta) %in% names(fields)]
  }
  
  # Drop Table
  if(overwrite & tbl_exists) {
    db_drop_table(tbl_name, conn)
  }
  
  # Create table
  usethis::ui_info("Creating table ...")
  
  if (!base::is.null(meta)) {
    DBI::dbCreateTable(conn, tbl_full_name, fields = cols)
    
  } else {
    DBI::dbCreateTable(conn, tbl_full_name, fields = fields)
  }
  
  # Add Primary Keys
  if (!base::is.null(pkeys)) {
    
    pkeys <- base::paste(pkeys, collapse = ", ")
    
    usethis::ui_info(glue::glue("Adding PKEY(s): {pkeys}"))
    
    cmd <- glue::glue_sql("ALTER TABLE {SQL(tbl_name)} ADD PRIMARY KEY ({SQL(pkeys)});",
                          .con = conn)
    
    res <- DBI::dbExecute(conn, cmd)
    
    base::stopifnot(res == 0)
  }
  
  # Insert data
  if(load_data & is.data.frame(fields) & nrow(fields) > 0) {
    usethis::ui_info(glue::glue("Appending data ... {nrow(fields)}"))
    DBI::dbAppendTable(conn, tbl_full_name, fields)
  }
  
  usethis::ui_done("Complete!")
}

#' @title Update / Alter Table Add PKEYs
#'
#'
db_update_table <- function(tbl_name,
                            conn = NULL,
                            pkeys = NULL,
                            fkeys = NULL) {
  
  if (base::is.null(pkeys) & base::is.null(fkeys)) {
    usethis::ui_stop("Missing one of the required keys: primary or foreign")
  }
  
  # Add Primary Keys
  if (!base::is.null(pkeys)) {
    
    pkeys <- base::paste(pkeys, collapse = ", ")
    
    usethis::ui_info(glue::glue("Adding PKEY(s): {pkeys}"))
    
    cmd <- glue::glue_sql("ALTER TABLE {DBI::SQL(tbl_name)} ADD PRIMARY KEY ({DBI::SQL(pkeys)});",
                          .con = conn)
    
    res <- DBI::dbExecute(conn, cmd)
    
    base::stopifnot(res == 0)
  }
  
  # Add Foreign Keys
  if (!base::is.null(fkeys)) {
    
    fkeys %>%
      names() %>%
      walk(function(.k) {
        
        ftbl <- .k
        pkeys <- base::paste(fkeys[[.k]], collapse = ", ")
        fkeys <- base::paste(fkeys[[.k]], collapse = ", ")
        
        usethis::ui_info(glue::glue("Adding Foreign Key(s): {tbl_name} ({pkeys}) => {ftbl} ({fkeys})"))
        
        # cmd <- glue::glue_sql("ALTER TABLE {DBI::SQL(tbl_name)} ADD FOREIGN KEY ({DBI::SQL(pkeys)}) REFERENCES {DBI::SQL(ftbl)} ({DBI::SQL(fkeys)});",
        #                       .con = conn)
        
        cmd <- DBI::SQL(glue::glue("ALTER TABLE {tbl_name} ADD FOREIGN KEY ({pkeys}) REFERENCES {ftbl} ({fkeys});",
                                   .con = conn))
        
        print(cmd)
        
        res <- DBI::dbExecute(conn, cmd)
        
        base::stopifnot(res == 0)
      })
  }
  
}

#' @title Drop Table from Database
#'
db_drop_table <- function(tbl_name, conn = NULL) {
  # Check Connection
  if (is.null(conn))
    conn <- db_connection()
  
  db_name <- db_in_use(conn)
  
  tbl_full_name <- db_table_id(tbl_name)
  
  # Check table exists before dropping
  if (db_table_exists(conn, tbl_name)) {
    
    usethis::ui_info(glue::glue("Dropping table [{tbl_name}] from [{db_name}] ..."))
    
    # Drop Table
    DBI::dbRemoveTable(conn, tbl_full_name)
    
    usethis::ui_done("Table Dropped!")
    
  } else {
    usethis::ui_stop(glue::glue("Table [{tbl_name}] does not exist in database [{db_name}]"))
  }
}

#' @title Create a new Database
#'
#' @param name
#' @param conn
#' @param owner
#'
db_create <- function(name,
                      conn = NULL,
                      owner = NULL) {
  # DB Connection
  if (is.null(conn))
    conn = db_connection()
  
  # Exclude SQLite/File based database
  if (stringr::str_detect(db_name(conn), ".db$|.sqlite$"))
    usethis::ui_stop("Invalid Input - File based connection do not support multiple databases")
  
  # Build query
  query <- glue::glue_sql("CREATE DATABASE {`name`}", .con = conn)
  
  if (!is.null(owner))
    query <- glue::glue_sql("CREATE DATABASE {`name`} WITH OWNER = {`owner`};",
                            .con = conn)
  
  # Execute query statement
  res <- tryCatch(
    expr = {DBI::dbExecute(conn, query)},
    warning = function(w){
      usethis::ui_warn("SQL warning: {query}")
      print(w)
    },
    error = function(e){
      usethis::ui_oops("SQL error: {query}")
      stop(e)
    }
  )
  
  return(res)
}

#' @title
db_munge <- function() {}

#' @title
db_query <- function() {}

#' @title
db_control <- function() {}

#' @title
#'
find_pkeys <- function(.df, colnames = FALSE) {
  
  combos <- .df %>%
    base::names() %>%
    purrr::accumulate(c)
  
  checks <- combos %>%
    map(syms) %>%
    map_lgl(~{
      .df %>%
        add_count(!!!.x) %>%
        filter(.data$n > 1) %>%
        nrow() %>%
        equals(0)
    })
  
  if (colnames)
    return(subset(combos, checks))
  
  which(checks)
}


#' @title Check if URL is valid
#'
host_ping <- function(base_url) {
  
  p_url <- httr::parse_url(base_url)
  host <- p_url$hostname
  res_p <- pingr::ping(host)
  check <- !base::any(base::is.na(res_p))
  
  if(!check) {
    msg <- stringr::str_c(base_url, " does not seem to be responding. Check your base url.")
    base::message(msg)
  }
  
  return(check)
}

#' @title Check is resource is online
#'
host_is_online <- function(base_url){
  pingr::is_online(base_url)
}


#' @title Get longest row from a dataframe variable
#' 
#' 
get_chr_max <- function(.df, var) {
  .df[[var]] %>% nchar(keepNA = F) %>% max(na.rm = T)
}


#' @title Get Data Structure for MySQL Import
#' 
#' 
get_structure <- function(.df) {
  .df %>% 
    purrr::map(class) %>% 
    tibble::as_tibble() %>% 
    tidyr::pivot_longer(
      cols = everything(),
      names_to = "var",
      values_to = "type") %>% 
    dplyr::rowwise() %>% 
    dplyr::mutate(
      len = dplyr::case_when(
        #type == "character" ~ .df[[!!var]] %>% nchar() %>% max(na.rm = TRUE),
        type == "character" ~ get_chr_max(.df = .df, var = var),
        type == "logical" ~ 1,
        TRUE ~ NA
      )
    ) %>% 
    dplyr::ungroup()
}

#' @title List columns & data types from all tables in names list
#' 
glimpse_all <- function(.df_list){
  names(.df_list) %>% 
    walk(function(.x) {
      cat(glue("\n\n{which(names(db) == .x)} - {.x}\n\n\n"))
      glimpse(db[[.x]])
      #print(glimpse(db[[.x]]))
    })
}

#' @title Add Foreign Key to table
#' 
#' 
add_foreign_keys <- function(conn, tbl_src, tbl_dst, col_src, col_dst) {
  
  fk_sql <- paste0("ALTER TABLE ", 
                   tbl_src, 
                   " ADD CONSTRAINT ",
                   paste0("fk_", tbl_src),
                   " FOREIGN KEY (", 
                   paste(col_src, collapse = ", "), 
                   ") REFERENCES ", 
                   tbl_dst,
                   " (",
                   paste(col_dst, collapse = ", "),
                   ") ON DELETE CASCADE ON UPDATE RESTRICT;")
  
  print(fk_sql)
  
  DBI::dbSendQuery(conn, fk_sql)
}
