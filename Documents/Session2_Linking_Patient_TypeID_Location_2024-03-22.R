library(odbc)
library(RODBC)
library(DBI)
library(sqldf)
library(dplyr)
library(rstudioapi)
library(tidyr)
library(lubridate)

ConnMySQL_part <- dbConnect(odbc(),
                            Driver = "MySQL ODBC 8.0 ANSI Driver",
                            Server = "mozart.fgh.org.mz",
                            Port = 49760,
                            Database = "mozart_q1_fy24_consolidated",
                            user = rstudioapi::askForPassword("Please enter username"),
                            password = rstudioapi::askForPassword("Please enter password"),
                            MULTI_HOST = 1)


#Define date at which you want to assess number of people active on treatment
enddate <- '2023-12-20'

#Establish cutoff dates for defining whether someone is active
next_pickup_date_cutoff <-as.Date(enddate) - days(28)
medication_pickup_date_cutoff <- as.Date(enddate) - days(58)

#Include a ceiling date to ensure erroneous dates are not included in pull
ceiling <- as.Date(enddate) + years(1)

#Establish which columns should be pulled from medication table,
sql_query <- paste(
  "WITH med_dispensations AS (
    SELECT
    m.location_uuid,
    m.encounter_uuid,
    m.patient_uuid,
    p.birthdate,
    p.gender,
    m.form_id,
    m.regimen_id,
    m.medication_pickup_date,
    m.next_pickup_date,
    m.mode_dispensation_id,
    m.med_sequence_id,
    m.type_dispensation_id,
    m.alternative_line_id,
    m.reason_change_regimen_id,
    med_side_effects_id,
    m.adherence_id,
    ROW_NUMBER() OVER (PARTITION BY p.patient_uuid ORDER BY m.next_pickup_date DESC, m.medication_pickup_date DESC) AS row_num
    FROM
    medication_wfd PARTITION(medication_wfd_y2023) m
    LEFT JOIN patient p ON m.patient_uuid = p.patient_uuid
    WHERE (m.next_pickup_date > '",next_pickup_date_cutoff," 00:00:00' AND m.next_pickup_date < '",ceiling," 00:00:00' AND m.form_id = 130) OR
    (m.medication_pickup_date > '",medication_pickup_date_cutoff," 00:00:00' AND m.medication_pickup_date < '",ceiling," 00:00:00' AND m.form_id = 166))
    
    SELECT *
    FROM med_dispensations
    WHERE row_num = 1;"
)

#Run SQL pull from MozART database
active_last_pickup <- dbGetQuery(ConnMySQL_part, sql_query)

#Pulling in and cleaning id look up table
type_id_lookup <- sqldf("SELECT * FROM type_id_lookup", connection = ConnMySQL_part)
type_id_lookup <- type_id_lookup %>%
  filter(column_name != "GENDER") %>% 
  mutate(id_type_lookup = as.integer(id_type_lookup)) %>% 
  arrange(table_name, column_name, id_type_lookup) %>%
  mutate(column_name = tolower(column_name)) %>%
  mutate(column_name = case_when(
    column_name == "med_line_id" ~ "med_sequence_id",
    column_name == "arv_side_effect_id" ~ "med_side_effects_id",
    TRUE ~ column_name
  ))

#Applying id descriptions to form_id, regimen_id and mode_dispensation_id columns
active_last_pickup <- active_last_pickup %>% 
  left_join(type_id_lookup %>% filter(column_name == "form_id") %>% select(id_type_lookup, id_type_desc), by = c("form_id" = "id_type_lookup")) %>% 
  rename(form = id_type_desc) %>%
  left_join(type_id_lookup %>% filter(column_name == "regimen_id") %>% select(id_type_lookup, id_type_desc), by = c("regimen_id" = "id_type_lookup")) %>% 
  rename(regimen = id_type_desc) %>%
  left_join(type_id_lookup %>% filter(column_name == "mode_dispensation_id") %>% select(id_type_lookup, id_type_desc), by = c("mode_dispensation_id" = "id_type_lookup")) %>% 
  rename(mode_dispensation = id_type_desc)
  

#Determining number of clients on each ARV regimen
active_last_pickup %>% group_by(form_id, regimen) %>% count() %>% arrange(desc(n)) %>% View()

#Applying Age categories
active_last_pickup <- active_last_pickup %>%
  mutate(age = floor(as.numeric(difftime(as.Date(medication_pickup_date), birthdate, units = "days")) / 365.25)) %>%
  mutate(fineAge = case_when(
    age < 1 ~ "<01",
    age >= 1 & age < 5 ~ "01-04",
    age >= 5 & age < 10 ~ "05-09",
    age >= 10 & age < 14 ~ "10-14",
    age >= 14 & age < 20 ~ "15-19",
    age >= 20 & age < 25 ~ "20-24",
    age >= 25 & age < 30 ~ "25-29",
    age >= 30 & age < 35 ~ "30-34",
    age >= 35 & age < 40 ~ "35-39",
    age >= 40 & age < 45 ~ "40-44",
    age >= 45 & age < 50 ~ "45-49",
    age >= 50 & age < 55 ~ "50-54",
    age >= 55 & age < 60 ~ "55-59",
    age >= 60 & age < 65 ~ "60-64",
    age >= 65 ~ "65+"
  )) %>%
  mutate(courseAge = case_when(
    age < 15 ~ "<15",
    age >= 15 ~ "15+",
    TRUE ~ NA_character_
  ))

#Linking to a location table
location_lookup <- read.csv(file="location_lookup_clean_FY24Q1.csv")
active_last_pickup_sites <- active_last_pickup %>% right_join(location_lookup, by=c("location_uuid"))

#Rolling up into site-level statistics
tx_curr_disaggs_FY24Q1 <- active_last_pickup_sites %>% 
  group_by(datim_id, sisma_id, province, district, datim_name, form, regimen, mode_dispensation, gender, fineAge) %>%
  count() %>% rename(TX_CURR = n) %>% ungroup()
