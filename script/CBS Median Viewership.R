library(AMGutils)
library(tidyverse)
library(RPostgreSQL)


library(DBI)
# Connect to a specific postgres database i.e. Heroku
db <- dbConnect(
  RPostgres::Postgres(),
  dbname = 'amg',
  host = 'angband.amg.sh',
  # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
  port = 5439,
  # or any other port specified by your DBA
  user = 'eabasolo',
  password = pass
)

ign <-
  DBI::dbGetQuery(
    db,
    "set search_path to restricted_viewership_zone_eabasolo, experian, media, thehive, pep, universes;"
  )


###### ------  Tables ------ ######

pt_tbl <- tbl(db, "program_tunings")
networks <- tbl(db, "networks")
series <- tbl(db, "series")
programs <- tbl(db, "programs")
stations <- tbl(db, "stations")
char_to_exp <- tbl(db, "char_exp_synthetic_crosswalk")

output_table_name <- "cbs_segment"

pt <- pt_tbl %>%
  select(
    media_market_id,
    station_id,
    series_id,
    program_id,
    household_key,
    tuning_start_time_utc,
    tuning_end_time_utc,
    quarter_hour_id,
    duration,
    date_utc,
    device_key,
    network_id
  ) %>%
  inner_join(networks, by = c("network_id" = "id")) %>%
  inner_join(series, by = c("series_id" = "id"))

median_viewer <- pt %>%
  filter(network_name == "CBS" &&
           date_utc > '2017-12-31' && date_utc < '2019-01-01') %>%
  group_by(series_name, household_key) %>%
  filter(series_name == "Young Sheldon" ||
           series_name == "NCIS" || series_name == "Survivor") %>%
  summarise(total_duration = sum(duration, na.rm = TRUE)) %>%
  summarise(median_total_duration = median(total_duration)) %>%
  select(series_name, median_total_duration)

ncis_viewers <- pt %>%
  filter(
    network_name == 'CBS' &&
      series_name == 'NCIS' &&
      date_utc > '2017-12-31' && date_utc < '2019-01-01'
  ) %>%
  inner_join(median_viewer, by = "series_name") %>%
  group_by(household_key, series_name, median_total_duration) %>%
  summarise(total_duration = sum(duration, na.rm = TRUE)) %>%
  filter(total_duration >= median_total_duration) %>%
  ungroup() %>%
  select(household_key) %>%
  mutate(ncis_viewer = TRUE)


young_sheldon_viewers <- pt %>%
  filter(
    network_name == 'CBS' &&
      series_name == 'Young Sheldon' &&
      date_utc > '2017-12-31' && date_utc < '2019-01-01'
  ) %>%
  inner_join(median_viewer, by = "series_name") %>%
  group_by(household_key, series_name, median_total_duration) %>%
  summarise(total_duration = sum(duration, na.rm = TRUE)) %>%
  filter(total_duration >= median_total_duration) %>%
  ungroup() %>%
  select(household_key) %>%
  mutate(young_sheldon_viewer = TRUE)

survivor_viewers <- pt %>%
  filter(
    network_name == 'CBS' &&
      series_name == 'Survivor'  &&
      date_utc > '2017-12-31' && date_utc < '2019-01-01'
  ) %>%
  inner_join(median_viewer, by = "series_name") %>%
  group_by(household_key, series_name, median_total_duration) %>%
  summarise(total_duration = sum(duration, na.rm = TRUE)) %>%
  filter(total_duration >= median_total_duration) %>%
  ungroup() %>%
  select(household_key) %>%
  mutate(survivor_viewer = TRUE)

all_viewers <-
  full_join(survivor_viewers, ncis_viewers, by = "household_key") %>%
  full_join(young_sheldon_viewers, by = "household_key")

final_segment_table <- pt %>%
  group_by(household_key) %>%
  select(household_key) %>%
  full_join(all_viewers, by = "household_key") %>%
  #full_join(cbs_viewers, by = "household_key") %>%
  mutate_all(funs(ifelse(is.na(.), FALSE, .))) %>%
  distinct()

# Append id605 for later demographic matching
final_segment_table <- final_segment_table %>%
  left_join(tbl(db, "char_exp_synthetic_crosswalk") %>%
              select(household_key, id605))

# Save results to Redshift
# Parameterized drop table if already exists
drop_output_sql <-
  paste0("DROP TABLE IF EXISTS restricted_viewership_zone_eabasolo.",
         output_table_name,
         ";")
DBI::dbExecute(db, sql(drop_output_sql))

# Upload table
redshift_aar_tunings <- compute_redshift(
  final_segment_table,
  tableName = output_table_name,
  schemaName = "restricted_viewership_zone_eabasolo",
  temporary = F,
  sortKey = "household_key",
  distKey = "household_key"
)
