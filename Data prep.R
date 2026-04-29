# ========================================================================== #
  #  THANAMIR BIRD — SCRIPT 1: Data cleaning

  #  What this script does:
  #    1. Loads the two raw data files
  #    2. Flags all known data quality issues to the console (nothing silently dropped)
  #    3. Cleans and standardises dates, surveyor names, column names
  #    4. Joins the eBird metadata to the transect covariates
  #    5. Writes one clean, joined dataframe as the input for all downstream scripts
  #
## ============================================================================ #


library(tidyverse)
library(readxl)
library(lubridate)


#### LOAD RAW DATA ############################################################ #


ebird_raw <- readr::read_csv("/Users/harryjobanputra/Documents/ZSL - Research assistant/WP5 - Thanamir Bird Data/Apr_26/Thanamir_ebirdmetadata.csv", show_col_types = FALSE)
covariates_raw <- readxl::read_excel("/Users/harryjobanputra/Documents/ZSL - Research assistant/WP5 - Thanamir Bird Data/Apr_26/thanamir_birdcovariates.xlsx")

# quick sanity check on row counts before we do anything
cat("eBird metadata: ", nrow(ebird_raw), "rows,", n_distinct(ebird_raw$`Submission ID`), "unique surveys\n")
cat("Covariates file:", nrow(covariates_raw), "rows,", n_distinct(covariates_raw$`submission ID`), "unique surveys\n\n")


# we expect 669 surveys in both files. the covariates file will show 667 unique IDs
# because of two duplicate submission IDs - those are flagged below


#### COLUMN NAME CLEANING ##################################################### #

# rename everything to snake_case here so we're not fighting column names later. 
# ebird names have spaces and brackets, covariates have mixed formatting.


names(covariates_raw)
names(ebird_raw)



ebird <- ebird_raw %>%
  dplyr::rename(
    submission_id       = `Submission ID`,
    common_name         = `Common Name`,
    scientific_name     = `Scientific Name`,
    taxonomic_order     = `Taxonomic Order`,
    count               = Count,
    state_province      = `State/Province`,
    county              = County,
    location_id         = `Location ID`,
    location            = Location,
    latitude            = Latitude,
    longitude           = Longitude,
    date                = Date,
    time                = Time,
    protocol            = Protocol,
    duration_min        = `Duration (Min)`,
    all_obs_reported    = `All Obs Reported`,
    distance_km         = `Distance Traveled (km)`,
    area_ha             = `Area Covered (ha)`,
    n_observers         = `Number of Observers`,
    breeding_code       = `Breeding Code`,
    obs_details         = `Observation Details`,
    checklist_comments  = `Checklist Comments`,
    ml_catalog          = `ML Catalog Numbers`
  )

covariates <- covariates_raw %>%
  dplyr::rename(
    transect_no         = `transect no`,
    date_cov            = date,
    submission_id       = `submission ID`,
    link                = link,
    surveyor_1          = `surveyor...5`,
    surveyor_2          = `surveyor...6`,
    surveyor_3          = `surveyor...7`,
    surveyor_4          = `surveyor...8`,
    centroid_lat        = centroid.lat,
    centroid_long       = centroid.long,
    management          = management.regime,
    dist_village_m      = `distance from village (M)`,
    transect_length_m   = length,
    elevation_m         = average.elevation,
    habitat             = `habitat type`
  )


# fix date structure
covariates <- covariates %>%
  dplyr::mutate(
    date_cov = as.Date(as.numeric(date_cov), origin = "1899-12-30")
  )

# the covariates file has one row per survey (not one row per transect), so
# transect-level stuff like elevation and habitat just repeats for every survey on
# that transect. 


#### QA 1): SUBMISSION ID MISMATCHES ##################################### #

# check if any IDs appear in one file but not the other. 

ids_ebird <- unique(ebird$submission_id)
ids_cov   <- unique(covariates$submission_id)

in_cov_not_ebird <- setdiff(ids_cov, ids_ebird)
in_ebird_not_cov <- setdiff(ids_ebird, ids_cov)


print(in_cov_not_ebird)
print(in_ebird_not_cov)

# Few IDs to flag here... 



#### QA 2): DUPLICATE SUBMISSION IDs IN COVARIATES ####################### #

# two submission IDs show up twice in the covariates file, each time against a
# different date. probably the same checklist entered for two survey slots by mistake.
# keeping the first occurrence for now but check which date is right.

dup_ids <- covariates %>%
  dplyr::count(submission_id) %>%
  dplyr::filter(n > 1) %>%
  dplyr::pull(submission_id)

cat("========================================================\n")
cat("QA FLAG 2: DUPLICATE SUBMISSION IDs IN COVARIATES FILE\n")
cat("========================================================\n")
cat("Submission IDs appearing more than once (", length(dup_ids), "):\n")
covariates %>%
  dplyr::filter(submission_id %in% dup_ids) %>%
  dplyr::select(transect_no, date_cov, submission_id) %>%
  dplyr::arrange(submission_id) %>%
  print()
cat("\nFor now, keeping the first occurrence of each duplicate.\n")
cat("ACTION NEEDED: Ramya to confirm which date is correct for each.\n\n")

# keep first occurrence only for now
covariates <- covariates %>%
  dplyr::distinct(submission_id, .keep_all = TRUE)




#### QA 3): DATE IN TRANSECT 8 ################################### #

# the earliest T8 entry in teh covariates file has a date that didn't parse
# properly. probably an entry error. 

cat("========================================================\n")
cat("QA FLAG 3: POSSIBLE DATE ERROR IN TRANSECT 8\n")
cat("========================================================\n")
bad_dates <- covariates %>%
  dplyr::filter(is.na(date_cov))

if (nrow(bad_dates) > 0) {
  cat("Rows with missing/unparseable dates:\n")
  print(bad_dates %>% dplyr::select(transect_no, date_cov, submission_id))
} else {
  cat("No completely missing dates found (date_cov parsed ok for all rows).\n")
}


cat("T8 entry with missing date (submission ID for Ramya to check):\n")
print(bad_dates %>% dplyr::select(transect_no, date_cov, submission_id))
cat("\nRaw value in Excel was '24-10-12' -- probably 2024-10-12 but needs confirming.\n")
cat("ACTION NEEDED: Ramya to correct this date in the covariates file.\n\n")





#### QA 4):  INCOMPLETE CHECKLISTS ######################################## #

# 2 surveys have all_obs_reported = 0, which I think means the surveyor didn't tick
# 'all species reported'. that matters because we can't infer absences from
# an incomplete checklist. will keep them in the dataset but flagging with a column.
# script 2 will filter to complete checklists only when computing frequencies.

incomplete <- ebird %>%
  dplyr::distinct(submission_id, all_obs_reported) %>%
  dplyr::filter(all_obs_reported == 0)

cat("========================================================\n")
cat("QA FLAG 4: INCOMPLETE CHECKLISTS (all_obs_reported = 0)\n")
cat("========================================================\n")
cat(nrow(incomplete), "surveys were not marked as complete checklists:\n")
print(incomplete$submission_id)
cat("\nThese will be retained in the full dataset but flagged with a column.\n")




#### QA 4): EFFORT OUTLIERS ############################################## #

# 15 surveys have duration > 200 min, a few are over 1000. typical survey is
# around 75-85 min so these look like entry errors (maybe hours entered instead
# of minutes or similar). one survey also has a 31km distance which is way off.
# flagging the IDs so they can be checked.

survey_effort <- ebird %>%
  dplyr::distinct(submission_id, duration_min, distance_km)

long_surveys <- survey_effort %>%
  dplyr::filter(duration_min > 200) %>%
  dplyr::arrange(desc(duration_min))

long_distance <- survey_effort %>%
  dplyr::filter(!is.na(distance_km), distance_km > 5) %>%
  dplyr::arrange(desc(distance_km))

cat("========================================================\n")
cat("QA FLAG 5: EFFORT OUTLIERS\n")
cat("========================================================\n")
cat("Surveys with duration > 200 minutes (", nrow(long_surveys), "):\n")
print(long_surveys)
cat("\nSurveys with distance > 5 km (", nrow(long_distance), "):\n")
print(long_distance)





#### QA 5): DATE PARSING ############################################################ #

# parse the date column in ebird from DD/MM/YYYY character to a proper Date
# then add year, month and a year_month column for grouping later on

ebird <- ebird %>%
  dplyr::mutate(
    date       = lubridate::dmy(date),
    year       = lubridate::year(date),
    month      = lubridate::month(date),
    year_month = lubridate::floor_date(date, unit = "month")
  )


# adding rough season labels for the region. boundaries based on the general climate pattern:
# winter dry (Nov-Feb), pre-monsoon (Mar-May), monsoon (Jun-Sep), post-monsoon (Oct).
# flag if you want different bounaries for season.
ebird <- ebird %>%
  dplyr::mutate(
    season = dplyr::case_when(
      month %in% c(11, 12, 1, 2) ~ "winter",
      month %in% c(3, 4, 5)      ~ "pre_monsoon",
      month %in% c(6, 7, 8, 9)   ~ "monsoon",
      month == 10                 ~ "post_monsoon"
    )
  )

cat("========================================================\n")
cat("DATE PARSING\n")
cat("========================================================\n")
cat("Date range in eBird data:\n")
cat("  From:", format(min(ebird$date, na.rm = TRUE)), "\n")
cat("  To:  ", format(max(ebird$date, na.rm = TRUE)), "\n\n")


#### QA 6): SURVEYOR CLEANING ####################################################### #

# strip whitespace and standardise case across the four surveyor columns.
# also build a team string per survey (sorted alphabetically so JONA+RISTHONG
# and RISTHONG+JONA are treated as the same team). we'll use this later to
# check if team composition is associated with how many species get recorded.


covariates <- covariates %>%
  dplyr::mutate(
    dplyr::across(
      dplyr::starts_with("surveyor"),
      ~ stringr::str_trim(stringr::str_to_upper(.))
    )
  )

covariates <- covariates %>%
  dplyr::rowwise() %>%
  dplyr::mutate(
    surveyor_team = dplyr::c_across(c(surveyor_1, surveyor_2, surveyor_3, surveyor_4)) %>%      .[!is.na(.) & . != ""] %>%
      sort() %>%
      paste(collapse = " + ")
  ) %>%
  dplyr::ungroup()

surveyor_pool <- covariates %>%
  tidyr::pivot_longer(
    cols      = c(surveyor_1, surveyor_2, surveyor_3, surveyor_4),
    names_to  = "surveyor_slot",
    values_to = "name"
  ) %>%
  dplyr::filter(!is.na(name), name != "") %>%
  dplyr::count(name, sort = TRUE)

cat("========================================================\n")
cat("SURVEYOR POOL\n")
cat("========================================================\n")
print(surveyor_pool)
cat("\n6 core surveyors account for nearly all surveys.\n")
cat("also a visiting team member. keeping them all in for now.\n\n")




#### QA 7): JOIN EBIRD TO COVARIATES ################################################ #

# left join so every observation row gets its transect covariates attached.
# the surveys that couldn't be matched (QA FLAG 1) will come through with
# NA covariates -- we add a flag column so they're easy to spot and filter.

cov_to_join <- covariates %>%
  dplyr::select(
    submission_id, transect_no, date_cov, management, habitat,
    elevation_m, dist_village_m, transect_length_m,
    centroid_lat, centroid_long, surveyor_team,
    surveyor_1, surveyor_2, surveyor_3, surveyor_4
  )

thanamir <- ebird %>%
  dplyr::left_join(cov_to_join, by = "submission_id") %>%
  dplyr::mutate(
    has_covariates     = !is.na(transect_no),
    complete_checklist = all_obs_reported == 1,
    effort_flag        = submission_id %in% c(long_surveys$submission_id,
                                              long_distance$submission_id)
  )

cat("========================================================\n")
cat("JOIN SUMMARY\n")
cat("========================================================\n")
cat("Total observation rows after join:", nrow(thanamir), "\n")
cat("Rows WITH covariate data:         ", sum(thanamir$has_covariates), "\n")
cat("Rows WITHOUT covariate data:      ", sum(!thanamir$has_covariates), "\n")
cat("  (these are from the", length(in_ebird_not_cov), "surveys missing from the covariates file)\n\n")

# survey-level summary
surveys_joined <- thanamir %>%
  dplyr::distinct(submission_id, .keep_all = TRUE) %>%
  dplyr::filter(has_covariates)

cat("Matched surveys by transect:\n")
surveys_joined %>%
  dplyr::count(transect_no) %>%
  dplyr::arrange(transect_no) %>%
  print()
cat("\n")



# All looks right. 8,451 observation rows, 8,411 with covariates, 40 without from those 3 unmatched surveys. 
# Survey counts per transect match what we saw in the initial exploration. 



#### FINAL DATASET STRUCTURE ################################################# #

cat("========================================================\n")
cat("FINAL DATASET COLUMNS\n")
cat("========================================================\n")
cat("Rows:", nrow(thanamir), " | Surveys:", n_distinct(thanamir$submission_id), "\n")
cat("Columns:", ncol(thanamir), "\n\n")
glimpse(thanamir)


#### WRITE OUTPUT ############################################################ #

# .rds keeps all the R data types intact (dates, logicals etc) which a CSV would
# mangle. scripts 2 and 3 just readRDS() this rather than redoing the cleaning.

if (!dir.exists("data")) dir.create("data")
saveRDS(thanamir, "data/thanamir_clean.rds")

cat("\n========================================================\n")
cat("DONE: data/thanamir_clean.rds written successfully.\n")
cat("Total rows:", nrow(thanamir), "\n")
cat("Surveys with full covariates:", sum(!is.na(thanamir$transect_no) & !duplicated(thanamir$submission_id)), "\n")
cat("========================================================\n\n")


#### OPEN QUESTIONS ###################################### #

# collecting everything we need confirmed before diving into analysis.

# 1. Can we confirm which transects are the three added in 2023? (I think T7, T8, T9)
# 2. What is the exact start date of the logging near Transect 8?
# 3. The date for the earliest T8 entry - what should it be? (see QA 3)
# 4. Duplicate submission IDs S123330083 (T2) and S123377174 (T4) - which date is right?
# 5. Can you check the 15 high-duration surveys and the 31km distance entry? (QA 4)
# 6. The 3 eBird IDs in the metadata but not the covariates - are these valid surveys?
