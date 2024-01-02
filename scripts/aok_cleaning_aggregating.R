# REACH SSD - AoK Data Processing and Analysis

# Part 1 - Deal with New Settlement Information

# Load required libraries
library(tidyverse)
library(butteR)
library(koboloadeR)
library(lubridate)
library(sf)
library(kobold)

# Define variables for script use
write_exact_matched_cl <- c("yes", "no")[2]
iso_date <- Sys.Date() %>% str_replace_all("-", "_")
month_input_data <- "2020-05-01"
data_issues <- list()

# Source additional required functions
source("scripts/functions/aok_aggregation_functions.R")
source("scripts/functions/aok_cleaning_functions.R")
source("scripts/functions/aok_set_paths.R")

# Read county level (ADM2) and Payam level (ADM3) boundary files and convert to WGS84
adm2 <- st_read(gdb, "ssd_admbnda_adm2_imwg_nbs_20180401", stringsAsFactors = F) %>% st_transform(crs = 4326)
payams <- st_read(gdb, "ssd_bnd_adm3_wfpge_un", stringsAsFactors = F)
master_settlement <- read.csv(master_settlement_list_from_previous_round_input_path, strip.white = TRUE, stringsAsFactors = FALSE, na.strings = c(" ", "", "NA"))
new_sett <- butteR::read_all_csvs_in_folder(input_csv_folder = new_settlement_folder) %>% bind_rows()

# Prepare master settlement data for fuzzy matching
colnames(master_settlement) <- paste0("mast.", colnames(master_settlement))
master_settlement_sf <- st_as_sf(master_settlement, coords = c("mast.X", "mast.Y"), crs = 4326) %>%
  mutate(mast.name_county_low = mast.NAMECOUNTY %>% tolower_rm_special())

# Read and process raw data
aok_raw_list <- butteR::read_all_csvs_in_folder(input_csv_folder = raw_data_folder)
aok_raw_list %>% purrr::map(nrow)
colnames(aok_raw_list$`REACH_SSD_AoK_V41D+C_May2020 - xml - 2020-06-02-05-13.xlsx_combined_final.csv`)
colnames(aok_raw_list$aok_raw_data_v40_1.csv) <- str_replace_all(colnames(aok_raw_list$aok_raw_data_v40_1.csv), "covid_awareness\\.", "CVD.")
colnames(aok_raw_list$aok_raw_data_v40_2.csv) <- str_replace_all(colnames(aok_raw_list$aok_raw_data_v40_2.csv), "covid_awareness\\.", "CVD.")
aok_raw <- dplyr::bind_rows(aok_raw_list)
bad_groups_that_were_added_v4 <- paste0("^", c("SU.", "P_001.", "CVD.", "csrf."), collapse = "|")
colnames(aok_raw) <- colnames(aok_raw) %>% str_replace_all(bad_groups_that_were_added_v4, "")

# Correct reversed latitude and longitude if needed
new_sett[, c("longitude", "latitude")] <- fix_swapped_lat_lon(df = new_sett, x = "longitude", "latitude")
new_sett <- sp_join_where_possible(df = new_sett, admin = adm2)

new_sett2 <- new_sett %>%
  mutate(
    new.enum_sett_county=paste0(D.info_settlement_other,D.info_county) %>% tolower_rm_special(),
    new.adm2_sett_county=paste0(D.info_settlement_other,adm2) %>% tolower_rm_special()
  )

exact_matches1 <- new_sett2 %>%
  mutate(matched_where = case_when(
    new.enum_sett_county %in% master_settlement_sf$mast.name_county_low ~ "enum", # Check with enums input
    new.adm2_sett_county %in% master_settlement_sf$mast.name_county_low ~ "shapefile_only"
  ),
  county_use = if_else(matched_where == "shapefile_only", adm2, D.info_county),
  name_use = if_else(matched_where == "shapefile_only", new.adm2_sett_county, new.enum_sett_county)
  ) %>%
  filter(!is.na(matched_where)) %>%
  left_join(master_settlement_sf %>% select(mast.name_county_low, mast.NAMEJOIN, mast.COUNTYJOIN), by = c("name_use" = "mast.name_county_low"))

exact_matches_cl <- exact_matches_to_cl(exact_match_data = exact_matches1, user = "zack", uuid_col = "uuid", settlement_col = "mast.NAMEJOIN", county_col = "mast.COUNTYJOIN")

if (write_exact_matched_cl == "yes") {
  write.csv(exact_matches_cl, auto_gen_CL_exact_match_output_path)
}

# Do need to work in the new settlement cols right around here
new_sett_no_coords <- new_sett2 %>%
  filter(!uuid%in% exact_matches1$uuid) %>%
  filter(is.na(longitude)|is.na(latitude))

cleaning_log_new_setts_no_coords <- new_sett_no_coords %>%
  mutate(change_type="remove_survey",
         issue=paste0("field did not provide coordinates/sufficient info for ",D.info_settlement_other, " settlement")
  ) %>%
  select(uuid, change_type,issue)

if (write_new_sett_no_coords_cleaning_log=="yes"){
  write.csv(cleaning_log_new_setts_no_coords, auto_gen_CL_new_sett_remove_output_path)
}

if(nrow(new_sett_no_coords)>0){
  data_issues[["new_settlement_sheet_entries_no_COORDS"]] <- new_sett_no_coords
}

new_sett_sf_unmatched <- new_sett2 %>%
  filter(!uuid%in% exact_matches1$uuid) %>%
  filter(!is.na(longitude)|!is.na(latitude)) %>%
  st_as_sf(coords=c("longitude","latitude"), crs=4326)

# Remove matched settlements from master
master_settlement_sf_not_matched <- master_settlement_sf %>%
  filter(!mast.name_county_low %in% c(new_sett_sf_unmatched$new.enum_sett_county, new_sett_sf_unmatched$new.adm2_sett_county))

# New_with_closest_old$name_new_settlement
new_with_closest_old <- butteR::closest_distance_rtree(new_sett_sf_unmatched ,master_settlement_sf_not_matched)

# Clean up dataset 
new_with_closest_old_vars <- new_with_closest_old %>%
  mutate(new.D.info_settlement_other= D.info_settlement_other %>% str_replace_all(c("-"="_"," "="_"))) %>%
  select(uuid,
         new.A.base=A.base,
         new.county_enum=D.info_county,
         new.county_adm2= adm2,
         new.sett_county_enum=new.enum_sett_county,
         new.sett_county_adm2= new.adm2_sett_county,
         new.D.info_settlement_other,
         mast.settlement=mast.NAMEJOIN,
         mast.name_county_low,
         dist_m)

# Add a few useful columns
settlements_best_guess <- new_with_closest_old_vars %>%
  mutate(gte_50=ifelse(dist_m<500, " < 500 m",">= 500 m"),
         string_proxy=stringdist::stringdist(a =new.sett_county_enum,
                                             b= mast.name_county_low,
                                             method= "dl", useBytes = TRUE)
  ) %>%
  arrange(dist_m,desc(string_proxy))

fuzzy_settlement_matching_done <- "no"
write_auto_gen_cleaning_logs <- "yes"
if(fuzzy_settlement_matching_done=="no"){
  new_settlement_evaluation <- evaluate_unmatched_settlements(user= "zack",new_settlement_table = settlements_best_guess, uuid_col="uuid")
  if(write_auto_gen_cleaning_logs=="yes"){
    write.csv(new_settlement_evaluation$checked_setlements,fuzzy_match_eval_table_output_path)
    write.csv(new_settlement_evaluation$cleaning_log,auto_gen_CL_output_path)

  }

}

if(fuzzy_settlement_matching_done=="yes"){
  new_settlement_evaluation <- list()
  new_settlement_evaluation$checked_setlements <- read_csv(fuzzy_match_eval_table_output_path)
  new_settlement_evaluation$cleaning_log <- read_csv(fuzzy_match_CL_output_path)

}

master_settlement <- read.csv(master_settlement_list_from_previous_round_input_path, stringsAsFactors = FALSE)

new_setts_add_to_master <- new_settlement_evaluation$checked_setlements %>%
  filter(action==2) %>%
  mutate(
    uuid= uuid,
    NAME= new.D.info_settlement_other %>%  gsub("'","",.) %>% gsub(" ","-", .),
    NAMEJOIN= NAME  %>% gsub("-","_",.),
    NAMECOUNTY=paste0(NAMEJOIN,new.county_adm2),
    COUNTYJOIN= new.county_adm2 ,
    DATE= month_input_data %>% ymd(),
    DATA_SOURC="AOK",
    IMG_VERIFD= 0,
    kobo_label= NAME
  ) %>% # Get coordinates from field data back in

  # Select(uuid,NAME,NAMEJOIN,NAMECOUNTY,COUNTYJOIN,DATE,DATA_SOURC,IMG_VERIFD,X,Y )
  left_join(new_sett_sf_unmatched %>%st_drop_geometry_keep_coords() ,
            by=c("uuid"="uuid")) %>%
  select(-uuid,NAME,NAMEJOIN,NAMECOUNTY,COUNTYJOIN,DATE,DATA_SOURC,IMG_VERIFD,X,Y ,kobo_label) %>%
  distinct()

new_setts_add_to_master <- new_setts_add_to_master %>%
  select(NAME,NAMEJOIN,NAMECOUNTY,COUNTYJOIN,DATE,DATA_SOURC,IMG_VERIFD,X,Y , kobo_label)

new_setts_add_to_master$DATE <- new_setts_add_to_master$DATE %>% as.character()
master_new <- bind_rows(new_setts_add_to_master,master_settlement )

master_new %>% nrow()
master_settlement %>% nrow()+ nrow(new_setts_add_to_master)

output_new_settlement_data <- "no"
if(output_new_settlement_data=="yes"){
  write.csv(master_new,new_master_settlement_output_path)
}

itemset_previous  <-  read_csv(itemset_previous_month_input_file)
settlements_other <-  itemset_previous %>% filter(list_name=="settlements" & name=="other")
itemset_previous_non_settlements <-  itemset_previous %>% filter(list_name!="settlements")

new_settlement_itemset <- master_new %>%
  arrange(COUNTYJOIN,kobo_label) %>%
  mutate(list_name="settlements") %>%
  select(list_name, name=NAMEJOIN, label=kobo_label, admin_2=COUNTYJOIN)

new_itemset_full <- bind_rows(new_settlement_itemset,settlements_other, itemset_previous_non_settlements)

output_new_settlement_data <- "yes"
if(output_new_settlement_data=="yes"){
  write.csv(master_new,new_master_settlement_output_path)
}

# Part 2 - Compile cleaning logs

iso_date <-  Sys.Date() %>%  str_replace_all("-","_")
month_input_data <- "2020-05-01"
data_issues <- list()

aok_raw_list  <- butteR::read_all_csvs_in_folder(input_csv_folder = raw_data_folder)
aok_raw  <-  bind_rows(aok_raw_list)

# Keep it as a list to match code that was already written below
# Here is the cleaning log related to new settlements that were fuzzy matched based on distance
new_settlement_evaluation <- list()
new_settlement_evaluation$cleaning_log <- read_csv(auto_gen_CL_output_path)

# Here is the cleaning log to remove surveys where the field never provided coordinates or the appropriate information
cleaning_log_settlement_no_coords <- read_csv(auto_gen_CL_new_sett_remove_output_path)

# Some hacky fixing of names
new_settlement_evaluation$cleaning_log  <-  new_settlement_evaluation$cleaning_log %>%
  select(uuid,spotted:suggested_new_value, -c(indicator:issue)) %>%
  rename_all(~str_replace_all(.,"suggested_","")) %>%
  rename(spotted_by="spotted") %>%
  mutate(source="auto-gen")

new_sett_exact_matches_cl <-  read_csv(auto_gen_CL_exact_match_output_path) %>%
  rename_all(~tolower(.)) %>%
  rename(spotted_by="spotted") %>%
  mutate(source="auto-gen")

# Read in field cleaning log
cleaning_log_format <- "multiple_xlsx"
if(cleaning_log_format=="multiple_csvs"){
  cleaning_logs <- butteR::read_all_csvs_in_folder(input_csv_folder = field_cleaning_logs_folder_path)
  field_cleaning_log <- bind_rows(cleaning_logs)
}
if(cleaning_log_format=="multiple_xlsx"){
  xlsx_files <- list.files(field_cleaning_logs_folder_path, full.names = T)
  xlsx_file_names <- list.files(field_cleaning_logs_folder_path, full.names = F)
  cleaning_logs <- list()
  for(i in seq_along(xlsx_files)){
    file_name_temp <-  xlsx_file_names[i]
    path_temp <- xlsx_files[i]
    print(file_name_temp)
    df <- readxl::read_xlsx(path = path_temp, sheet = "cleaning_log")
    cleaning_logs[[file_name_temp]] <- df
  }
  field_cleaning_log <- bind_rows(cleaning_logs) %>% as_tibble()
  }

# Quick check- in early round there was a problem where AOs were providng logs with different uuid columns
num_uuid_in_cl <- field_cleaning_log %>% select(contains("uid")) %>% colnames() %>% length()
if(num_uuid_in_cl>1){
  print(cleaning_log %>% select(contains("uid")) %>% colnames() )
}else(print("GOOD- only one uuid column found"))

# Put all log titles into lower case -- they should address this in the template
field_cleaning_log <-  field_cleaning_log %>% select(uuid:Issue) %>% rename_all(~tolower(.)) %>% mutate(source="field")

# With the new settlement mapping template they are not supposed to touch settlements in the logs.
remove_from_cl <- field_cleaning_log %>% filter(str_detect(string = indicator, "^D.info_settlement")) %>% filter(change_type=="change_response") %>% pull(uuid)
field_cleaning_log_clean1 <-  field_cleaning_log %>% filter(!uuid %in% remove_from_cl)

# Hacky way to fix the logs- this will have to be ad-hoc if ssd does not standardise processes and data quality issues
field_cleaning_log$indicator[! field_cleaning_log$indicator %in% colnames(aok_raw)]
field_cleaning_log_clean2 <-  field_cleaning_log_clean1 %>% mutate(
  indicator=ifelse(str_detect(indicator,"^covid"), paste0("CVD.",indicator),indicator),

)
field_cleaning_log_clean2$indicator <- field_cleaning_log_clean2$indicator %>% str_replace_all("SU.","") #%>% tolower()

# Bind cleaning logs together
cleaning_log_full <- bind_rows(field_cleaning_log_clean2,
                             new_sett_exact_matches_cl,
                             new_settlement_evaluation$cleaning_log,
                             cleaning_log_settlement_no_coords) %>% as_tibble()


# We dont care about logs that are no action, dont have a change_type, or have no uuid
cleaning_log_actionable <-  cleaning_log_full %>% filter(change_type!="no_action" & !is.na(change_type)&!is.na(uuid))

# Cleaning_log_actionable
# Set to 'yes'
write_full_cleaning_log <- "yes"
if(write_full_cleaning_log=="yes"){
  write.csv(cleaning_log_actionable, full_actionable_cleaning_log_path)
  }

# Part 3 - Implement compiled cleaning logs
# To begin this step you must have cleaned and compiled all cleaning logs and new master settlement list (for checks)

iso_date <-  Sys.Date() %>%  str_replace_all("-","_")
month_input_data <- "2020-05-01"
source("scripts/functions/aok_set_paths.R")
source("scripts/functions/aok_aggregate_settlement4.R")
source("scripts/functions/aok_aggregation_functions.R")

write_ki_level <- c("yes","no")[1]
write_data_issues <- c("yes","no")[1]

convert_logical_to_yn <- function(x){
  x %>% as.character() %>% str_replace_all(c("1"="yes","0"="no"))
  x %>% as.character() %>% str_replace_all(c("TRUE"="yes","FALSE"="no"))
}

# Master_settlement <- read_csv(master_settlement_list_from_previous_round_input_path)
aok_raw_list  <- butteR::read_all_csvs_in_folder(input_csv_folder = raw_data_folder)
aok_raw_list %>% purrr::map(nrow)
cleaning_log_full <- read_csv(full_actionable_cleaning_log_path)
master_settlement <- read_csv(new_master_settlement_output_path)

data_issues <- list()

# Multiple different column names used between tool versions, new groupings added that destroy the analysis -- deal with them here
colnames(aok_raw_list$aok_raw_data_v40_1.csv) <-  str_replace_all(colnames(aok_raw_list$aok_raw_data_v40_1.csv),"covid_awareness\\.","CVD.")
colnames(aok_raw_list$aok_raw_data_v40_2.csv) <-  str_replace_all(colnames(aok_raw_list$aok_raw_data_v40_2.csv),"covid_awareness\\.","CVD.")
aok_raw <-  dplyr::bind_rows(aok_raw_list)
bad_groups_that_were_added_v4 <- paste0("^",c("SU.","P_001.","CVD.","csrf."),collapse="|")
colnames(aok_raw) <-  colnames(aok_raw) %>% str_replace_all(bad_groups_that_were_added_v4,"")

aok_raw <- aok_raw %>%
  mutate(Q.ha_type=str_replace_all(Q.ha_type,c("^in_kind"="through_in_kind")))

# General check
na_response_table <- butteR::get_na_response_rates(aok_raw) %>% arrange(desc(perc_non_response))

# Ensure all UUIDS are unique
aok_raw  <-  distinct(aok_raw,X_uuid, .keep_all= TRUE)
sm_cols_df <- butteR::extract_sm_option_columns(df = aok_raw,name_vector=colnames(aok_raw))

# SSD downloads select multiple as binary integers, convert these to yes, no
aok_raw <- aok_raw %>%
  mutate_at(.vars= sm_cols_df$sm_options,.funs = convert_logical_to_yn)
aok_raw %>% select(sm_cols_df$sm_options)

aok_raw <- aok_raw %>%
  mutate(name_county_low= paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special())

master_settlement <- master_settlement %>%
  mutate(name_county_low= NAMECOUNTY %>%tolower_rm_special())

check_all_settlements_against_master <- aok_raw%>%
  mutate(name_county_low= paste0(D.info_settlement, D.info_county) %>% butteR::tolower_rm_special()) %>%
  filter(D.info_settlement!="other") %>%
  filter(!name_county_low%in%master_settlement$name_county_low) %>% as_tibble() %>%
  select(D.info_county,D.info_settlement, name_county_low)
if(nrow(check_all_settlements_against_master)==0){
  print("Good - all settlements are contained in the master dataset")
}else{
  print("Settlements in data set that are not in master")
}

# Add base to cleaning log
cleaning_log_full <-  cleaning_log_full %>% left_join(aok_raw %>%
                                            select(X_uuid,A.base ), by=c("uuid"="X_uuid")) %>%
  mutate(z_id=paste0("z_",1:nrow(.)))

# Check cleaning log
aok_cleaning_checks1 <- butteR::check_cleaning_log(df = aok_raw, df_uuid = "X_uuid",
                                                 cl = cleaning_log_full,
                                                 cl_change_type_col = "change_type",
                                                 cl_change_col = "indicator",
                                                 cl_uuid = "uuid",
                                                 cl_new_val = "new_value")

# View issues in cleaning log
data_issues[["field_CL_issues"]] <- aok_cleaning_checks1

# Re-run checks with filtered cleaning logs.
aok_cleaning_checks2 <- butteR::check_cleaning_log(df = aok_raw, df_uuid = "X_uuid",
                                                 cl = cleaning_log_full2,
                                                 cl_change_type_col = "change_type",
                                                 cl_change_col = "indicator",
                                                 cl_uuid = "uuid",
                                                 cl_new_val = "new_value")

# Implement log
aok_clean <- butteR::implement_cleaning_log(df = aok_raw, df_uuid = "X_uuid",
                                          cl = cleaning_log_full2,
                                          cl_change_type_col = "change_type",
                                          cl_change_col = "indicator",
                                          cl_uuid = "uuid",
                                          cl_new_val = "new_value")

# Quick check - should be 0 rows
aok_clean %>%
  mutate(name_county_low=paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special()) %>%
  filter(D.info_settlement!="other") %>%
  filter(!name_county_low%in% master_settlement$name_county_low) %>%
  select(X_uuid,A.base,name_county_low, D.info_county,D.info_settlement,D.info_settlement_other)

# Another quick check
if(aok_clean %>%
   filter(D.info_settlement!="other") %>%
   mutate(name_county_low=paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special()) %>%
   filter(!name_county_low%in% master_settlement$name_county_low) %>% nrow()>0){
  print("BAD - cleaning log has added a new settlement that doesnt belong in new settlement list")
}else{
  print("GOOD- no settlement errors introduced in cleaning log")
}

# Write KI level output.
if(write_ki_level=="yes"){
  write.csv(aok_clean,ki_level_clean_data)
}

# Part 4 - Aggregate to settlement level
output_hex_dataset <- c("yes","no")[1]
aok_clean <- read_csv(ki_level_clean_data)
aok_clean %>% filter(D.info_settlement=="other") %>% select(D.info_settlement)

# Convert binary to yes, no
convert_binary_to_yn <- function(x){
  yn <- x %>% as.character() %>% str_replace_all(c("1"="yes","0"="no"))
  return(yn)

}

aok_clean <- aok_clean %>%
  mutate_at(.vars= sm_cols_df$sm_options %>% as.character(),.funs = convert_binary_to_yn)

master_new <- read_csv(new_master_settlement_output_path)
ks <- readxl::read_xlsx(path = kobo_tool_input_path, sheet = "survey")
kc <- readxl::read_xlsx(path = kobo_tool_input_path, sheet = "choices")
payams <- st_read(gdb, "ssd_bnd_adm3_wfpge_un", stringsAsFactors = F )

sm_cols_df$parent_name

write.csv(sm_cols_df$parent_name, file= "ParentName.csv")
write.csv(sm_cols_df$sm_options, file= "SM_options.csv")

bad_groups_that_were_added <- paste0("^",c("SU.","P_001.","CVD.","csrf."),collapse="|")
aok_clean2 <- aok_clean
colnames(aok_clean2)  <-  colnames(aok_clean2) %>% str_replace_all(bad_groups_that_were_added,"")

# Make column name lookup table
colname_table <- tibble(no_groups=colnames(aok_clean2) %>%
                        butteR::remove_kobo_grouper(max_prefix_length = 3) %>%
                        butteR::remove_kobo_grouper(max_prefix_length = 3) ,
                      with_groups=colnames(aok_clean) %>% as.character())

aok_clean_aggregated_settlement <- aggregate_aok_by_settlement(clean_aok_data = aok_clean2,
                                                               current_month = month_input_data,
                                                               kobo_survey_sheet = ks)

# Use Kobold to impute skip logic
ks2 <- ks %>%
  filter(name%in% colnames(aok_clean_aggregated_settlement))

xls_lt <- butteR:::make_xlsform_lookup_table(kobo_survey = ks,kobo_choices = kc,label_column = "label")
sm2 <- xls_lt %>% filter(str_detect(question_type, "^select_multiple| ^select multiple")) %>% pull(xml_format_data_col)

aok_clean_aggregated_settlement  <-  aok_clean_aggregated_settlement %>% rename(protection_issues_covid19.dontknow_covid19 = protection_issues_covid19.don.tknow_covid19)

# Kobold needs select multiple to be how they were in tool (T/F)
aok_clean_aggregated_settlement2 <- aok_clean_aggregated_settlement %>% mutate_at(sm2, function(x)ifelse(x=="yes",TRUE,FALSE))

# Order_we_want <- colnames(aok_clean_aggregated_settlement)
settlement_level_kobold <- kobold::kobold(survey = ks2,choices = kc,data = aok_clean_aggregated_settlement2)
settlement_level_kobold_SL <- kobold:::relevant_updater(settlement_level_kobold)
aok_aggregated_3 <- settlement_level_kobold_SL$data

aok_aggregated_4 <- aok_aggregated_3 %>% mutate_at(sm2, convert_logical_to_yn)
aok_aggregated_5 <- purrr::map_df(aok_aggregated_4, ~ifelse(is.na(.),'SL',.))

# Join Payam data
aok_aggregated_6 <-  aok_aggregated_5 %>%
  mutate(
    name_county_low=paste0(info_settlement,info_county) %>% butteR::tolower_rm_special())
master_new <-  master_new %>%
  mutate(
    name_county_low=paste0(NAMECOUNTY) %>% butteR::tolower_rm_special())

aok_aggregated_7 <- aok_aggregated_6 %>% left_join(master_new %>% select(X,Y,name_county_low), by="name_county_low")
aok_aggregated_6 %>% nrow(); aok_aggregated_7 %>% nrow()
aok_aggregated_8 <-  aok_aggregated_7 %>% distinct() # Check for duplicates

aok_aggregated_9 <- aok_aggregated_8 %>% filter(!is.na(X))

aok_aggregated_with_payams <- st_as_sf(aok_aggregated_9,coords = c("X","Y"),crs=4326) %>%
  st_join(payams %>% select(adm3_name))

colname_table_filt_final <- colname_table %>% filter(no_groups %in% colnames(aok_aggregated_with_payams))

aok_agg_final_w_groups <- aok_aggregated_with_payams %>%
  rename_at(.vars = colname_table_filt_final$no_groups,function(x){x <- colname_table_filt_final$with_groups}) %>%
  rename(D.ki_coverage="ki_coverage",
         D.settlecounty="settlecounty")


if(write_monthly_settlement_data == "yes"){
  write.csv(aok_agg_final_w_groups, settlement_aggregated_monthly_data_output_file,na="SL")}

# Part 5 - Hex Level Aggregations
mast_settlement <- read.csv(new_master_settlement_output_path, stringsAsFactors = FALSE, na.strings=c(""," ")) %>%
  mutate(name_county_low=NAMECOUNTY %>% tolower_rm_special())
aok_monthly <- read_csv(settlement_aggregated_monthly_data_output_file) %>%
  mutate(name_county_low=paste0(D.info_settlement, D.info_county) %>% tolower_rm_special())

aok_monthly %>%
  # Mutate(name_county_low= paste0(D.info_settlement,D.info_county) %>% tolower_rm_special()) %>%
  filter(!name_county_low %in% mast_settlement$name_county_low) %>%
  select(info_state,D.info_county, D.info_settlement)

aok_with_coordinates  <- aok_monthly %>% left_join(mast_settlement %>% select(X,Y,name_county_low), by="name_county_low")
aok_sf <- st_as_sf(aok_with_coordinates,coords = c("X","Y"),crs=4326)

adm2 <-  st_read(gdb,"ssd_admbnda_adm2_imwg_nbs_20180401", stringsAsFactors = F ) %>% st_transform(adm2,crs=4326)
hex_grid  <-  st_read(dsn = "inputs/gis_data",layer ="Grids_info", stringsAsFactors=F ) %>% st_transform(crs=4326) %>% select(-settlement)

data_hex_pt <-  aok_sf %>% st_join(hex_grid)
data_hex_pt <- data_hex_pt %>% st_join(adm2)

# Table with # ki/settlement per grid
grid_evaluation_table <- data_hex_pt %>%
  group_by(State_id) %>%
  summarise(num_setts_grid=n(),
            num_ki_grid=sum(D.ki_coverage))

# Apply threshold to grid
valid_grids <- grid_evaluation_table %>%
  filter(num_setts_grid>1,num_ki_grid>1) %>%
  pull(State_id)

# Create new composites
kc %>% filter(list_name=="AoO_IDP") %>% select(name)
ks %>% filter(name=="idp_location") %>% select(type)

data_hex_pt$J.j2

data_hex_pt_w_composite <- data_hex_pt %>%
  mutate(
# idp_sites= ifelse(J.j2.idp_location=="informal_sites",1,0)
    IDP_present= ifelse(F.idp_now=="yes",1,0),
    IDP_time_arrive=  ifelse(F.f2.idp_time_arrive %in% c("1_month","3_months"),1,0),
    IDP_majority=  ifelse( F.f2.idp_perc %in% c("half","more_half"),1,0),
    food_inadequate= ifelse(G.food_now == "no", 1,0),
    less_one_meal = ifelse(G.meals_number %in% c("one", "less_than_1"),1,0),
    hunger_severe_worse = ifelse(S.shock_hunger %in% c("hunger_severe", "hunger_worst"),1,0),
    wildfood_sick_alltime = ifelse(G.food_wild_emergency=="yes"|G.food_wild_proportion=="more_half",1,0),
    skipping_days = ifelse(G.food_coping_comsumption.skip_days == "yes",1,0),
    flooded_shelter = ifelse(J.shelter_flooding == "yes",1,0),
    fsl_composite = (food_inadequate +less_one_meal+hunger_severe_worse+wildfood_sick_alltime+skipping_days)/5
  )

# Extract new columns added (should be only composite). 
# Cool hack to get just the last added vars (to avg)
vars_to_avg <- names(data_hex_pt_w_composite)[!names(data_hex_pt_w_composite)%in%names(data_hex_pt)]
analyzed_by_grid <- data_hex_pt_w_composite %>%
  group_by(State_id)%>%
  summarise_at(vars(vars_to_avg),mean, na.rm=T)

num_pts_per_grid <- data_hex_pt_w_composite %>% group_by(State_id) %>%
  summarise(number_pts=n()) %>% st_drop_geometry()

analyzed_by_grid <-  analyzed_by_grid %>% left_join(num_pts_per_grid)

# This will result in a data frame
analyzed_by_grid_filt <- analyzed_by_grid %>%
  ungroup() %>%
  mutate_at(.vars = vars_to_avg,
            .funs = function(x){ifelse(analyzed_by_grid$State_id %in% valid_grids,x,NA)}) %>%
  st_drop_geometry()

# This will keep it as a spatial file -- so far they have just wanted the csv rather than spatial
analyzed_by_grid_filt_shp <- analyzed_by_grid %>%
  ungroup() %>%
  mutate_at(.vars = vars_to_avg,
            .funs = function(x){ifelse(analyzed_by_grid$State_id %in% valid_grids,x,NA)})

if(output_hex_dataset=="yes"){
  write.csv(analyzed_by_grid_filt,hex_aggregated_monthly_data_output_file)
}

# Part 6 - Long term aggregation
aok_current_month  <- read_csv(settlement_aggregated_monthly_data_output_file)

input_longterm_settlement_aggregated_file <-  paste0("inputs/",
                                                   monthly_folder,
                                                   "/longterm/",
                                                   "REACH_SSD_settlement_aggregated_longterm_AoK.csv")

output_longterm_settlement_aggregated_file <-  paste0("outputs/",
                                                        monthly_folder,"/aggregated_data/",iso_date,
                                                        "_REACH_SSD_settlement_aggregated_longterm_AoK_",
                                                        month_label,"2020_Data.csv")

output_longterm_settlement_aggregated_file_reduced  <-  paste0("outputs/",
                                                    monthly_folder,"/aggregated_data/",iso_date,
                                                    "_REACH_SSD_settlement_aggregated_longterm_AoK_",
                                                    month_label,"2020_Data_reduced.csv")

aok_longterm_old  <- read_csv(input_longterm_settlement_aggregated_file)

aok_new_longterm  <-  bind_rows(
                              aok_current_month,
                              aok_longterm_old)

write.csv(aok_new_longterm,output_longterm_settlement_aggregated_file)

aok_new_longterm_reduced <- filter(aok_new_longterm, aok_new_longterm$month >= as.Date("2020-02-01"))
write.csv(aok_new_longterm_reduced,output_longterm_settlement_aggregated_file_reduced)
