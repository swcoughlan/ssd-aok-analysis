aggregate_aok_by_settlement <-  function(clean_aok_data, current_month, kobo_survey_sheet){

clean_aok_data2 <- clean_aok_data
colnames(clean_aok_data2) <- colnames(clean_aok_data) %>%
  butteR::remove_kobo_grouper(max_prefix_length = 3) %>%
  butteR::remove_kobo_grouper(max_prefix_length = 3)

colname_table <- data.frame(with_group=colnames(clean_aok_data),
                          without_group=colnames(clean_aok_data2)) %>%
  mutate_all(.funs = as.character)

# Kobo_survey_sheet  <-  ks
# Clean_aok_data  <-  aok_clean4
select_ones_questions <- ks %>%
  filter(str_detect(type,"select_one")) %>%
  pull(name)

# Extract select multiple and replace 0s and 1s with no and yes
sm_cols_df <- butteR::extract_sm_option_columns(df = clean_aok_data2,name_vector=colnames(clean_aok_data2)) %>%
  mutate_all(.funs=as.character)


# Removes the concatenated select multiple from the table
sm_cols_df2 <- sm_cols_df %>%
  filter(parent_name!=sm_options) %>%
  ungroup() %>% as_tibble()
# sm_cols_df2%>% View()
sm_concat_cols <- sm_cols_df %>%
  filter(parent_name==sm_options)

class(sm_cols_df2)

sm_concat_cols2 <- sm_concat_cols %>%
  left_join(colname_table, by=c("sm_options"= "without_group")) %>%
  filter(!str_detect(sm_options,"note"))

sm_cols_df3 <- sm_cols_df2 %>%
  left_join(colname_table, by=c("sm_options"= "without_group"))

essential_grouping_cols <- c("info_state", "info_county", "info_settlement")

clean_aok_data2 <- clean_aok_data2 %>% mutate_at(.vars = essential_grouping_cols, .funs = trimws)

clean_up_sm_questions <-  function(x) {x %>%as.character() %>%  trimws() %>% tolower() }
clean_aok_data2 <- clean_aok_data2 %>% mutate_at(.vars=sm_cols_df3$sm_options, .funs = clean_up_sm_questions)

## Remove columns we don't need. 
clean_aok_data2 <-  select(clean_aok_data2, everything(), -contains("note"), - contains("_other"))

# These are select_one exceptions that should be aggregated with AoK yes
special_case_select_one_to_aggregate_with_aok_yes <- c("hc_leave", "idp_returnees", "hc_now", "hcdisp_now", "idp_now",
                                                "refugee_return_now", "food_distr", "food_wild_emergency", "food_wild_now",
                                                "community_healthworkers", "facility_stocked", "nfi_distribution",
                                                "shelter_damage", "shelter_flooding", "shelter_open_yn", "latrine_now",
                                                "water_quality", "water_safety", "water_source_animals", "cattle_disease",
                                                "crop_distruptions", "livestock_disease", "land_disputes", "prot_incidence",
                                                "prot_looting", "prot_unaccompanied", "mines", "supp_feeding",
                                                "assistance_now", "cash_voucher", "complaint_awareness", "ha_protection_issues_men",
                                                "ha_protection_issues_women", "ha_satisfied", "most_relevant",
                                                "market_now", "market_safety")

select_multiple_aok_yes <-  sm_cols_df3$sm_options
aok_yes_cols <- c(special_case_select_one_to_aggregate_with_aok_yes,select_multiple_aok_yes)

aok_yes_cols <- aok_yes_cols[aok_yes_cols %in% colnames(clean_aok_data2)]
aok_yes_cols <- aok_yes_cols[!aok_yes_cols %in% essential_grouping_cols]
aok_yes_cols_dropped <- aok_yes_cols[aok_yes_cols %in% colnames(clean_aok_data2)==FALSE]

# Apply the "aok_no" function to these set of columns
aok_no_cols <- c( "food_now",
          "land_available", "ag_seeds", "ag_tools", "idp_supported",
          "idp_leadership", "local_authorities")

aok_highest_cols <- c("mine_acc_numb")

aok_recent_cols <- c("idp_time_arrive",
              "refugee_return_time_arrive")
aok_conflict_cols <- c("food_no_reason1")

aok_yes_cols <- aok_yes_cols[!aok_yes_cols%in%c(aok_no_cols,aok_highest_cols,aok_recent_cols,aok_conflict_cols,sm_concat_cols2$sm_options)]

select_ones_questions <- c(select_ones_questions,sm_concat_cols2$sm_options)

aok_mode_cols <- select_ones_questions[!select_ones_questions %in%
                                            c(special_case_select_one_to_aggregate_with_aok_yes,
                                              c("enumerator_id"),
                                              aok_no_cols,aok_highest_cols, aok_recent_cols,aok_conflict_cols)]

settlement_yes <- clean_aok_data2 %>%
select(essential_grouping_cols,aok_yes_cols
) %>%
group_by(!!!syms(essential_grouping_cols)) %>%
summarise_all(.funs=aok_yes)

settlement_no  <-   clean_aok_data2 %>%
select(essential_grouping_cols,aok_no_cols) %>%
group_by(!!!syms(essential_grouping_cols)) %>%
summarise_all(.funs=aok_no)

settlement_mode  <-  clean_aok_data2 %>%ungroup() %>%
select(essential_grouping_cols,aok_mode_cols) %>%
group_by(!!!syms(essential_grouping_cols)) %>%
summarise_all(.funs=AoK)

settlement_recent  <-  clean_aok_data2 %>%
select(essential_grouping_cols,aok_recent_cols) %>%
group_by(!!!syms(essential_grouping_cols)) %>%
summarise_all(.funs=aok_recent)

settlement_highest  <- clean_aok_data2 %>%
select(essential_grouping_cols,aok_highest_cols) %>%
group_by(!!!syms(essential_grouping_cols)) %>%
summarise_all(.funs=aok_highest)

## Apply the "aok_conflict" function to these set of columns
settlement_conflict  <-  clean_aok_data2 %>%
select(essential_grouping_cols, aok_conflict_cols) %>%
group_by(!!!syms(essential_grouping_cols)) %>%
summarise_all(.funs=aok_conflict)


# Table join. Merge all these columns/fields into one database
# analysis_df_list <- list(settlement_yes, settlement_no,settlement_mode)
analysis_df_list <- list(settlement_yes, settlement_no,settlement_mode,settlement_recent, settlement_highest,settlement_conflict)
settlement_joined <- purrr::reduce(analysis_df_list, right_join, by=c("info_state","info_county","info_settlement"))
settlement_yes$U.market_now_barriers_at
settlement <- settlement_joined

# Rearrange the columns in our database in the same order appear on the tool
settlement  <-  settlement %>% select(order(match(names(settlement), names(clean_aok_data2))))

# Check missing column names
missing_columns <- colnames(clean_aok_data2)[colnames(clean_aok_data2)%in% colnames(settlement)==FALSE]

columns_not_needed <-  c("month", "today", "deviceid", "type_visit", "ki_gender", "D_startTime",
                    "info_settlement_final", "info_payam", "type_contact", "remotely_how",
                    "remotely_how.s_phone", "remotely_how.mob_app", "remotely_how.m_phone",
                    "remotely_how.other", "remotely_how.radio", "remotely_how.internet",
                    "remotely_how.dontknow", "ki_recent", "F_startTme", "other",
                    "other_idp", "other_returnees_push", "other_returnees_pull",
                    "other_refugee_returnee_push", "other_refugee_returnees_pull",
                    "G_startTme_1", "food_now_type", "food_enough_fewmeals", "food_coping_comsumption",
                    "P_startTme", "breastfeeding_change", "children_food_prep_change",
                    "L_startTme", "food_coping_livelihoods", "current_activities",
                    "cutlivation_yes_land_no", "crops_yes_tools_no", "U_startTme",
                    "market_now_barriers", "market_now_barriers.dontknow", "I_startTime",
                    "T_startTime", "key_meaasage", "key_signs", "get_ebola", "everyone_risk",
                    "prevent_ebola", "hand_wash", "wild_animals", "bush_meat", "no_fruits",
                    "no_sharp_object", "protective_clothes", "seek_medical", "H_startTme",
                    "N_startTime", "safe_yes_incident_yes", "S_startTime", "J_startTime",
                    "shelter_materials", "K_startTime", "M_startTme", "R_startTme",
                    "O_startTme", "mine_areas", "Z_startTme", "end_survey_protection",
                    "LE_1_remote", "LE_2_unpopulated", "LE_3_nohostcommunity", "LE_4_food_enough_fewmeals",
                    "LE_5_food_insufficient_manymeals", "LE_7_food_enough_malnutritiondeath",
                    "LE_8_food_enough_hungersevere", "LE_9_cutlivation_yes_land_no",
                    "LE_10_crops_yes_seedstools_no", "LE_11_forage_yes_wildfoods_no",
                    "LE_12_morehalf_wildfood_morethan_3meals", "LE_13_hunting_source_yes_activity_no",
                    "LE_14_fishing_source_yes_activity_no", "LE_15_market_source_yes_access_no",
                    "LE_16_assist_source_yes_no_distribution", "LE_17_safe_but_serious_concerns_women",
                    "LE_18_safe_but_serious_concerns_men", "LE_19_safe_but_serious_concerns_girls",
                    "LE_20_safe_but_serious_concerns_boys", "LE_26_safe_yes_incident_yes",
                    "LE_21_mainshelter_HC_permanent", "LE_22_mainshelter_IDP_permanent",
                    "LE_23_IDP_bush_yes_open_no", "LE_24_water_borehole_no_borehole",
                    "X_id", "X_uuid", "start", "end", "enumerator_id", "survey_start_time",
                    "ki_age", "gps", "_gps_latitude", "_gps_longitude", "_gps_altitude",
                    "X__version__", "X_submission_time", "X_index", "X_validation_status")

check_these <- missing_columns[missing_columns %in% columns_not_needed ==FALSE]

ki_coverage  <-  clean_aok_data2 %>%
group_by(!!!syms(essential_grouping_cols)) %>%
summarise(ki_coverage=n())

settlement  <-  settlement %>%
ungroup() %>%
left_join(ki_coverage) %>%
mutate(settlecounty=paste0(info_settlement,info_county)) %>%
select(info_state, info_county, info_settlement, settlecounty, ki_coverage, everything())

settlement  <-  tibble::add_column(as.data.frame(settlement), month = rep(current_month, len = nrow(settlement)), .before = 1)
return(settlement)
}