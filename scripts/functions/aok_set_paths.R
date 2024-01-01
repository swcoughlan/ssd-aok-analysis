# set paths

#INPUTS
# GIS
gdb<- "inputs/gis_data"



monthly_folder<-month_input_data %>% str_replace_all("-","_") %>% str_sub(end=7)
month_label<-lubridate::month(month_input_data,label = T)%>% as.character()

prev_month<-month_input_data %>% ymd() %m-% months(1)
prev_month_folder<-prev_month %>% str_replace_all("-","_") %>% str_sub(end=7)
prev_month_label<- lubridate::month(prev_month, label=T) %>% as.character()


raw_data_folder<-paste0("inputs/" , monthly_folder,"/raw_data")
field_cleaning_logs_folder_path <- paste0("inputs/" , monthly_folder,"/cleaning_logs")


kobo_tool_input_path<-paste0("inputs/", monthly_folder,"/kobo_tool/",monthly_folder,"_AoK_Tool_final.xlsx")

prev_round_LT_input_path<-paste0("outputs/",
                                 prev_month_folder,
                                 "/aggregated_data/",
                                 prev_month_folder,
                                 "_reach_ssd_settlement_aggregated_LONGTERM_AoK_",
                                 prev_month_label,"2020.csv")

#READ IN LATEST MASTER SETTLEMENT LIST
master_settlement_list_from_previous_round_input_path <- paste0("outputs/",
                                              prev_month_folder,
                                              "/settlement_lists/",
                                              prev_month_folder,
                                              "_MASTER_SETTLEMENT_list_UPDATED_with_",
                                              prev_month_label,
                                              "_Data.csv")

new_settlement_folder<-paste0("inputs/",monthly_folder,"/new_settlements")

itemset_previous_month_input_file<-paste0("outputs/",
                         prev_month_folder,
                         "/settlement_lists/",
                         prev_month_folder,
                         "_ITEMSET_UPDATED_with_",
                         prev_month_label,
                         "_Data.csv")


# outputs -----------------------------------------------------------------

data_issues_output_folder<- paste0("outputs/",
                            monthly_folder,
                            "/data_issues/" )
data_issues_1_no_coordinates_output_path<-paste0(data_issues_output_folder, "reach_ssd_AoK_new_setts_with_NO_COORDS_",
       month_label,"Data.csv")

fuzzy_match_eval_table_output_path<-paste0("outputs/" ,
                                         monthly_folder,
                                         "/settlement_lists/fuzzy_eval_tables/",
                                         monthly_folder,
                                         "_new_setts_fuzzy_eval_",
                                         month_label,"_Data.csv")


auto_gen_CL_output_path<-paste0("outputs/" ,
                                   monthly_folder,
                                   "/cleaning_logs/",
                                   monthly_folder,
                                   "_AUTOGEN_cleaning_log_new_settlements_",
                                   month_label,"_Data.csv")
auto_gen_CL_exact_match_output_path<-paste0("outputs/" ,
                                   monthly_folder,
                                   "/cleaning_logs/",
                                   monthly_folder,
                                   "_AUTOGEN_cleaning_log_new_settlements_exact_matches_",
                                   month_label,"_Data.csv")
auto_gen_CL_new_sett_remove_output_path<-paste0("outputs/" ,
                                   monthly_folder,
                                   "/cleaning_logs/",
                                   monthly_folder,
                                   "_AUTOGEN_cleaning_log_new_settlements_no_coords_",
                                   month_label,"_Data.csv")

full_actionable_cleaning_log_path<-paste0("outputs/" ,
                                   monthly_folder,
                                   "/cleaning_logs/",
                                   monthly_folder,
                                   "_cleaning_log_full_actionable_",
                                   month_label,"_Data.csv")


itemset_output_file_name<- paste0("outputs/",
                                  monthly_folder,
                                  "/settlement_lists/",
                                  monthly_folder,
                                  "_ITEMSET_UPDATED_with_",
                                  month_label,
                                  "_Data.csv")
cleaning_log_output_file<- paste0("outputs/",
                                  monthly_folder,
                                  "/cleaning_logs/",
                                  monthly_folder,
                                  "_cleaning_log_full_",
                                  month_label,
                                  "2020.csv")
data_issues_output_prefix<-paste0("outputs/",
                                  monthly_folder,
                                  "/data_issues/",
                                  monthly_folder,"_")

new_master_settlement_output_path<-paste0("outputs/",
       monthly_folder,
       "/settlement_lists/",
       monthly_folder,
       "_MASTER_SETTLEMENT_list_UPDATED_with_",
       month_label,
       "_Data.csv")

"outputs/2020_03/aggregated_data/"
settlement_aggregated_monthly_data_output_file<- paste0("outputs/",
                                  monthly_folder,"/aggregated_data/",iso_date,
                                  "_reach_ssd_settlement_aggregated_AoK_",
                                  month_label,"2020_Data.csv")
ki_level_clean_data<- paste0("outputs/",
                                  monthly_folder,"/clean_ki_data/",iso_date,
                                  "_reach_ssd_ki_level_aok_",
                                  month_label,"2020_Data.csv")
hex_aggregated_monthly_data_output_file<- paste0("outputs/",
                                  monthly_folder,"/aggregated_data/",iso_date,
                                  "_reach_ssd_HEX_aggregated_AoK_",
                                  month_label,"2020_Data.csv")


settlement_aggregated_LT_data_output_file<- paste0("outputs/",
                                                   monthly_folder,"/aggregated_data/",iso_date,
                                                   "_reach_ssd_settlement_aggregated_LONGTERM_AoK_",
                                                   month_label,"2020.csv")
