# LOAD LIBRARIES -------------------------------------------------------
library(tidyverse)
library(sf)
library(butteR)
library(lubridate)
library(srvyr)
source("scripts/functions/aok_aggregation_functions.R")


# SET MONTH ------------------------------------------------------------
month_of_assessment<-"2020-09-01"

# LOAD IN DATA ---------------------------------------------------------
aok_sep <- read.csv("outputs/2020_09/2020_10_14_reach_ssd_settlement_aggregated_AoK_Sep2020_Data.csv", stringsAsFactors = FALSE, na.strings=c(""," "))
indicators <- readLines("scripts/AAP/indicators2.csv")
indicators2 <- readLines("scripts/AAP/indicators3.csv")
indicators3 <- readLines("scripts/AAP/indicators_sep2020.csv")

aok_sep %>% nrow()
aok_sep2<-aok_sep %>%
  mutate(month=month %>% ymd()) %>%
  group_by(D.info_county, D.info_settlement)

st_layers("inputs/gis_data")
adm2<- st_read(dsn="inputs/gis_data","ssd_admbnda_adm2_imwg_nbs_20180401", stringsAsFactors=F ) %>% st_transform(crs=4326)
aok_sep2<- aok_sep2 %>%
  mutate(
    name_county_low=paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special())

mast_settlement<-read.csv("outputs/2020_09/2020_09_MASTER_SETTLEMENT_list_UPDATED_with_Sep_Data.csv", stringsAsFactors = FALSE, na.strings=c(""," "))

#good quick check to make sure we have all settlements in master list
aok_sep2 %>%
  filter(!name_county_low %in% mast_settlement$name_county_low) %>%
  select(D.info_state,D.info_county, D.info_settlement)

aok_sep2_missing <- aok_sep2 %>%
  filter(!name_county_low %in% mast_settlement$name_county_low) %>%
  select(D.info_state,D.info_county, D.info_settlement, name_county_low)
aok_sep2_missing %>% write.csv('temp/aok_sep2_missing_settlements.csv')

aok_with_coordinates<-aok_sep2 %>% left_join(mast_settlement %>% select(X,Y,name_county_low), by="name_county_low")
aok_with_coordinates<-aok_with_coordinates %>% ungroup()
aok_with_coordinates %>% write.csv('aok_with_coordinates.csv')
aok_sf<-st_as_sf(aok_with_coordinates, coords = c("X","Y"), crs=4326)

valid_counties<-mast_settlement %>%
  left_join(aok_sep2 %>% mutate(assessed=1) %>% select(name_county_low,assessed),by="name_county_low") %>%
  group_by(COUNTYJOIN) %>%
  summarise(total_assesed=sum(assessed,na.rm=T),
            total_settlements=n()) %>%
  mutate(percent_assessed= total_assesed/total_settlements) %>%
  filter(total_assesed>=0.05) %>%
  pull(COUNTYJOIN)

#HEX GRID HAS ALREADY BEEN CREATED
hex_grid <- st_read(dsn = "inputs/gis_data",layer ="Grids_info_wgs84", stringsAsFactors=F ) %>% st_transform(crs=4326) %>% select(-settlement)

hex_data_poly<-hex_grid %>% st_join(aok_sf)

data_hex_pt<-aok_sf %>% st_join(hex_grid)
data_hex_pt<-data_hex_pt %>% st_join(adm2)

grid_evaluation_table<-data_hex_pt %>%
  group_by(State_id) %>%
  summarise(num_setts_grid=n(),
            num_ki_grid=sum(D.ki_coverage))
valid_grids<-grid_evaluation_table %>%
  filter(num_setts_grid>1,num_ki_grid>1) %>%
  pull(State_id)

to_title_remove_unknowns<- function(x){
  ifelse(x %in% c("NC", "SL","dontknow", "other","Other","none","None"),NA,x) %>% str_to_title(.)
}

unique(data_hex_pt$Q.people_complains)

data_hex_pt_cleaned<-data_hex_pt %>%
  mutate(
    Q.people_complains_new = case_when(Q.people_complains %in% c("less_than_half") ~"yes",
                                       Q.people_complains %in% c("more_than_half","about_half","none_complain","dont_know") ~"no")
  )

data_hex_pt_cleaned<- data_hex_pt_cleaned %>%
  mutate_at(.vars= c(indicators3), .funs = to_title_remove_unknowns)


data_hex_pt_cleaned %>% write.csv('temp/data_hex_pt_cleaned.csv')

cols_to_analyze <- c(indicators3)
aap_svy<-as_survey(data_hex_pt_cleaned)

grid_quant_analysis<- butteR::mean_prop_working(design =aap_svy,
                                                list_of_variables = cols_to_analyze,
                                                aggregation_level =  "State_id",
                                                na_replace = F)
county_quant_analysis<- butteR::mean_prop_working(design =aap_svy,
                                                  list_of_variables = cols_to_analyze,
                                                  aggregation_level =  "admin2RefN",
                                                  na_replace = F)

grid_quant_analysis %>% write.csv('temp/grid_quant_analysis.csv')
county_quant_analysis %>% write.csv('temp/county_quant_analysis.csv')

county_analysis_quant_to_map<-county_quant_analysis %>%
  select(admin2RefN,Q.people_complains_new.Yes)

county_analysis_quant_to_map<-county_analysis_quant_to_map %>%
  mutate_at(.vars = c("Q.people_complains_new.Yes"),
            .funs = function(x) {ifelse(county_analysis_quant_to_map$admin2RefN %in% valid_counties, x,NA )})

grid_analysis_quant_to_map<-grid_quant_analysis %>%
  select(State_id,Q.people_complains_new.Yes)

grid_analysis_quant_to_map<-grid_analysis_quant_to_map %>%
  mutate_at(.vars = c("Q.people_complains_new.Yes"),
            .funs = function(x) {ifelse(grid_analysis_quant_to_map$State_id %in% valid_grids, x,NA )})

adm2_w_analysis<-adm2 %>%
  #left_join(county_analysis_categ %>% st_drop_geometry()) %>%
  left_join(county_analysis_quant_to_map)

hex_grid_w_analysis<-hex_grid %>% left_join(grid_analysis_quant_to_map)

hex_grid_w_analysis %>%  st_write("qgis/AAP_sep2020_2.gpkg",layer="AAP_2020_06_hex_sep_AoK_2_NEW", layer_options = 'OVERWRITE=YES', update = TRUE)
adm2_w_analysis %>%  st_write("qgis/AAP_sep2020_2.gpkg",layer="AAP_2020_06_county_sep_AoK_2_NEW", layer_options = 'OVERWRITE=YES', update = TRUE)
aok_sf %>% select(D.info_state, D.info_county,D.info_settlement) %>%
  st_write("qgis/AAP_sep2020_2.gpkg",layer="AAP_2020_06_settlement_sep_2_NEW", layer_options = 'OVERWRITE=YES', update = TRUE)

