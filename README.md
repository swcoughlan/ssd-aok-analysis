- [SOUTH SUDAN AOK DATA PROCESSING
  TUTORIAL](#south-sudan-aok-data-processing-tutorial)
- [SETUP](#setup)
  - [packages](#packages)
- [INPUTS](#inputs)
  - [New Settlement Data](#new-settlement-data)
  - [Cleaning Logs](#cleaning-logs)
- [DEALING WITH NEW SETTLEMENT
  DATA](#dealing-with-new-settlement-data)
  - [Reformatting Data/ Minor
    Manipulations](#reformatting-data-minor-manipulations)
  - [Checking/Cross Referencing Settlement Data, Cleaning Logs and
    Assessment
    Data](#checkingcross-referencing-settlement-data-cleaning-logs-and-assessment-data)
  - [Exact Matches](#exact-matches)
  - [Finding The Closest Point](#finding-the-closest-point)
  - [Evaluating the closest point](#evaluating-the-closest-point)
  - [NEW SETTLEMENT OUTPUTS](#new-settlement-outputs)
- [AGGREATION/ANALYSIS](#aggreationanalysis)
  - [County Level Aggregation](#county-level-aggregation)
  - [Hexagonal Aggregation](#hexagonal-aggregation)
- [NEXT STEPS](#next-steps)

# SOUTH SUDAN AOK DATA PROCESSING TUTORIAL

This project was developed in January 2020 to automate South Sudan AOK
data collation and analysis. This document serves as a tutorial for the
the next Analyst. As this Git-hub will no longer be maintained after
March 6 2020 it is recommended that this github be forked by the
responsible GIS/Data Unit Manager and the fork be maintained and
updated.

The actual process is mean to be implemented entirely through the script
currently labeled **AOK_cleaning_aggregating.R** The markdown (rmd)
file was simple made to create the tutorial and enhance documentation.

# SETUP

## packages

You will need the packages below. This analysis is dependent on the
butteR package which can be downloaded from github (link below).

```r
# install.packages("devtools")
devtools::install_github("zackarno/butteR")
```

```r
library(tidyverse)
library(butteR)
library(koboloadeR)
library(lubridate)
library(sf)
source("scripts/functions/aok_aggregation_functions.R")
source("scripts/functions/aok_cleaning_functions.R")
source("scripts/functions/aok_aggregate_by_county_wrapped.R")
```

# INPUTS

## New Settlement Data

```r
month_of_assessment<-"2020-02-01"

admin_gdb<- "../../gis_data/gis_base/boundaries/county_shapefile"
master_settlement<-read.csv("inputs/48_December_2019/SSD_Settlements_V37.csv", stringsAsFactors = FALSE)
colnames(master_settlement)<-paste0("mast.",colnames(master_settlement))
master_settlement_sf<- st_as_sf(master_settlement,coords=c("mast.X","mast.Y"), crs=4326)


new_settlements<-butteR::read_all_csvs_in_folder(input_csv_folder = "inputs/2020_02/new_settlements")
new_sett<-bind_rows(new_settlements)
new_sett<-new_sett %>% filter(action=="Map")

adm2<- st_read(admin_gdb,"ssd_admbnda_adm2_imwg_nbs_20180401" )
```

    ## Reading layer `ssd_admbnda_adm2_imwg_nbs_20180401' from data source `C:\02_REACH_SSD\gis_data\gis_base\boundaries\county_shapefile' using driver `ESRI Shapefile'
    ## Simple feature collection with 78 features and 17 fields
    ## geometry type:  POLYGON
    ## dimension:      XY
    ## bbox:           xmin: -477970.6 ymin: 385787.9 xmax: 827553.4 ymax: 1352704
    ## epsg (SRID):    32636
    ## proj4string:    +proj=utm +zone=36 +datum=WGS84 +units=m +no_defs

```r
adm2<-st_transform(adm2,crs=4326)
new_sett_sf<-st_as_sf(new_sett,coords=c("long","lat"), crs=4326)

# LOAD RAW DATA -----------------------------------------------------------
# aok_raw<-download_aok_data(keys_file = "scripts/functions/keys.R")
# write.csv(aok_raw,"inputs/2020_02/2020_02_FEB_AOK_RAW_20200301.csv")
# aok_raw<-read.csv("inputs/2020_02/2020_02_FEB_AOK_RAW_20200301.csv", stringsAsFactors = F,na.strings = c("n/a","", ""))
aok_raw<-read.csv("inputs/2020_02/2020_02_FEB_AOK_RAW_20200304_from_KOBO.csv", stringsAsFactors = F,na.strings = c("n/a","", ""))
```

## Cleaning Logs

Once cleaning logs are revieved this chunk can be filled. First step is
to compile all of the logs into one. Next we can check and implement
them using the two butteR tools commented out below.

```r
# STEP 1 COMPILE CLEANING LOGS --------------------------------------------

# cleaning_logs<-butteR::read_all_csvs_in_folder(input_csv_folder = "inputs/2020_02/cleaning_logs")
# cleaning_log<-bind_rows(cleaning_logs)

# CHECK CLEANING LOG
# butteR::check_cleaning_log()
# butteR::implement_cleaning_log()

# OUTPUT ISSUES TO AOS

# IMPLEMENT CLEANING LOG
```

# DEALING WITH NEW SETTLEMENT DATA

## Reformatting Data/ Minor Manipulations

```r
# SPATIAL JOIN
new_sett_sf<-new_sett_sf %>% st_join( adm2 %>% dplyr::select(adm2=admin2RefN))

new_sett_sf<-new_sett_sf %>%
  mutate(
    new.enum_sett_county=paste0(D.info_settlement_other,D.info_county) %>% tolower_rm_special(),
    new.adm2_sett_county=paste0(D.info_settlement_other,adm2) %>% tolower_rm_special()
  )

master_settlement_sf<-master_settlement_sf %>%
  mutate(
    mast.settlement_county_sanitized= mast.NAMECOUNTY %>% tolower_rm_special()
  )
```

## Checking/Cross Referencing Settlement Data, Cleaning Logs and Assessment Data

Compare new settlements to data after initial round of data cleaning
implementation too make sure that AO has not already dealt wih the
settlement in the cleaning log. Technically, if the settlement is
addressed in the cleaning log they should have put “cleaning_log” under
the action column in the new settlement data set.

If the settlement in the new settlement sheet has been addressed in the
cleaning log, remove it from the new settlement sheet.

```r
# CHECK IF NEW SETTLEMENTS HAVE BEEN FIXED IN CL --------------------------

aok_clean1<-aok_raw

# aok_clean1[aok_clean1$X_uuid=="b4d97108-3f34-415d-9346-f22d2aa719ea","D.info_settlement_other"]<-NA
# aok_clean1[aok_clean1$X_uuid=="b4d97108-3f34-415d-9346-f22d2aa719ea","D.info_settlement"]<-"Bajur"


remove_from_new_sett<-aok_clean1 %>%
  filter(X_uuid %in% new_sett_sf$uuid  & is.na(D.info_settlement_other))%>%
  select(X_uuid,D.info_settlement) %>% pull(X_uuid)

new_sett_sf<- new_sett_sf %>% filter(!uuid %in% remove_from_new_sett)
```

## Exact Matches

It is possible that the enumerator entered “other” for D.new_settlement
and then wrote a settlement that already existed. These cases are easy
to find. If this situation occurs, it is an error during data
collection/cleaning and should therefore be addressed in a cleaning log
for documentation purposes. The extract_matches_to_cl to transform
this information into a cleaning log, which can then be implemented with
the butteR::implement_cleaning_log function.

```r
# NEW SETTLEMENT DATA WHICH MATCHES MASTER SETTLEMENTS EXACTLY ------------

exact_matches1<-new_sett_sf %>%
  mutate(matched_where= case_when(new.enum_sett_county %in% master_settlement$mast.settlement_county_sanitized~"enum", #CHECK WITH ENUMS INPUT
                                  new.adm2_sett_county %in% master_settlement$mast.settlement_county_sanitized~"shapefile_only" #CHECK WITH SHAEFILE COUNTY
  )) %>%
  filter(!is.na(matched_where)) #ONLY RETURN EXACT MATCHES

# WRITE EXACT MATCHES TO CLEANING LOG TO THEN IMPLEMENT ON DATA.
aok_exact_matches_cl<-exact_matches_to_cl(exact_match_data = exact_matches1,user = "Jack")

#NOW IMPLEMENT THIS CLEANING LOG!
aok_clean2<-butteR::implement_cleaning_log(df = aok_clean1,df_uuid = "X_uuid",
                                           cl = aok_exact_matches_cl,
                                           cl_change_type_col = "change_type",
                                           cl_change_col = "indicator",
                                           cl_uuid = "uuid",
                                           cl_new_val = "new_value")
```

    ## character(0)
    ## [1] "NOT IN DATASET"
    ## [1] "no change_response in log"
    ## [1] "no surveys to remove in log"

## Finding The Closest Point

We will next use the butteR::closest_distance_rtree tool to find the
closest point in the master settlement list to each of the remaining new
settlements. The following code just runs this tool, cleans up the
output and also performs fuzzy string distance measurement which may be
helpful.

```r
#EXTRACT NEW SETTLEMENTS WHICH DO NO MATCH
new_sett_sf_unmatched<- new_sett_sf %>% filter(!uuid %in% exact_matches1$uuid)

#REMOVE MATCHED SETTLEMENTS FROM MASTER
master_settlement_sf_not_matched<-master_settlement_sf %>%
  filter(mast.settlement_county_sanitized %in% new_sett_sf_unmatched$new.enum_sett_county==FALSE)

# MATCH NEW SETTLEMENT TO CLOSEST SETTLEMENT IN MASTER --------------------

new_with_closest_old<-butteR::closest_distance_rtree(new_sett_sf_unmatched %>%
                                                       st_as_sf(coords=c("X","Y"), crs=4326) ,master_settlement_sf_not_matched)
#CLEAN UP DATASET
new_with_closest_old_vars<-new_with_closest_old %>%
  mutate(new.D.info_settlement_other= D.info_settlement_other %>% gsub("-","_",.)) %>%
  select(uuid,
         new.A.base=A.base,
         new.county_enum=D.info_county,
         new.county_adm2= adm2,
         new.sett_county_enum=new.enum_sett_county,
         new.sett_county_adm2= new.adm2_sett_county,
         new.D.info_settlement_other=D.info_settlement_other,
         mast.settlement=mast.NAMEJOIN,
         mast.settlement_county_sanitized,
         dist_m)



# ADD A FEW USEFUL COLUMNS - THIS COULD BE WRITTEN TO A CSV AND WOULD BE THE BEST OUTPUT TO BE REVIEWED
settlements_best_guess<-new_with_closest_old_vars %>%
  mutate(gte_50=ifelse(dist_m<500, " < 500 m",">= 500 m"),
         string_proxy=stringdist::stringdist(a =new.sett_county_enum,
                                             b= mast.settlement_county_sanitized,
                                             method= "dl", useBytes = TRUE)
  ) %>%
  arrange(dist_m,desc(string_proxy))
```

## Evaluating the closest point

The object, “settlements_best_guess,” is the best output to review and
determine if the found closest settlement in the master list should take
the place of the new settlement or not. The user has two options:

1.  The less preferred option: can write this output to a csv and
    manually assess the the table. Add a column called “action”. If the
    the master settlement should replace the “new settlement” put 1,
    otherwise 2. This file will then need to be read back into the
    script. Additionally, cleaning log entries will need to be added for
    any record in which a 1 was prescribed to action.

2.  The preferred option: The code below can be used to interactively
    assess the matched settlements in the R environment. When running
    the code in an R script a menu will prompt you through the
    interaction. Each line in the settlements_best_guess will be
    printed with the prompt: “1. fix with master, or 2. master is not
    correct.” The user must press 1 or two based on there understanding
    of the names and distances provided.

After cycling through each record, the resulting object is a list of two
data frames. The first data frame is the same list used as the input
with the additional column “action” where action=1 means fix data set
with new settlement and 2 means this is a new settlement. The second
data frame is the auto-generated cleaning log for all records where the
user decided to fix the settlement with a settlement from the master
list.

```r
# HOWEVER, TO KEEP EVERYTHING IN THE R ENVIRONMENT- HERE IS AN INTERACTIVE FUNCTION TO MODIFY THE SETTLEMENT BEST GUESS DF IN PLACE
# OUTUT WILL BE A CLEANING LOG (IF THERE ARE CHANGES TO BE MADE)
new_settlement_evaluation<-evaluate_unmatched_settlements(user= "zack",new_settlement_table = settlements_best_guess)

new_settlement_evaluation$checked_setlements
new_settlement_evaluation$cleaning_log


#IN THE ACTUAL SCRIPT IF WHERE THE CLEANING LOG IS CREATED IT WILL BE IMPLEMENTED HERE. HOWEVER SINCE THE CLEAING LOG CANNOT BE CREATED INTERACTIVELY WHEN THE DOCUMENT IS KNIT WE WILL NOT IMPLEMENT THE LOG
if(nrow(new_settlement_evaluation$cleaning_log>0)){
aok_clean3<-butteR::implement_cleaning_log(df = aok_clean2,df_uuid = "X_uuid",
                                           cl =new_settlement_evaluation$cleaning_log ,
                                           cl_change_type_col = "change_type",
                                           cl_change_col = "suggested_indicator",
                                           cl_uuid = "uuid",
                                           cl_new_val = "suggested_new_value")}
else{
  aok_clean3<-aok_clean2
}
```

## NEW SETTLEMENT OUTPUTS

### Generate New Itemset

Interactive functions cannot be used in a knitted document. Therefore,
to fully understand this functionality the user will have to use this
code in the R-Script.

For the sake of showing how to produce the new itemset for the ODK tool
and producing the new master settlement list I will create the action
column in R and name it appropriately

```r
#this would normally be unnecessary as the objects would be created from the interactive function
#############################################################################
new_settlement_evaluation<-list()
new_settlement_evaluation$checked_setlements<-settlements_best_guess
new_settlement_evaluation$checked_setlements$action<-c(1,1,2,2,2)
#############################################################################

#THIS CLEANING LOG IMPLEMENTATION IS ACTUALLY CORRECT IN THE PREVIOUS PLACEMENT

#put into itemset format
new_sets_to_add_itemset<-new_settlement_evaluation$checked_setlements %>%
  filter(action==2) %>%
  mutate(
    list_name="settlements",
    label= new.D.info_settlement_other %>% gsub("_","-", .)
  ) %>%
  select(list_name,name=new.D.info_settlement_other, label,admin_2=new.county_adm2)

# read in previous itemset
itemset<-read.csv("inputs/tool/REACH_SSD_AoK_V38_Febuary2020/itemsets.csv", strip.white = T, stringsAsFactors = T,na.strings = c(" ",""))
itemset_not_other<-itemset %>% filter(name!="other")
itemset_other<- itemset %>% filter(name=="other")


itemset_binded<-bind_rows(list(new_sets_to_add_itemset,itemset_not_other)) %>% arrange(admin_2)
itemset_full_binded<- bind_rows(list(itemset_binded,itemset_other))
```

### Generate New Master Settlement List

Only settlements determined to be actually new settlements should be
added to the master settlement list. The following code reads the master
settlement in again and adds all of the checked new settlements that are
determined to be new.

```r
# NEXT WE ADD THE NEW SETTLEMENTS TO THE MASTER LIST
master_settlement<-read.csv("inputs/48_December_2019/SSD_Settlements_V37.csv", stringsAsFactors = FALSE)

new_setts_add_to_master<-new_settlement_evaluation$checked_setlements %>%
  filter(action==2) %>%
  mutate(
    NAME= new.D.info_settlement_other %>% gsub("_","-", .),
    NAMEJOIN= mast.settlement,
    NAMECOUNTY=paste0(NAMEJOIN,new.county_adm2),
    COUNTYJOIN= new.county_adm2 ,
    DATE= month_of_assessment %>% ymd(),
    DATA_SOURC="AOK",
    IMG_VERIFD= 0
  ) %>% #get coordinates from field data back in
  left_join(new_sett_sf %>%
              st_drop_geometry_keep_coords(), by="uuid") %>%
  filter(!is.na(X)) %>%
  select(NAME,NAMEJOIN,NAMECOUNTY,COUNTYJOIN,DATE,DATA_SOURC,IMG_VERIFD,X,Y)

master_new<-bind_rows(list(new_setts_add_to_master,master_settlement %>% mutate(DATE=dmy(DATE))))
```

# AGGREATION/ANALYSIS

## County Level Aggregation

Once the cleaning log(s) have been implemented and the new settlements
dealt with it is time to aggregate/analyze the data. It is important to
note that any cleaning logs generated from the new settlement process
must be implemented and binded to the original compiled cleaning log for
documentation purposes.

The aggregation relies on functions built many years ago. It is
recommended that this process be streamlined in the future with a new
script. However, for the time being I have simply wrapped the entire
aggregation script inside a function that will be run in this chunk
(“aok_aggregate_by_county”). This function is literally the same as
the script that has been used previously for aggregating, but i have
added some useful prompts/warnings. For example, when the tool changes
the function will print out which columns are new and have not been
added to the aggregation script/function. These columns
(question/choices) need to be added to the appropriate aggregation
function (i.e aok_yes,aok_mode, etc.).

```r
###########################################
#ok assume we have imlpemented all cleaning
############################################
aok_clean3<-aok_clean2
iso_date<- Sys.Date() %>%  str_replace_all("-","_")
#maybe change the way assessment month is represented
aggregated_file_name<- paste0("outputs/", iso_date,"_reach_ssd_aok_data_analysis_basic_JAN2020_Data.csv")

#next start with the rmeove grouper stuff.
prev_round<-read.csv("inputs/2020_01/2020_02_13_reach_ssd_aok_clean_data_compiled.csv", stringsAsFactors = FALSE, na.strings=c("", " ", NA, "NA"))



aok_clean_by_county<-aggregate_aok_by_county(clean_aok_data = aok_clean3,aok_previous = prev_round, current_month = month_of_assessment)
```

    ## [1] "WARNING you missed these: "
    ## c("start", "end", "A.enumerator_id", "B.survey_start_time", "B.ki_age",
    ## "L.food_coping_livelihoods.sell_more_charcoak", "U.market_now_barriers.no_changes",
    ## "U.market_now_barriers.communual_violence", "U.market_now_barriers.robberies",
    ## "U.market_now_barriers.market_too_far", "U.market_now_barriers.sexual_violence",
    ## "U.market_now_barriers.bad_roads", "U.market_now_barriers.flooding_on_way",
    ## "U.market_now_barriers_at", "U.market_now_barriers_at.no_changes",
    ## "I.health_now.ngo_mobile", "I.facility_stocked", "I.community_healthworkers",
    ## "I.health_team", "M.education_level", "M.education_level.secondary_three_four",
    ## "M.education_level.primary_six_eight", "M.education_level.primary_one_five",
    ## "M.education_level.secondary_one_two", "Q.cash_voucher", "Q.ha_type",
    ## "Q.ha_mechanism_settlement", "Q.ha_protection_issues_women",
    ## "Q.ha_protection_issues_men", "Q.ha_fear_women", "Q.ha_fear_men",
    ## "gps", "Z._gps_latitude", "Z._gps_longitude", "Z._gps_altitude",
    ## "Z._gps_precision", "Z.tool_endTime", "LQ_1_remote", "LQ_2_unpopulated",
    ## "LQ_3_nohostcommunity", "LQ_4_food_enough_fewmeals", "LQ_5_food_insufficient_manymeals",
    ## "LQ_7_food_enough_malnutritiondeath", "LQ_8_food_enough_hungersevere",
    ## "LQ_9_cutlivation_yes_land_no", "LQ_10_crops_yes_seedstools_no",
    ## "LQ_11_forage_yes_wildfoods_no", "LQ_12_morehalf_wildfood_morethan_3meals",
    ## "LQ_13_hunting_source_yes_activity_no", "LQ_14_fishing_source_yes_activity_no",
    ## "LQ_15_market_source_yes_access_no", "LQ_16_assist_source_yes_no_distribution",
    ## "LQ_17_safe_but_serious_concerns_women", "LQ_18_safe_but_serious_concerns_men",
    ## "LQ_19_safe_but_serious_concerns_girls", "LQ_20_safe_but_serious_concerns_boys",
    ## "LQ_26_safe_yes_incident_yes", "LQ_21_mainshelter_HC_permanent",
    ## "LQ_22_mainshelter_IDP_permanent", "LQ_23_IDP_bush_yes_open_no",
    ## "LQ_24_water_borehole_no_borehole", "X__version__", "X_submission_time",
    ## "X_index")

```r
# write.csv(aok_clean_by_county,aggregated_file_name)
```

Once aggregation is completed it should be written to csv. This csv can
be binded to the long term data using the long term data aggregation
script. The long term data is the input for the Tableau workbook.

```r
# insert code here
```

## Hexagonal Aggregation

The monthly data should then be aggregated to the hexagonal grid for
fact sheet maps. The grid has already been created and can simply be
loaded. I think it might be best if this portion is eventually wrrapped
into a function. The only things that will change here are potentially
the questions to be analyzed which can be specified/modified as vector
which can be used as one of the function arguments.

It is important to note that all hexagons with \>= 2 Ki and \>= 2
settlements are reported on.

The chunk below does the following uses the master settlment list to add
coordinates to the AOK data (which does not originally have coordinates)
and performs a sptial join with the hex grid through the following
steps. 1. Read in hex grid (in UTM) 2. Convert the newly generated
master settlement list to a spatial object, transform to UTM (same as
hex grid), and strip out the important columns. 3. Add date class
columns to the clean aok data. 4. Join master settlement to grid. 5.
Merge AOK and master settlement list based on concatenated
county-settlement name vector.

```r
#READ IN HEX GRID
hex_grid <- st_read(dsn = "inputs/GIS",layer ="Grids_info") %>%
  mutate( id_grid = as.numeric(rownames(.)))
```

    ## Reading layer `Grids_info' from data source `C:\02_REACH_SSD\themes\ssd_aok\inputs\GIS' using driver `ESRI Shapefile'
    ## Simple feature collection with 2440 features and 9 fields
    ## geometry type:  POLYGON
    ## dimension:      XY
    ## bbox:           xmin: -497964.6 ymin: 375000.2 xmax: 829941.6 ymax: 1387501
    ## epsg (SRID):    32636
    ## proj4string:    +proj=utm +zone=36 +datum=WGS84 +units=m +no_defs

```r
master_sett_new<-master_new %>%
  mutate(id_sett = as.numeric(rownames(.))) %>%
  st_as_sf(coords=c("X","Y"), crs=4326) %>%
  st_transform(crs=st_crs(hex_grid)) %>%
  select(NAME:COUNTYJOIN)

aok_clean3<-aok_clean3 %>%
  mutate(date=month_of_assessment %>% ymd(),
         month=month(date),
         year=year(date),
         #if we use D.info_settlement_final the others wont match until clean
         D.settlecounty=paste0(D.info_settlement,D.info_county)) %>%
  # therefore use D.info_settlement and filter others for tutorial
  filter(D.info_settlement!="other")

sett_w_grid <- st_join(master_sett_new, hex_grid)
assessed_w_grid <-inner_join(sett_w_grid, aok_clean3, by = c("NAMECOUNTY"="D.settlecounty") )
```

Aggregate the data to the hexagon grid-level through the following
steps: 1. Calculate \# KIs per settlement (D.ki_coverage) 2. Calculate
the \# of settlements/grid, and \# KIs/grid 3. Filter out grids with
less than 2 KIs or Settlements (would be good to have citation for this
rule).

```r
grid_summary<-assessed_w_grid %>%
  group_by(NAMECOUNTY,State_id) %>%
  summarise(D.ki_coverage=n()) %>%
  group_by(State_id) %>%
  summarise(settlement_num=n() ,ki_num=sum(D.ki_coverage) )

#Filter Grids with less than 2 KIs
grid_summary_thresholded <- grid_summary %>% filter(ki_num > 1, settlement_num > 1)
```

Next we will create composite indicators to analyze at the grid level.
This may need to be edited to add or remove composite indicators later.

```r
#create new composites
assessed_w_grid_w_composite<-assessed_w_grid %>%
  mutate(
    idp_sites= ifelse(J.j2.idp_location=="informal_sites",1,0),
    IDP_present= ifelse(F.idp_now=="yes",1,0),
    IDP_time_arrive=  ifelse(F.f2.idp_time_arrive %in% c("1_month","3_month"),1,0),
    IDP_majority=  ifelse( F.f2.idp_perc %in% c("half","more_half"),1,0),
    food_inadequate= ifelse(G.food_now == "no", 1,0),
    less_one_meal = ifelse(G.meals_number %in% c("one", "Less_than_1"),1,0),
    hunger_severe_worse = ifelse(S.shock_hunger %in% c("hunger_severe", "hunger_worst"),1,0),
    wildfood_sick_alltime = ifelse(G.food_wild_emergency=="yes"|G.food_wild_proportion=="all",1,0),
    skipping_days = ifelse(G.food_coping_comsumption.skip_days == "yes",1,0),
    flooded_shelter = ifelse(J.shelter_flooding == "yes",1,0),
    fsl_composite = (food_inadequate +less_one_meal+hunger_severe_worse+wildfood_sick_alltime+skipping_days)/5
  )

#extract new columns added (should be only composite). You can add new composites above and this will still work
vars_to_avg<-names(assessed_w_grid_w_composite)[!names(assessed_w_grid_w_composite)%in%names(assessed_w_grid)]

analyzed_by_grid<-assessed_w_grid_w_composite %>%
  group_by(id_grid, State_id,month,year,date,D.info_state, D.info_county)%>%
  summarise_at(vars(vars_to_avg),mean, na.rm=T)
```

Once analyzed you can write the aggreagted data to a csv or left_join
it to the original hex data and write it out as a polygon straight for
mapping.

```r
#Filter Grids with less than 2 KIs

analyzed_by_grid_thresholded<-analyzed_by_grid %>%
  filter(State_id %in% grid_summary_thresholded$State_id)

# write.csv(
  # analyzed_by_grid_thresholded,
  # file = paste0(month_of_assessment %>% str_replace_all("-","_"),"_AoK_hex_aggregations.csv"),
  # na = "NA",
  # row.names = FALSE)


hex_grid_polygon_with_aggregated_data<-hex_grid %>% left_join(analyzed_by_grid_thresholded %>% st_drop_geometry())

# or write it out to a polgon file for mapping
#using st_write function
```

# NEXT STEPS

streamline the aok_by_county agggregation function so that it does not
have to be continusly edited.
