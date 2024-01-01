#PART 1

library(rgdal) # used to load the shapefiles
library(dplyr) # because I wouldn't leave home without it
library(tibble)

sourced_path <- parent.frame(2)$ofile
if (!is.null(sourced_path)) setwd(dirname(sourced_path))

AOK <- read.csv("2020_07_01_REACH_SSD_settlement_aggregated_longterm_AoK_May2020_Data.csv")

Grids <- readOGR(dsn = "./shp",layer ="Grids_info")
settlements <- readOGR(dsn = "./shp",layer ="SSD_Settlements")

plot(Grids)
plot(settlements, add = TRUE)

# Set a unique identifier for both of the data frames
Grids@data <- mutate(Grids@data, id_grid = as.numeric(rownames(Grids@data)))
settlements@data <- mutate(settlements@data, id_sett = as.numeric(rownames(settlements@data)))

# The settlements get the value of the grids they are on top of
setgrids <- over(settlements, Grids)
# # the order didn't change so re add id_sett to the new table
setgrids <- mutate(setgrids, id_sett = as.numeric(rownames(setgrids)))
#Now join each original settlement to its grid location
setgrids <- left_join(settlements@data, setgrids, by = c("id_sett" = "id_sett"))

#create a new column of the unique settlement column in the long term data to match the settlements one
AOK$NAMECOUNTY <- AOK$D.settlecounty

Assessed <-left_join(setgrids, AOK, by = ("NAMECOUNTY") )

#Let us first filter the current month data. This will need to be modified monthly
Assessed_current <- Assessed %>% filter(month == "2020-05-01")


#Make sure there are no duplicates in the unique column
distinct(Assessed_current,NAMECOUNTY, .keep_all= TRUE)

#No Null values allowed
Assessed_current <- Assessed_current %>% filter(!is.na(D.settlecounty))

#Settlements that received assistance
Assessed_current$received_assistance <- 0
Assessed_current$received_assistance[(Assessed_current$Q.assistance_now == "yes")] <- 1

#let us check how many settlements and KIs we have per grid
grid_summary <- Assessed_current %>% group_by(State_id) %>%
  summarise(settlement_num = length(D.settlecounty), ki_num= sum(D.ki_coverage), assistance_received= sum(received_assistance))


#Filter Grids with less than 2 KIs
grids_threshold <- grid_summary %>% filter(ki_num > 1, assistance_received >=1 )


#PART 2

#Select the columns we need

Assessed_AoK_AAP <- Assessed_current %>%
  select(starts_with("Q."),id_grid,id_sett,D.ki_coverage, NAME,
         COUNTYJOIN,State_id )

Assessed_AoK_assisted <- Assessed_AoK_AAP %>% filter(Q.assistance_now == "yes")

#Composite indicator: Average proportion of assessed settlements reporting 'no' to four indicators of dissatisfaction with different aspects of humanitarian service delivery2
#most people are receiving enough information about the assistance that is available
#most people feel like their opinions are considered enough by humanitarian actors
#most people satisfied with the assistance received in past six months
#most people feel respected by humanitarian actors

#most people are not receiving enough information about the assistance that is available
Assessed_AoK_assisted$assistance_info_not_received <- 0
Assessed_AoK_assisted$assistance_info_not_received[(Assessed_AoK_assisted$Q.assistance_info == "no")] <- 1

#most people feel like their opinions are not considered enough by humanitarian actors
Assessed_AoK_assisted$opinion_not_considered <- 0
Assessed_AoK_assisted$opinion_not_considered[(Assessed_AoK_assisted$Q.opinions_considered == "no")] <- 1

#most people not satisfied with the assistance received in past six months
Assessed_AoK_assisted$assistance_unsatisfactory <- 0
Assessed_AoK_assisted$assistance_unsatisfactory[(Assessed_AoK_assisted$Q.ha_satisfied == "no")] <- 1

#most people feel respected by humanitarian actors
Assessed_AoK_assisted$humanitarians_disrespectful <- 0
Assessed_AoK_assisted$humanitarians_disrespectful[(Assessed_AoK_assisted$Q.ha_respect == "no")] <- 1

#Proportion of assessed settlements reporting that any form of humanitarian assistance had been accessed in their settlement in the six months prior to data collection3
grids_threshold$Prop_assistance_received <- grids_threshold$assistance_received / grids_threshold$settlement_num
grid_summary$Prop_assistance_received <- grid_summary$assistance_received / grid_summary$settlement_num

#Assessed settlements reporting that most people are receiving enough information about the assistance available to them
Assessed_AoK_assisted$assistance_info_received <- 0
Assessed_AoK_assisted$assistance_info_received[(Assessed_AoK_assisted$Q.assistance_info == "yes")] <- 1

#Assessed settlements reporting that most people feel like their opinions are considered enough by humanitarian actors
Assessed_AoK_assisted$opinion_considered <- 0
Assessed_AoK_assisted$opinion_considered[(Assessed_AoK_assisted$Q.opinions_considered == "yes")] <- 1

#Assessed settlements reporting that most people perceive humanitarian assistance as going to those who need it most
Assessed_AoK_assisted$targetting_fair <- 0
Assessed_AoK_assisted$targetting_fair[(Assessed_AoK_assisted$Q.ha_targeting_far == "yes")] <- 1

#assessed settlements reporting that humanitarian assistance has exposed them to protection concerns
Assessed_AoK_assisted$protection_concern <- 0
Assessed_AoK_assisted$protection_concern[(Assessed_AoK_assisted$Q.ha_protection_concern == "yes")] <- 1

#assessed settlements reporting that most people perceive that humanitarian assistance received is the most relevant
Assessed_AoK_assisted$most_relevant <- 0
Assessed_AoK_assisted$most_relevant[(Assessed_AoK_assisted$Q.most_relevant == "yes")] <- 1

#assessed settlements reporting that most people feel respected by humanitarian actors
#most people feel respected by humanitarian actors
Assessed_AoK_assisted$humanitarians_respectful <- 0
Assessed_AoK_assisted$humanitarians_respectful[(Assessed_AoK_assisted$Q.ha_respect == "yes")] <- 1


#Numbers per grid per reported indicator
grids_summary_AAP <- Assessed_AoK_assisted %>%
  select(State_id,id_grid,humanitarians_respectful,most_relevant,protection_concern,targetting_fair, opinion_considered, assistance_info_received,
         humanitarians_disrespectful, assistance_unsatisfactory, opinion_not_considered, assistance_info_not_received)%>%
  group_by_(.dots = c("id_grid", "State_id"))%>%
  summarise_all(funs(sum))

grids_summary_AAP <- left_join(grids_threshold,grids_summary_AAP, by = c("State_id" = "State_id"))


#Proportion of assessed settlements reporting dissatisfaction with the humanitarian assistance received in the six months prior to data collection
grids_summary_AAP$Prop_assistance_unsatisfactory <-grids_summary_AAP$assistance_unsatisfactory/grids_summary_AAP$assistance_received

#Proportion of assessed settlements reporting that most people are receiving enough information about the assistance available to them
grids_summary_AAP$assistance_info_received <-grids_summary_AAP$assistance_info_received/grids_summary_AAP$assistance_received

#Proportion of assessed settlements reporting that most people feel like their opinions are considered enough by humanitarian actors
grids_summary_AAP$opinion_considered <-grids_summary_AAP$opinion_considered/grids_summary_AAP$assistance_received


#Proportion of assessed settlements reporting that most people perceive humanitarian assistance as going to those who need it most
grids_summary_AAP$targetting_fair <-grids_summary_AAP$targetting_fair/grids_summary_AAP$assistance_received

#Proportion of assessed settlements reporting that humanitarian assistance has exposed them to protection concerns
grids_summary_AAP$protection_concern <-grids_summary_AAP$protection_concern / grids_summary_AAP$assistance_received

#Proportion of assessed settlements reporting that most people perceive that humanitarian assistance received is most relevant
grids_summary_AAP$most_relevant <-grids_summary_AAP$most_relevant / grids_summary_AAP$assistance_received

#Proportion of assessed settlements reporting that most people feel respected by humanitarian actors
grids_summary_AAP$humanitarians_respectful <-grids_summary_AAP$humanitarians_respectful / grids_summary_AAP$assistance_received

# AAP Composite indicators
#Composite indicator: Average proportion of assessed settlements reporting 'no' to four indicators of dissatisfaction with different aspects of humanitarian service delivery2
#Proportion of assessed settlements reporting that most people are not receiving enough information about the assistance that is available
grids_summary_AAP$assistance_info_not_received <-grids_summary_AAP$assistance_info_not_received / grids_summary_AAP$assistance_received

#Proportion of assessed settlements reporting that most people feel like their opinions are not considered enough by humanitarian actors
grids_summary_AAP$opinion_not_considered <-grids_summary_AAP$opinion_not_considered / grids_summary_AAP$assistance_received

#Proportion of assessed settlements reporting that most people not satisfied with the assistance received in past six months
grids_summary_AAP$assistance_unsatisfactory <-grids_summary_AAP$assistance_unsatisfactory / grids_summary_AAP$assistance_received

#Proportion of assessed settlements reporting that most people feel not respected by humanitarian actors
grids_summary_AAP$humanitarians_disrespectful <-grids_summary_AAP$humanitarians_disrespectful / grids_summary_AAP$assistance_received

# AAP Composite indicators
grids_summary_AAP$AAP_Composite <- (grids_summary_AAP$assistance_info_not_received+grids_summary_AAP$opinion_not_considered+grids_summary_AAP$assistance_unsatisfactory+grids_summary_AAP$humanitarians_disrespectful)/4


  write.csv(
    grids_summary_AAP,
    file = "2020_05/grids_summary_AAP.csv",
    na = "NA",
    row.names = FALSE)

  write.csv(
    grid_summary,
    file = "2020_05/grids_coverage_AAP.csv",
    na = "NA",
    row.names = FALSE)

  write.csv(
    Assessed_current,
    file = "2020_05/assessed_Settlements.csv",
    na = "NA",
    row.names = FALSE)
