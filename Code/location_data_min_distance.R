library(arrow)
library(tidyverse)

library(sf)
library(fixest)
library(modelsummary)
location<-sf::st_read("//Sen.valuta.nhh.no/project/Psonr/NARING_Dagligvare.gdb.zip")
location1<-location%>%distinct(adresse, postnr, poststed,.keep_all=T)

min_distance_to_other_category <- function(gdf, competitor) {
  min_distances <- numeric(nrow(gdf))
  
  # Loop over each row in the sf object
  for (i in 1:nrow(gdf)) {
    current_row <- gdf[i, ]
    current_store<-current_row$id
   # print(current_store)
    # Filter out rows that belong to the same category
    other_stores <- gdf%>%filter(id!=current_store)
    if(competitor==1){
      other_stores <- other_stores%>%filter(paraplykjede!=current_row$paraplykjede)
    }else if(competitor==2){
      other_stores <- other_stores%>%filter(kjedeid!=current_row$kjedeid)
      
    }
   # print(nrow(other_stores))
    # Calculate distances to other categories
    distances <- st_distance(rep(current_row$Shape, nrow(other_stores)), other_stores$Shape, by_element = T)
    print(i)
    # Store the minimum distance
    min_distances[i] <- min(distances, na.rm=T)
   
  }
  #print(min_distances)
  
  return(min_distances)
}
check_distance<-min_distance_to_other_category(location1,0)
check_distance_otstore<-min_distance_to_other_category(location1,1)
check_distance_othgroup<-min_distance_to_other_category(location1,2)

location1$min_distance_all<-check_distance
location1$min_distance_oth_brand<-check_distance_otstore
location1$min_distance_oth_group<-check_distance_othgroup

location2<-location1 %>% st_drop_geometry()

write_csv(location2, "//Sen.valuta.nhh.no/project/Psonr/location_with_min_distance_new.csv")