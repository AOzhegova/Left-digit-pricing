library(duckdb)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(tidyverse)
library(arrow)
con = dbConnect(duckdb(), shutdown = TRUE)

#stem<-"'//Sen.valuta.nhh.no/project/RetailFR/processed_scanner_data/NG/2018/09/"
stem<-"'//Sen.valuta.nhh.no/project/RetailFR/processed_scanner_data/NG/2018/**/*"
stem1<-"'//Sen.valuta.nhh.no/project/RetailFR/processed_scanner_data/Coop/2018/**/*"


tab<-paste('read_parquet(', stem, ".parquet')", sep="" )
#tab<-paste('read_parquet(', 'NG_2018_09*.parquet)', sep="" )

tab1<-paste('read_parquet(', stem1, ".parquet')", sep="" )

ng=tbl(con, tab)


ng<-ng%>%filter( sales>0 & grepl("Pant", sku_description, ignore.case=T)==F & grepl("Pose", sku_description, ignore.case=T)==F )%>%
  mutate(kron=as.numeric(floor(sales)%%10), ore=as.numeric(round(sales-floor(sales),2)), store_id=as.character(store_id) , date=as.Date(date))%>%
  filter(date>as.Date("2018-05-31") & date<as.Date("2018-12-01") & sales<=1000 )%>%mutate(week=as.numeric(strftime(date, "%U")), ppu=sales/quantity)

#june_ng<-ng%>%filter(month(date)==6)%>%dplyr::select(week, sku_gtin, store_id, ppu)%>%group_by(week, sku_gtin, store_id, ppu)%>%summarize(n=n())%>%summarize(mode=max(n))%>%collect()
#write_parquet(june_mode_price, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/june_data_week_store_sku.parquet")

#skus<-ng%>%dplyr::select("sku_gtin")%>%sample(., prob=.1)

tab1<-paste('read_parquet(', stem1, ".parquet')", sep="" )

coop=tbl(con, tab1)
coop<-coop%>%filter(sales>0 &grepl("Pant", sku_description, ignore.case=T)==F & grepl("Pose", sku_description, ignore.case=T)==F)%>%
  mutate(kron=as.numeric(floor(sales)%%10), ore=as.numeric(round(sales-floor(sales),2)), sku_gtin=as.numeric(sku_gtin), store_id=as.character(store_id), date=as.Date(date))%>%
  filter(date>as.Date("2018-05-31") & date<as.Date("2018-12-01") & sales<=1000 )

loc=tbl(con, "read_csv('//Sen.valuta.nhh.no/project/Psonr/location_with_min_distance_new.csv')")%>%mutate(id_new=case_when(
  kjedeid=="Rema"~gln,
T~id
))
loc<-loc%>%dplyr::select(id, kjedeid, paraplykjede, min_distance_all, min_distance_oth_brand, min_distance_oth_group)%>%distinct()

product=tbl(con, "read_csv('//Sen.valuta.nhh.no/project/RetailFR/product_characteristics/product_data_final_vetduat_meny.csv')")
#product<-product%>%dplyr::select(GTIN,ProductGroupName,product_group_level_01, calories,fat, sugars )

coop<-left_join(coop, loc, by=c("store_id"="id"))%>%filter(kjedeid%in%c("kiwi", "meny", "spar", "n\u00e6rbutikken", "joker", "Extra", "Marked", "Matkroken", "Mega","Obs", "Prix"))


ng<-left_join(ng, loc, by=c("store_id"="id"))%>%filter(kjedeid%in%c("kiwi", "meny", "spar", "n\u00e6rbutikken", "joker", "Extra", "Marked", "Matkroken", "Mega","Obs", "Prix"))


ng<-ng%>%mutate(week=as.numeric(strftime(date, "%U")))

coop<-coop%>%mutate(week=as.numeric(strftime(date, "%U")))
#coop<-coop%>%dplyr::select(week, sku_gtin, store_id, sales)%>%group_by(week, sku_gtin, store_id)%>%summarize(trans=n(), price=mode(sales))

#all_data<-all_data%>%left_join(product, by=c("sku_gtin"="GTIN"))

#stem2<-"'//Sen.valuta.nhh.no/project/RetailFR/processed_scanner_data/REMA_withouID/2018/**/*"
stem3<-"'//Sen.valuta.nhh.no/project/RetailFR/processed_scanner_data/REMA/2018/**/*"

tab2<-paste('read_parquet(', stem3, ".parquet')", sep="" )
rema=tbl(con, tab2)

#tab3<-paste('read_parquet(', stem3, ".parquet')", sep="" )
#rema_new=tbl(con, tab3)

#rema<-rema%>%filter(is.na(sku_gtin)==F)%>%filter( sales>0 & grepl("Pant", sku_description, ignore.case=T)==F & grepl("Pose", sku_description, ignore.case=T)==F )%>%
#  mutate(kron=as.numeric(floor(sales)%%10), ore=as.numeric(round(sales-floor(sales),2)) , date=as.Date(date))%>%
#  filter(date>as.Date("2018-05-31") & date<as.Date("2018-12-01") & sales<=1000 )

rema<-rema%>%filter(is.na(sku_gtin)==F)%>%rename(store_id="business_gln")%>%filter( sales>0 & grepl("Pant", sku_description, ignore.case=T)==F & grepl("Pose", sku_description, ignore.case=T)==F )%>%
  mutate(kron=as.numeric(floor(sales)%%10), ore=as.numeric(round(sales-floor(sales),2)) , date=as.Date(date))%>%
  filter(date>as.Date("2018-05-31") & date<as.Date("2018-12-01") & sales<=1000 )

#rema<-rema%>%filter(is.na(sku_gtin)==F)%>%rename(store_id="business_gln")%>%filter( sales>0 & grepl("Pant", sku_description, ignore.case=T)==F & grepl("Pose", sku_description, ignore.case=T)==F )%>%
#  mutate(kron=as.numeric(floor(sales)%%10), ore=as.numeric(round(sales-floor(sales),2)) , date=as.Date(date))%>%
#  filter(date>as.Date("2018-05-31") & date<as.Date("2018-12-01") & sales<=1000 )
#rema<-left_join(rema, loc, by=c("store_id"="id"))#%>%filter(kjedeid%in%c("kiwi", "meny", "spar", "n\u00e6rbutikken", "joker", "Extra", "Marked", "Matkroken", "Mega","Obs", "Prix"))
rema<-left_join(rema, loc, by=c("store_id"="id"))%>%mutate(kjedeid="Rema")#%>%filter(kjedeid%in%c("kiwi", "meny", "spar", "n\u00e6rbutikken", "joker", "Extra", "Marked", "Matkroken", "Mega","Obs", "Prix"))


rema<-rema%>%mutate(week=as.numeric(strftime(date, "%U")))
all_data <- ng%>%union_all(coop)%>%union_all( rema)

all_data<-all_data%>%mutate(ppu=sales/quantity)

june_mode<-all_data%>%filter(month(date)==6)%>%dplyr::select(week, sku_gtin, store_id, ppu)%>%group_by(week, sku_gtin, store_id, ppu)%>%summarize(n=n())%>%
  group_by(week, sku_gtin, store_id)%>%mutate(mode=max(n))%>%filter(n==mode)%>%collect()
#check<-june_mode%>%collect()
write_parquet(june_mode, "//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/june_modes_week_store_sku.parquet")
rm(june_mode)
gc()
july_mode<-all_data%>%filter(month(date)==7)%>%dplyr::select(week, sku_gtin, store_id, ppu)%>%group_by(week, sku_gtin, store_id, ppu)%>%summarize(n=n())%>%
  group_by(week, sku_gtin, store_id)%>%mutate(mode=max(n))%>%filter(n==mode)%>%collect()
write_parquet(july_mode, "//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/july_modes_week_store_sku.parquet")
rm(july_mode)
gc()

aug_mode<-all_data%>%filter(month(date)==8)%>%dplyr::select(week, sku_gtin, store_id, ppu)%>%group_by(week, sku_gtin, store_id, ppu)%>%
summarize(n=n())%>%group_by(week, sku_gtin, store_id)%>%mutate(mode=max(n))%>%filter(n==mode)%>%collect()
write_parquet(aug_mode, "//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/aug_modes_week_store_sku.parquet")
rm(aug_mode)
gc()
sep_mode<-all_data%>%filter(month(date)==9)%>%dplyr::select(week, sku_gtin, store_id, ppu)%>%group_by(week, sku_gtin, store_id, ppu)%>%
  summarize(n=n())%>%group_by(week, sku_gtin, store_id)%>%mutate(mode=max(n))%>%filter(n==mode)%>%collect()
write_parquet(sep_mode, "//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/sep_modes_week_store_sku.parquet")
rm(sep_mode)
gc()
oct_mode<-all_data%>%filter(month(date)==10)%>%dplyr::select(week, sku_gtin, store_id, ppu)%>%group_by(week, sku_gtin, store_id, ppu)%>%
  summarize(n=n())%>%group_by(week, sku_gtin, store_id)%>%mutate(mode=max(n))%>%filter(n==mode)%>%collect()
write_parquet(oct_mode, "//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/oct_modes_week_store_sku.parquet")
rm(oct_mode)
gc()
nov_mode<-all_data%>%filter(month(date)==11)%>%dplyr::select(week, sku_gtin, store_id, ppu)%>%group_by(week, sku_gtin, store_id, ppu)%>%
  summarize(n=n())%>%group_by(week, sku_gtin, store_id)%>%mutate(mode=max(n))%>%filter(n==mode)%>%collect()
write_parquet(nov_mode, "//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/nov_modes_week_store_sku.parquet")
rm(nov_mode)
gc()




dbDisconnect(con)
