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
  filter(date>as.Date("2018-05-31") & date<as.Date("2018-12-01") & sales<=1000 )



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
#all_data%>%filter(is.na(kjedeid)==F& ppu<=100)%>%mutate(base=floor(round(ppu,2)))%>%ggplot(aes(x=base))+geom_histogram(binwidth=1)
#all_data%>%filter(is.na(kjedeid)==F & ppu<=100)%>%group_by(kjedeid, store_id)%>%summarize(kron=mean(as.numeric(kron==9)))%>%
#  ggplot(aes(x=kron, color=kjedeid))+geom_density()
#all_data<-all_data%>%left_join(loc, by=c("store_id"="id"))


#all_data1<-all_data%>%dplyr::select(week, sku_gtin, store_id, sales)%>%group_by(week, sku_gtin, store_id)%>%summarize(trans=n())#, price=mode(sales))

#all_data1<-all_data%>%mutate(rand=runif())%>%filter(rand<=.01)%>%dplyr::select(week, store_id, sales, quantity,,kron, ore, kjedeid,sku_gtin, min_distance_all)%>%collect()


# all_data<-all_data%>%mutate(owner=case_when(
#   kjedeid=="Rema"~"Rema", 
#   kjedeid%in%c("kiwi", "meny", "spar", "n\u00e6rbutikken", "joker")~"NG",
#   kjedeid%in%c( "Extra", "Marked", "Matkroken", "Mega","Obs", "Prix")~"Coop"
# ))

june_dat<-all_data%>%filter(month(date)==6)%>%dplyr::select(week, sku_gtin, store_id,kjedeid, ppu, quantity)%>%group_by(week, sku_gtin, store_id,kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
write_parquet(june_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/june_data_week_store_sku.parquet")
rm(june_dat)
july_dat<-all_data%>%filter(month(date)==7)%>%dplyr::select(week, sku_gtin, store_id,kjedeid, ppu, quantity)%>%group_by(week, sku_gtin, store_id, kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
write_parquet(july_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/july_data_week_store_sku.parquet")
rm(july_dat)
gc()
aug_dat<-all_data%>%filter(month(date)==8)%>%dplyr::select(week, sku_gtin, store_id,kjedeid, ppu, quantity)%>%group_by(week, sku_gtin, store_id, kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
aug_dat%>%group_by(is.na(week))%>%tally()
write_parquet(aug_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/aug_data_week_store_sku.parquet")
rm(aug_dat)
gc()
sep_dat<-all_data%>%filter(month(date)==9)%>%dplyr::select(week, sku_gtin, store_id,kjedeid, ppu, quantity)%>%group_by(week, sku_gtin, store_id, kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
sep_dat%>%group_by(is.na(week))%>%tally()

write_parquet(sep_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/sep_data_week_store_sku.parquet")
rm(sep_dat)
gc()
oct_dat<-all_data%>%filter(month(date)==10)%>%dplyr::select(week, sku_gtin, store_id, kjedeid, ppu, quantity )%>%group_by(week, sku_gtin, store_id, kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
oct_dat%>%group_by(is.na(week))%>%tally()

write_parquet(oct_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/oct_data_week_store_sku.parquet")
rm(oct_dat)
gc()
nov_dat<-all_data%>%filter(month(date)==11)%>%dplyr::select(week, sku_gtin, store_id, kjedeid,ppu, quantity)%>%group_by(week, sku_gtin, store_id, kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
nov_dat%>%group_by(is.na(week))%>%tally()

write_parquet(nov_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/nov_data_week_store_sku.parquet")
rm(nov_dat)
gc()

# all_data%>%filter(is.na(kjedeid)==F & is.infinite(ppu)==F)%>%mutate(kron=as.numeric(as.numeric(floor(ppu)%%10)==9), ore=as.numeric(as.numeric(round(ppu-floor(ppu),2))>=.9), owner=case_when(
#   kjedeid=="Rema"~"Rema", 
#   kjedeid%in%c("kiwi", "meny", "spar", "n\u00e6rbutikken", "joker")~"NG",
#   kjedeid%in%c( "Extra", "Marked", "Matkroken", "Mega","Obs", "Prix")~"Coop"
# ))%>%group_by(kjedeid, owner)%>%summarize(kron=mean(kron), ore=mean(ore))%>%arrange(kron)%>%print(n=100)
# 
# base_share<-all_data%>%filter(is.na(kjedeid)==F)%>%mutate(kron=as.numeric(as.numeric(floor(ppu)%%10)==9), base=((ppu%/%10)*10))%>%dplyr::select(base, kron)%>%collect()
# 
# ll<-all_data1%>%group_by(week, sku_gtin, store_id,kjedeid)%>%summarize(quantity=sum(quantity), price=median(ppu))
# ll<-ll%>%mutate(price=round(price,2), kron=as.numeric(floor(price)%%10), ore=as.numeric(round(price-floor(price),2)))
# ll<-ll%>%filter(quantity>0)
# 



dbDisconnect(con)
