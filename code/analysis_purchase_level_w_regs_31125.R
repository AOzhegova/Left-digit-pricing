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


avgs<-all_data%>%group_by(store_id,week, kjedeid)%>%summarize(avg_kron=mean(as.numeric(kron==9)), avg_ore=mean(as.numeric(ore>=.9)))%>%collect()
avgs<-avgs%>%mutate(owner=case_when(
  kjedeid=="Rema"~"Rema", 
  kjedeid%in%c("kiwi", "meny", "spar", "n\u00e6rbutikken", "joker")~"NG",
  kjedeid%in%c( "Extra", "Marked", "Matkroken", "Mega","Obs", "Prix")~"Coop"
))

gm <- tibble::tribble(
  ~raw,        ~clean,          ~fmt,
  "nobs",      "Observations",             0,
)
kron<-summary(feols(avg_kron~owner, cluster="store_id", data=avgs), cluster="store_id")
ore<-summary(feols(avg_ore~owner, cluster="store_id", data=avgs), cluster="store_id")

modelsummary(list("Kroner"=kron,"Ore"=ore), stars=T, coef_map =c("(Intercept)"="Coop (intercept)", "ownerNG"="Norgesgruppen", "ownerRema"="Rema") , gof_map = gm , output="markdown")

avgs<-avgs%>%ungroup()%>%mutate(kjedeid=relevel(as.factor(kjedeid), ref="Prix"))
kron<-summary(feols(avg_kron~kjedeid, cluster="store_id", data=avgs), cluster="store_id")
ore<-summary(feols(avg_ore~kjedeid, cluster="store_id", data=avgs), cluster="store_id")

modelsummary(list("Kroner"=kron,"Ore"=ore), stars=T, gof_map = gm, coef_map = c(
  "(Intercept)" = "Prix (intercept) (c)", 
  "kjedeidExtra" = "Extra (c)", 
  "kjedeidMega" = "Mega (c)", 
  "kjedeidMarked" = "Marked (c)", 
  "kjedeidMatkroken" = "Matkroken (c)", 
  "kjedeidObs" = "Obs (c)", 
  "kjedeidjoker" = "Joker (n)", 
  "kjedeidkiwi" = "Kiwi (n)", 
  "kjedeidmeny" = "Meny (n)", 
  "kjedeidnærbutikken" = "Nærbutikken (n)", 
  "kjedeidspar" = "Spar (n)",
  "kjedeidRema" = "Rema"
) )

avgs<-avgs%>%mutate(format = case_when(
    kjedeid %in% c("kiwi", "Extra", "Rema", "Prix") ~ "discount",
    kjedeid %in% c("spar", "meny", "Mega") ~ "supermarket",
    kjedeid %in% c("joker", "nærbutikken", "Marked", "Matkroken") ~ "convenience",
    kjedeid == "Obs" ~ "hypermarket",
    TRUE ~ NA_character_  # Default case for unexpected values
  ))

kron<-summary(feols(avg_kron~format|owner, cluster="store_id", data=avgs), cluster="store_id")
ore<-summary(feols(avg_ore~format|owner, cluster="store_id", data=avgs), cluster="store_id")

modelsummary(list("Kroner"=kron,"Ore"=ore), stars=T, gof_map = gm, coef_map = c(
  "(Intercept)" = "Convenience (intercept) ", 
  "formatdiscount"="Discount",
  "formathypermarket"="hypermarket",
  "formatsupermarket"="supermarket"
), output="markdown")


# 
# 
# june_dat<-all_data%>%filter(month(date)==6)%>%dplyr::select(week, sku_gtin, store_id,kjedeid, ppu, quantity)%>%group_by(week, sku_gtin, store_id,kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
# write_parquet(june_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/june_data_week_store_sku.parquet")
# rm(june_dat)
# july_dat<-all_data%>%filter(month(date)==7)%>%dplyr::select(week, sku_gtin, store_id,kjedeid, ppu, quantity)%>%group_by(week, sku_gtin, store_id, kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
# write_parquet(july_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/july_data_week_store_sku.parquet")
# rm(july_dat)
# gc()
# aug_dat<-all_data%>%filter(month(date)==8)%>%dplyr::select(week, sku_gtin, store_id,kjedeid, ppu, quantity)%>%group_by(week, sku_gtin, store_id, kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
# aug_dat%>%group_by(is.na(week))%>%tally()
# write_parquet(aug_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/aug_data_week_store_sku.parquet")
# rm(aug_dat)
# gc()
# sep_dat<-all_data%>%filter(month(date)==9)%>%dplyr::select(week, sku_gtin, store_id,kjedeid, ppu, quantity)%>%group_by(week, sku_gtin, store_id, kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
# sep_dat%>%group_by(is.na(week))%>%tally()
# 
# write_parquet(sep_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/sep_data_week_store_sku.parquet")
# rm(sep_dat)
# gc()
# oct_dat<-all_data%>%filter(month(date)==10)%>%dplyr::select(week, sku_gtin, store_id, kjedeid, ppu, quantity )%>%group_by(week, sku_gtin, store_id, kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
# oct_dat%>%group_by(is.na(week))%>%tally()
# 
# write_parquet(oct_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/oct_data_week_store_sku.parquet")
# rm(oct_dat)
# gc()
# nov_dat<-all_data%>%filter(month(date)==11)%>%dplyr::select(week, sku_gtin, store_id, kjedeid,ppu, quantity)%>%group_by(week, sku_gtin, store_id, kjedeid)%>%summarize(trans=n(), price=mean(ppu), quantity=sum(quantity))%>%collect()
# nov_dat%>%group_by(is.na(week))%>%tally()
# 
# write_parquet(nov_dat, "//Sen.valuta.nhh.no/project/Psonr/Aggregate Data/nov_data_week_store_sku.parquet")
# rm(nov_dat)
# gc()
# 


dbDisconnect(con)
