library(duckdb)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(tidyverse)
library(arrow)
library(fixest)
library(ggpattern)
con = dbConnect(duckdb(), shutdown = TRUE)

tab<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/june_data_week_store_sku.parquet')", sep="" )
tab1<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/july_data_week_store_sku.parquet')", sep="" )
tab2<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/aug_data_week_store_sku.parquet')", sep="" )
tab3<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/sep_data_week_store_sku.parquet')", sep="" )
tab4<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/oct_data_week_store_sku.parquet')", sep="" )
tab5<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/nov_data_week_store_sku.parquet')", sep="" )

june_mode<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/june_modes_week_store_sku.parquet')", sep="" )
july_mode<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/july_modes_week_store_sku.parquet')", sep="" )
aug_mode<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/aug_modes_week_store_sku.parquet')", sep="" )
sep_mode<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/sep_modes_week_store_sku.parquet')", sep="" )
oct_mode<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/oct_modes_week_store_sku.parquet')", sep="" )
nov_mode<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/nov_modes_week_store_sku.parquet')", sep="" )

june<-tbl(con, tab)

june1<-tbl(con, june_mode)%>%dplyr::select(week, sku_gtin, store_id, ppu)

june<-left_join(june,june1, by=c("week", "sku_gtin", "store_id"))

july <- tbl(con, tab1)
july1 <- tbl(con, july_mode) %>% dplyr::select(week, sku_gtin, store_id, ppu)
july <- left_join(july, july1, by = c("week", "sku_gtin", "store_id"))

august <- tbl(con, tab2)
august1 <- tbl(con, aug_mode) %>% dplyr::select(week, sku_gtin, store_id, ppu)
august <- left_join(august, august1, by = c("week", "sku_gtin", "store_id"))

september <- tbl(con, tab3)
september1 <- tbl(con, sep_mode) %>% dplyr::select(week, sku_gtin, store_id, ppu)
september <- left_join(september, september1, by = c("week", "sku_gtin", "store_id"))

october <- tbl(con, tab4)
october1 <- tbl(con, oct_mode) %>% dplyr::select(week, sku_gtin, store_id, ppu)
october <- left_join(october, october1, by = c("week", "sku_gtin", "store_id"))

november <- tbl(con, tab5)
november1 <- tbl(con, nov_mode) %>% dplyr::select(week, sku_gtin, store_id, ppu)
november <- left_join(november, november1, by = c("week", "sku_gtin", "store_id"))

all_data<-union_all(june,july)%>%union_all(august)

all_data1<-union_all(september, october)%>%union_all(november)

analysis_dat<-all_data%>%collect()
analysis_dat1<-all_data1%>%collect()

analysis_dat<-analysis_dat%>%union_all(analysis_dat1)
rm(analysis_dat1)
gc()
analysis_dat<-analysis_dat%>%mutate(digits=nchar(as.character(sku_gtin)))
analysis_dat<-analysis_dat%>%filter(digits==13|digits==8)
analysis_dat<-analysis_dat%>%mutate(digits=NULL)

gc()
analysis_dat<-analysis_dat%>%mutate(ppu=round(ppu,2), kron=as.numeric(floor(ppu)%%10), ore=as.numeric(round(ppu-floor(ppu),2)))

gc()
analysis_dat<-analysis_dat%>%mutate(owner=case_when(
  kjedeid=="Rema"~"Rema", 
  kjedeid%in%c("kiwi", "meny", "spar", "n\u00e6rbutikken", "joker")~"NG",
  kjedeid%in%c( "Extra", "Marked", "Matkroken", "Mega","Obs", "Prix")~"Coop"
))

gc()

analysis_dat<-analysis_dat%>%filter(is.na(ppu)==F & is.infinite(ppu)==F & is.na(store_id)==F )
gc()

analysis_dat<-analysis_dat%>%mutate(kron_9=as.numeric(kron==9), ore_9=as.numeric(ore>=.9))


analysis_dat%>%group_by( owner)%>%summarize(kron=mean(kron_9), ore=mean(ore_9))%>%arrange(kron)
analysis_dat%>%group_by( owner,kjedeid)%>%summarize(kron=mean(kron_9), ore=mean(ore_9))%>%arrange(kron)


analysis_dat%>%group_by(kjedeid, week)%>%summarize(price=mean(price))%>%filter(kjedeid!="Obs")%>%ggplot(aes(x=week, y=price, color=as.factor(kjedeid)))+geom_line()+labs(x="Week", y="Average Price", color="Chain")


analysis_dat%>%group_by(kjedeid, owner)%>%summarize(kron=mean(kron_9))%>%arrange(kron)%>%ungroup()%>%mutate(row=row_number(), owner_lab=case_when(
  owner=="Coop"~"Group 1", 
  owner=="NG"~"Group 2", 
  owner=="Rema"~"Group 3"
))%>%
  ggplot(aes(x=row, y=kron))+
  geom_bar(stat="identity", aes(fill = owner_lab))+scale_fill_stata()+theme_classic()+theme(panel.border=element_rect(color="black", fill=NA, linewidth=1.1))+scale_x_continuous(breaks=seq(1,12))+
  labs(x="Brands", y="Share of Prices ending in 9 Kroner", fill="Owner")+theme( text = element_text(size=18))


analysis_dat%>%group_by(kjedeid, owner)%>%summarize(kron=mean(as.numeric(kron==9)),ore=mean(as.numeric(ore>=.9)))%>%arrange(kron)%>%ungroup()%>%mutate(row=row_number(), owner_lab=case_when(
  owner=="Coop"~"Group 1", 
  owner=="NG"~"Group 2", 
  owner=="Rema"~"Group 3"
))%>%
  ggplot(aes(x=row, y=ore))+
  geom_bar(stat="identity", aes(fill = owner_lab))+scale_fill_stata()+theme_classic()+theme(panel.border=element_rect(color="black", fill=NA, linewidth=1.1))+scale_x_continuous(breaks=seq(1,12))+
  labs(x="Brands", y="Share of Prices ending in >=90 Ore", fill="Owner")+theme( text = element_text(size=18))




summary(feols(kron_9~owner|sku_gtin+week, data=analysis_dat), cluster="store_id")


summary(feols(log(trans+1)~log(price)+as.factor(kron==9)+as.factor(ore>=.9)|sku_gtin+week+store_id, data=analysis_dat%>%filter(owner=="Rema")), cluster="store_id")
gc()
summary(feols(log(trans+1)~log(price)+as.factor(kron==9)+as.factor(ore>=.9)|sku_gtin+week+store_id, data=analysis_dat%>%filter(owner=="NG")), cluster="store_id")
gc()
summary(feols(log(trans+1)~log(price)+as.factor(kron==9)+as.factor(ore>=.9)|sku_gtin+week+store_id, data=analysis_dat%>%filter(owner=="Coop")), cluster="store_id")
gc()

top_sku<-analysis_dat%>%group_by(sku_gtin)%>%summarize(quant=sum(quantity))%>%arrange(desc(quant))%>%slice_head(n=50000)

analysis_dat<-analysis_dat%>%filter(sku_gtin%in%top_sku$sku_gtin)

analysis_dat<-analysis_dat%>%mutate(kron=as.numeric(floor(price)%%10), ore=as.numeric(round(price-floor(price),2)), base=floor(price))


analysis_dat%>%group_by(kjedeid, store_id)%>%summarize(kron=mean(as.numeric(kron==9)))%>%ggplot(aes(x=kron, color=kjedeid))+geom_density()

product<-read_csv('//Sen.valuta.nhh.no/project/RetailFR/product_characteristics/product_data_final_vetduat_meny.csv')


june_wprod<-june%>%left_join(product, by=c("sku_gtin"="GTIN"))
dbDisconnect(con)

