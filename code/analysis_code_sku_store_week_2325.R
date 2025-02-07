library(duckdb)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(tidyverse)
library(arrow)
library(fixest)
library(ggpattern)
con = dbConnect(duckdb(), shutdown = TRUE)

tab<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Project Folder/Data/Aggregate Data/june_data_week_store_sku.parquet')", sep="" )
tab1<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Project Folder/Data/Aggregate Data/july_data_week_store_sku.parquet')", sep="" )
tab2<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Project Folder/Data/Aggregate Data/aug_data_week_store_sku.parquet')", sep="" )
tab3<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Project Folder/Data/Aggregate Data/sep_data_week_store_sku.parquet')", sep="" )
tab4<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Project Folder/Data/Aggregate Data/oct_data_week_store_sku.parquet')", sep="" )
tab5<-paste('read_parquet(',"'//Sen.valuta.nhh.no/project/Psonr/Project Folder/Data/Aggregate Data/nov_data_week_store_sku.parquet')", sep="" )

june<-tbl(con, tab)

july<-tbl(con, tab1)
august<-tbl(con, tab2)
sept<-tbl(con, tab3)
oct<-tbl(con, tab4)
nov<-tbl(con, tab5)

all_data<-union_all(june,july)%>%union_all(august)

all_data1<-union_all(sept, oct)%>%union_all(nov)

analysis_dat<-all_data%>%collect()
analysis_dat1<-all_data1%>%collect()

analysis_dat<-analysis_dat%>%union_all(analysis_dat1)
rm(analysis_dat1)
gc()
analysis_dat<-analysis_dat%>%mutate(digits=nchar(as.character(sku_gtin)))
analysis_dat<-analysis_dat%>%filter(digits==13|digits==8)
analysis_dat<-analysis_dat%>%mutate(digits=NULL)

gc()
analysis_dat<-analysis_dat%>%mutate(price=round(price,2), kron=as.numeric(floor(price)%%10), ore=as.numeric(round(price-floor(price),2)))

gc()
analysis_dat<-analysis_dat%>%mutate(owner=case_when(
  kjedeid=="Rema"~"Rema", 
  kjedeid%in%c("kiwi", "meny", "spar", "n\u00e6rbutikken", "joker")~"NG",
  kjedeid%in%c( "Extra", "Marked", "Matkroken", "Mega","Obs", "Prix")~"Coop"
))

gc()

analysis_dat<-analysis_dat%>%filter(is.na(price)==F & is.infinite(price)==F & is.na(store_id)==F )
gc()

analysis_dat<-analysis_dat%>%mutate(kron_9=as.numeric(kron==9))


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

