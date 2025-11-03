library(duckdb)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(tidyverse)
library(arrow)
library(fixest)
library(broom)
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
analysis_dat<-analysis_dat%>%filter(is.infinite(ppu)==F)
gc()
analysis_dat<-analysis_dat%>%mutate(ppu=round(ppu,3),floor=floor(ppu),  group=(floor%/%10)*10)
categories<-read_csv("//Sen.valuta.nhh.no/project/Psonr/Data/sku_kassalcat_top95.csv")
analysis_dat<-left_join(analysis_dat, categories, by=c("sku_gtin"="sku"))

analysis_dat<-analysis_dat%>%filter(floor<=100 & is.na(kassalcat)==F)#%>%mutate(rand=runif(n=nrow(.)))#%>%filter(rand<.5)

#analysis_dat<-analysis_dat%>%filter(floor<=100)%>%mutate(rand=runif(n=nrow(.)))%>%filter(rand<.1)



output<-feols(log(quantity+1)~as.factor(floor)|week+store_id+sku_gtin+kassalcat, data=analysis_dat, se="iid")


plot<-tidy(output)
plot<-plot%>%mutate(floor=seq(1,100,1), group=(floor%/%10)*10)
ggplot(data=plot, aes(x=floor, y=estimate, color=as.factor(group)))+geom_point()+geom_smooth(method="lm")

analysis_dat<-analysis_dat%>%mutate(kron=as.numeric(as.numeric(floor(ppu)%%10)==9), ore=as.numeric(as.numeric(round(ppu-floor,2))>=.9))
analysis_dat<-analysis_dat%>%group_by(kassalcat )%>%nest()

analysis_dat<-analysis_dat%>%mutate(regs=map(data, ~feols(log(quantity+1)~log(ppu)+kron*ore+kjedeid|week+sku_gtin, data=.x, cluster="store_id")))


coef_se_df <- analysis_dat %>%
     mutate(
             tidy_out = map(regs, ~tidy(.x))  # includes estimate and std.error
         ) %>%
       select(  tidy_out) %>%
       unnest(tidy_out) %>%
       select(  term, estimate, std.error)%>%ungroup()#%>%mutate(floor=as.numeric(gsub("as\\.factor\\(floor\\)", "", term)),group=(floor%/%10)*10)


coef_se_df<-coef_se_df%>%mutate(lag=lag(estimate,1), diff=estimate-lag, isnine=ifelse(floor%in%c(9,19,29,39,49,59,69,79,89,99), 1, 0), iszero=ifelse(floor%in%c(10,20,30,40,50,60,70,80,90,100), 1, 0))

ggplot(data=coef_se_df, aes(x=floor, y=estimate,group=as.factor(kjedeid), color=as.factor(group)))+geom_point()+geom_smooth(method="lm")
