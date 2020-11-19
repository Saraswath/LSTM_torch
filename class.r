library("dplyr")
library(tools)
library(R.utils)
library(data.table)
library(infuser)
library(tibble)
library('stringr')

source('~/AFS/AFS_connection.R')
source('~/AFS/Redshift_connection.R')
source('~/AFS/sql_utils.R')
source('~/AFS/to_redshift.R')

# Creating desired tables in datamining schema
dbSendUpdateFromFile(conn,"~/AFS/create_table.sql")

# Incremental load logic
upload_data_inc <- function(conn, afsbookings){
  
  temp_table <- dbSendUpdate(conn, "create temporary table in_afs_class_temp 
                               (like datamining.in_afs_class)")
  
  insert_table <- 'in_afs_class_temp'
  values_to_insert <- afsbookings
  
  to_redshift(conn, insert_table,values_to_insert)
  
  query <- "delete from datamining.in_afs_class where id in 
            ( select dm.id from datamining.in_afs_class dm
            inner join in_afs_class_temp tmp
            on dm.id =tmp.id)"
  
  common <- dbSendUpdate(conn,query)
  
  dbSendUpdate(conn,"insert into datamining.in_afs_class select * from in_afs_class_temp")
  
  # insert_table <- 'datamining.in_afs_class'
  # values_to_insert <- afsbookings
  # 
  # to_redshift(conn, insert_table,values_to_insert)
  
  # dbSendUpdate(conn,"drop table in_afs_class_temp")
}

# Update to datamining
upload_data <- function(conn, afsbookings){
  insert_table <- 'datamining.in_afs_class'
  values_to_insert <- afsbookings
  to_redshift(conn, insert_table,values_to_insert)
}


# Update the logs for the table
update_inc_logs <- function(conn, afsbookings){
  dbSendUpdate(conn,"update datamining.in_afs_inc_load_logs set latest_flag = FALSE where 
                          table_name = 'datamining.in_afs_class'")
  row_count <- dbGetQuery(conn,"select count(1) from datamining.in_afs_class")
  extracted_row_count <- nrow(afsbookings)
  latest_updated_time <- Sys.time()
  table_name <- 'datamining.in_afs_class'
  latest_flag <- TRUE
  
  df <- data.frame(table_name,extracted_row_count,latest_updated_time, row_count, latest_flag)
  print(df)
  insert_table <- 'datamining.in_afs_inc_load_logs'
  values_to_insert <- df
  to_redshift(conn, insert_table,values_to_insert)
  
}

#update the settings
update_inc_load_settings <- function(conn, afsbookings,NoOfRows){
  
  table_name <- 'datamining.in_afs_class'
  latest_updated_time <- max(afsbookings$updated_at)
  
  if(NoOfRows > 0){
    
    query = paste("update datamining.in_afs_inc_load_settings set latest_updated_time = '",latest_updated_time,"' where table_name = 'datamining.in_afs_class'")
    print("settings query")
    print(query)
    dbSendUpdate(conn,query)
  }
  else{
    
    df <- data.frame(table_name,latest_updated_time)
    print(df)
    insert_table <- 'datamining.in_afs_inc_load_settings'
    values_to_insert <- df
    to_redshift(conn, insert_table,values_to_insert)
  }
  
}

#Check if data exists in Load Settings table
NoOfRows <- dbGetQuery(conn,"select count(1) from datamining.in_afs_inc_load_settings
                              where table_name = 'datamining.in_afs_class' 
                              ")
print("no of rows")
print(NoOfRows)

#Check if settings data is found on the Inc Load Settings file
if(NoOfRows > 0) {
  #Get data only after the max updated date
  
  LastUpdatedDates <- dbGetQuery(conn,"select latest_updated_time from datamining.in_afs_inc_load_settings 
                                where table_name = 'datamining.in_afs_class'")
  
  print("last updates data")
  print(LastUpdatedDates)
  query <- paste("select id,
name,
start_timestamp,
end_timestamp,
min_age,
max_age,
organiser_name,
description,
latitude,
longitude,
address,
city,
state,
zipcode,
image_url,
video_url,
term_type,
is_private,
is_published,
is_delete,
is_draft,
equipments_required,
amenities_available,
is_active,
total_likes,
overall_rating,
is_reco_created,
is_reco_updated,
reco_created_at,
reco_updated_at,
role_id,
space_id,
subspace_id,
created_at,
updated_at,
created_by,
updated_by,
is_online,
online_link,
online_link_msg from public.class
                                    where updated_at >= ","'",LastUpdatedDates,"'" )
  print(query)
  afsbookings <- dbGetQuery(conn_afs,query)
  print("incremental load")
  if(nrow(afsbookings)>0)
  {
    upload_data_inc(conn, afsbookings)
    update_inc_load_settings(conn, afsbookings,NoOfRows)
    update_inc_logs(conn, afsbookings)
  }
  
} else {
  #Full data load since no data is found
  
  dbSendUpdate(conn,"truncate table datamining.in_afs_class")
  
  afsbookings <- dbGetQuery(conn_afs,"select id,
name,
start_timestamp,
end_timestamp,
min_age,
max_age,
organiser_name,
description,
latitude,
longitude,
address,
city,
state,
zipcode,
image_url,
video_url,
term_type,
is_private,
is_published,
is_delete,
is_draft,
equipments_required,
amenities_available,
is_active,
total_likes,
overall_rating,
is_reco_created,
is_reco_updated,
reco_created_at,
reco_updated_at,
role_id,
space_id,
subspace_id,
created_at,
updated_at,
created_by,
updated_by,
is_online,
online_link,
online_link_msg from public.class")
  print(class(afsbookings))
  print("full load")
  upload_data(conn, afsbookings)
  update_inc_load_settings(conn, afsbookings,NoOfRows)
  update_inc_logs(conn, afsbookings)
}

## Make sure to close the connection once done
dbDisconnect(conn_afs)
dbDisconnect(conn)