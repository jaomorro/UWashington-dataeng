# Databricks notebook source
# contribution data
from pyspark.sql.types import StructType,StructField,StringType,LongType,IntegerType
import pyspark.sql.functions as sf

# COMMAND ----------

# MAGIC %md ##### Load in campaign contribution files and create dataframe

# COMMAND ----------

# campaign contribution data

# list of files
campaign_link_files = ["itcont08.txt","itcont10.txt","itcont12.txt","itcont14.txt","itcont16.txt","itcont18.txt"]

# define a schema
schema_fin = StructType([StructField("CMTE_ID", StringType()),	StructField("AMNDT_IND", StringType()),	StructField("RPT_TP", StringType()),	StructField("TRANSACTION_PGI", StringType()),	StructField("IMAGE_NUM", LongType()),	StructField("TRANSACTION_TP", StringType()),	StructField("ENTITY_TP", StringType()),	StructField("NAME", StringType()),	StructField("CITY", StringType()),	StructField("STATE", StringType()),	StructField("ZIP_CODE", StringType()),	StructField("EMPLOYER", StringType()),	StructField("OCCUPATION", StringType()),	StructField("TRANSACTION_DT", StringType()),	StructField("TRANSACTION_AMT", IntegerType()),	StructField("OTHER_ID", StringType()),	StructField("TRAN_ID", StringType()),	StructField("FILE_NUM", IntegerType()),	StructField("MEMO_CD", StringType()),	StructField("MEMO_TEXT", StringType()),	StructField("SUB_ID", LongType())])


# create dataframe (loop through files)
for i in range(len(campaign_link_files)):
  file = campaign_link_files[i]
  file_yr = "20"+file[6:8]
  file_path = f"dbfs:/autumn_2019/jaomorro/campaign_finance_data/ind_contributions/{file}"
  if i == 0:
    fin_data = spark.read.option("delimiter", "|").csv(file_path, header="false", schema=schema_fin)
    fin_data = fin_data.withColumn("FILE_YR",sf.lit(file_yr))
  else:
    data_load = spark.read.option("delimiter", "|").csv(file_path, header="false", schema=schema_fin)
    data_load = data_load.withColumn("FILE_YR",sf.lit(file_yr))
    fin_data = fin_data.union(data_load)

# COMMAND ----------

# MAGIC %md ##### Save data as a parquet

# COMMAND ----------

#fin_data.write.parquet("dbfs:/autumn_2019/jaomorro/parq_files/fin_data.parquet")
fin_data_parq = spark.read.parquet("dbfs:/autumn_2019/jaomorro/parq_files/fin_data.parquet")

# COMMAND ----------

# MAGIC %md ##### Load in campaign link files and create dataframe

# COMMAND ----------

# campaign contribution link data
# these files are the bridge between candidates and individual contributions

# list of files
campaign_link_files = ["ccl08.txt","ccl10.txt","ccl12.txt","ccl14.txt","ccl16.txt","ccl18.txt"]

# define the schema
schema_ccl = StructType([StructField("CAND_ID", StringType()),	StructField("CAND_ELECTION_YR", StringType()),	StructField("FEC_ELECTION_YR", StringType()),	StructField("CMTE_ID", StringType()),	StructField("CMTE_TP", StringType()),	StructField("CMTE_DSGN", StringType()),	StructField("LINKAGE_ID", StringType())])

# Create dataframe (loop through files)
for i in range(len(campaign_link_files)):
  file = campaign_link_files[i]
  file_yr = "20"+file[3:5]
  file_path = f"dbfs:/autumn_2019/jaomorro/campaign_finance_data/campaign_link/{file}"
  if i == 0:
    ccl_data = spark.read.option("delimiter", "|").csv(file_path, header="false", schema=schema_ccl)
    ccl_data = ccl_data.withColumn("FILE_YR",sf.lit(file_yr))
  else:
    data_load = spark.read.option("delimiter", "|").csv(file_path, header="false", schema=schema_ccl)
    data_load = data_load.withColumn("FILE_YR",sf.lit(file_yr))
    ccl_data = ccl_data.union(data_load)

# COMMAND ----------

# MAGIC %md ##### Load in candidate master files and create dataframe

# COMMAND ----------

# candidate master data

# list of files
candidate_master_files = ["cn08.txt","cn10.txt","cn12.txt","cn14.txt","cn16.txt","cn18.txt",]	

# define the schema
schema_cnd = StructType([StructField("CAND_ID", StringType()),	StructField("CAND_NAME", StringType()),	StructField("CAND_PTY_AFFILIATION", StringType()),	StructField("CAND_ELECTION_YR", StringType()),	StructField("CAND_OFFICE_ST", StringType()),	StructField("CAND_OFFICE", StringType()),	StructField("CAND_OFFICE_DISTRICT", StringType()),StructField("CAND_ICI", StringType()),StructField("CAND_STATUS", StringType()),StructField("CAND_PCC", StringType()),StructField("CAND_ST1", StringType()),StructField("CAND_ST2", StringType()),StructField("CAND_CITY", StringType()),StructField("CAND_ST", StringType()),StructField("CAND_ZIP", StringType())])

# Create dataframe (loop through files)
for i in range(len(candidate_master_files)):
  file = candidate_master_files[i]
  file_yr = "20"+file[2:4]
  file_path = f"dbfs:/autumn_2019/jaomorro/campaign_finance_data/candidate_master/{file}"
  if i == 0:
    cnd_data = spark.read.option("delimiter", "|").csv(file_path, header="false", schema=schema_cnd)
    cnd_data = cnd_data.withColumn("FILE_YR",sf.lit(file_yr))
  else:
    data_load = spark.read.option("delimiter", "|").csv(file_path, header="false", schema=schema_cnd)
    data_load = data_load.withColumn("FILE_YR",sf.lit(file_yr))
    cnd_data = cnd_data.union(data_load)
    
# parse out names : split the CAND_NAME field into first name, last name, and full name fields
split_column = sf.split(cnd_data["CAND_NAME"], ",")
cnd_data = cnd_data.withColumn("first_name", split_column.getItem(1)).withColumn("last_name", split_column.getItem(0)).withColumn("full_name", sf.concat(split_column.getItem(1), sf.lit(" "), split_column.getItem(0)))

# COMMAND ----------

# MAGIC %md ##### Load in election result files and create dataframes

# COMMAND ----------

# create election result dataframes
file_path = f"dbfs:/autumn_2019/jaomorro/election_data/"
house_results = spark.read.option("delimiter",",").csv(f"{file_path}HouseResults.csv", header="true")
senate_results = spark.read.option("delimiter",",").csv(f"{file_path}SenateResults.csv", header="true")
president_results = spark.read.option("delimiter",",").csv(f"{file_path}PresidentResults.csv", header="true")

# add column "cand_office" so we can join to the campaign contribution data later on
house_results = house_results.withColumn("cand_office", sf.lit("H"))
senate_results = senate_results.withColumn("cand_office", sf.lit("S"))
president_results = president_results.withColumn("cand_office", sf.lit("P"))

# split name to get first/last name - house
df = house_results.select(house_results.candidate)
split_col = sf.split(df["candidate"]," ")
house_results = house_results.withColumn("cand_first_name", split_col.getItem(0))
house_results = house_results.withColumn("cand_last_name", split_col.getItem(sf.size(split_col)-1))

# split name to get first/last name - senate
df = senate_results.select(senate_results.candidate)
split_col = sf.split(df["candidate"]," ")
senate_results = senate_results.withColumn("cand_first_name", split_col.getItem(0))
senate_results = senate_results.withColumn("cand_last_name", split_col.getItem(sf.size(split_col)-1))

# split name to get first/last name - president
df = president_results.select(president_results.candidate)
split_col = sf.split(df["candidate"]," ")
president_results = president_results.withColumn("cand_first_name", split_col.getItem(sf.size(split_col)-1))
president_results = president_results.withColumn("cand_last_name_v1", split_col.getItem(0))
president_results = president_results.withColumn("cand_last_name", sf.regexp_replace(president_results.cand_last_name_v1,",","")) #remove trailing comma

# COMMAND ----------

# MAGIC %md ##### Prep the House results data

# COMMAND ----------

"""
We create a column "is_winner" and mark the record that received the most votes in each election and then dedupe the data
Ultimately we will need to join this data to the campaign contributions data
These are disparate data sources and there is no unique ID to join them on so we will end up joining on
    Last name
    Party
    Election year
    Office
    State
    District
In order to join on these fields we must ensure each dataset is unique by these fields
In most cases this is not an issue but there are a few instances (1-2%) that have dupes as defined above. When this is the case I am removing these records altogether. I realize any final analysis will be absent these records but I given the small number of instances, it was a worthwhile tradeoff to enable the datasets to be joined on a 1:1 basis.   
"""

# house winners
house_winners = house_results.filter(house_results.stage=="gen").groupBy(house_results.state
                                      , house_results.district
                                      , house_results.year).agg(sf.max(house_results["candidatevotes"].cast(IntegerType()))).select(house_results.state
                                                                                                          , house_results.district
                                                                                                          , house_results.year
                                                                                                          , sf.col("max(CAST(candidatevotes AS INT))").alias("mx_votes"))

# left join to add is_winner column
house_results_left = house_results.filter(house_results.stage=="gen").alias("a").join(house_winners.alias("b"), 
                   (house_results.state==house_winners.state)
                   & (house_results.district==house_winners.district)
                   & (house_results.year==house_winners.year)
                   & (house_results.candidatevotes==house_winners.mx_votes), how="left").select("a.*",sf.when(sf.col("mx_votes").isNull(),0).otherwise(sf.lit("1")).alias("is_winner"))

# house result records with multiple candidates from same party and last name in race
df = house_results_left.select(house_results_left.year
                          , house_results_left.state_po
                          , house_results_left.office
                          , house_results_left.district
                          , house_results_left.party
                          , house_results_left.cand_last_name
                         ).filter((house_results_left.party=="democrat") | (house_results_left.party=="republican")).filter(house_results_left.year>="2008")

# identify dupes
df1 = df.groupBy(df.year, df.state_po, df.office, df.district, df.party, df.cand_last_name).count().select(df.year, df.state_po, df.office, df.district, df.party, df.cand_last_name, sf.col("count").alias("cnt"))

df_house_non_dupes = df1.filter(df1.cnt==1)

# remove dupes
house_results_deduped = house_results_left.alias("a").join(df_house_non_dupes.alias("b"), 
                                                      (house_results_left.year == df_house_non_dupes.year) &
                                                      (house_results_left.state_po == df_house_non_dupes.state_po) &
                                                      (house_results_left.office == df_house_non_dupes.office) &
                                                      (house_results_left.district == df_house_non_dupes.district) &
                                                      (house_results_left.party == df_house_non_dupes.party) &
                                                      (house_results_left.cand_last_name == df_house_non_dupes.cand_last_name)
                                                     ).select("a.*")

for col in house_results_deduped.columns:
    house_results_deduped = house_results_deduped.withColumn(col, sf.upper(sf.col(col)))

# COMMAND ----------

# MAGIC %md ##### Prep the Senate results data

# COMMAND ----------

"""
We create a column "is_winner" and mark the record that received the most votes in each election and then dedupe the data
Ultimately we will need to join this data to the campaign contributions data
These are disparate data sources and there is no unique ID to join them on so we will end up joining on
    Last name
    Party
    Election year
    Office
    State
In order to join on these fields we must ensure each dataset is unique by these fields
In most cases this is not an issue but there are a few instances (1-2%) that have dupes as defined above. When this is the case I am removing these records altogether. I realize any final analysis will be absent these records but I given the small number of instances, it was a worthwhile tradeoff to enable the datasets to be joined on a 1:1 basis.   
"""

# senate winners
senate_winners = senate_results.filter(senate_results.stage=="gen").groupBy(senate_results.state
                                      , senate_results.year).agg(sf.max(senate_results["candidatevotes"].cast(IntegerType()))).select(senate_results.state
                                                                                                          , senate_results.year
                                                                                                          , sf.col("max(CAST(candidatevotes AS INT))").alias("mx_votes"))

# left join to add is_winner column
senate_results_left = senate_results.filter(senate_results.stage=="gen").alias("a").join(senate_winners.alias("b"), 
                   (senate_results.state==senate_winners.state)
                   & (senate_results.year==senate_winners.year)
                   & (senate_results.candidatevotes==senate_winners.mx_votes), how="left").select("a.*",sf.when(sf.col("mx_votes").isNull(),0).otherwise(sf.lit("1")).alias("is_winner"))

# senate result records with multiple candidates from same party and last name in race
df = senate_results_left.select(senate_results_left.year
                          , senate_results_left.state_po
                          , senate_results_left.office
                          , senate_results_left.party
                          , senate_results_left.cand_last_name
                         ).filter((senate_results_left.party=="democrat") | (senate_results_left.party=="republican")).filter(senate_results_left.year>="2008")

# identify dupes
df1 = df.groupBy(df.year, df.state_po, df.office, df.party, df.cand_last_name).count().select(df.year, df.state_po, df.office, df.party, df.cand_last_name, sf.col("count").alias("cnt"))
df_senate_non_dupes = df1.filter(df1.cnt==1)

# remove dupes
senate_results_deduped = senate_results_left.alias("a").join(df_senate_non_dupes.alias("b"), 
                                                      (senate_results_left.year == df_senate_non_dupes.year) &
                                                      (senate_results_left.state_po == df_senate_non_dupes.state_po) &
                                                      (senate_results_left.office == df_senate_non_dupes.office) &
                                                      (senate_results_left.party == df_senate_non_dupes.party) &
                                                      (senate_results_left.cand_last_name == df_senate_non_dupes.cand_last_name)
                                                     ).select("a.*")

for col in senate_results_deduped.columns:
    senate_results_deduped = senate_results_deduped.withColumn(col, sf.upper(sf.col(col)))

# COMMAND ----------

# MAGIC %md ##### Prep the President results data

# COMMAND ----------

"""
We create a column "is_winner" and mark the record that received the most votes in each election and then dedupe the data
Ultimately we will need to join this data to the campaign contributions data
These are disparate data sources and there is no unique ID to join them on so we will end up joining on
    Last name
    First name
    Party
    Election year
    Office
In order to join on these fields we must ensure each dataset is unique by these fields
In most cases this is not an issue but there are a few instances (1-2%) that have dupes as defined above. When this is the case I am removing these records altogether. I realize any final analysis will be absent these records but I given the small number of instances, it was a worthwhile tradeoff to enable the datasets to be joined on a 1:1 basis.   
"""

# president votes aggregated
president_votes = president_results.filter(president_results.year>=2008).groupBy(president_results.year
                                            , president_results.cand_first_name
                                            , president_results.cand_last_name
                                            , president_results.cand_office
                                            , president_results.party
                                           , president_results.candidate).agg(sf.sum(president_results["candidatevotes"].cast(IntegerType()))).select(
                                                                           president_results.year
                                                                           , president_results.cand_first_name
                                                                           , president_results.cand_last_name
                                                                           , president_results.cand_office
                                                                           , president_results.candidate
                                                                           , president_results.party
                                                                           , sf.col("sum(CAST(candidatevotes AS INT))").alias("votes"))

# president winners
president_winners = president_votes.filter(president_votes.year>=2008).groupBy(
                                      president_votes.year).agg(sf.max(president_votes["votes"].cast(IntegerType()))).select(
                                                                                                          president_votes.year
                                                                                                          , sf.col("max(CAST(votes AS INT))").alias("mx_votes"))

#display(president_winners)

# left join to add is_winner column
president_results_left = president_votes.alias("a").join(president_winners.alias("b"), 
                   (president_votes.year==president_winners.year)
                   & (president_votes.votes==president_winners.mx_votes), how="left").select("a.*",sf.when(sf.col("mx_votes").isNull(),0).otherwise(sf.lit("1")).alias("is_winner_most"))

# account for Trump winning 2016 with less votes
president_results_left = president_results_left.alias("a").select("a.*", 
                            sf.when((((sf.col("is_winner_most")==1) | ((sf.col("year")=="2016") & (sf.col("cand_last_name")=="Trump") 
                            & (sf.col("party")=="republican"))) 
                            & (sf.col("cand_last_name") != "Clinton")),1).otherwise(sf.lit("0")).alias("is_winner"))

president_results_left = president_results_left.withColumnRenamed("year","year").withColumnRenamed("cand_first_name","cand_first_name").withColumnRenamed("cand_last_name","cand_last_name").withColumnRenamed("cand_office","cand_office").withColumnRenamed("candidate","candidate").withColumnRenamed("party","party").withColumnRenamed("votes","votes").withColumnRenamed("is_winner","is_winner")

# president result records with multiple candidates from same party and last name in race
df = president_results_left.select(president_results_left.year
                          , president_results_left.cand_office
                          , president_results_left.party
                          , president_results_left.cand_last_name
                         ).filter((president_results_left.party=="democrat") | (president_results_left.party=="republican")).filter(president_results_left.year>="2008")

# identify dupes
df1 = df.groupBy(df.year, df.cand_office, df.party, df.cand_last_name).count().select(df.year, df.cand_office, df.party, df.cand_last_name, sf.col("count").alias("cnt"))
df_president_non_dupes = df1.filter(df1.cnt==1)

# remove dupes
president_results_deduped = president_results_left.alias("a").join(df_president_non_dupes.alias("b"), 
                                                      (president_results_left.year == df_president_non_dupes.year) &
                                                      (president_results_left.cand_office == df_president_non_dupes.cand_office) &
                                                      (president_results_left.party == df_president_non_dupes.party) &
                                                      (president_results_left.cand_last_name == df_president_non_dupes.cand_last_name)
                                                     ).select("a.*")

for col in president_results_deduped.columns:
    president_results_deduped = president_results_deduped.withColumn(col, sf.upper(sf.col(col)))

# COMMAND ----------

# MAGIC %md ##### Dedupe candidate master data

# COMMAND ----------

"""
The candidate master file contains one record for each candidate who has either registered with the Federal Election Commission or appeared on a ballot list prepared by a state elections office. A candidate has a unique ID for each office they are running for. For example, Hillary Clinton has a single CAND_ID for her senate runs and a different CAND_ID for her presidential runs. The CAND_ID is the same across years though as long as it is for the same office. Data is not unique by CAND_ID and CAND_OFFICE though. The file can contain duplicates of CAND_ID and CAND_OFFICE with different CAN_NAMEs. For example, Hillary Clinton can appear as Hillary Clinton, Hillary Rodham Clinton, and Hillary Rodham Clinton / Timothy Michael Kaine. Here I am deduping the cnd_data dataframe so it is unique by:

CAND_PTY_AFFILIATION
CAND_OFFICE_ST
CAND_OFFICE
CAND_OFFICE_DISTRICT
FILE_YR
last_name
"""


# CAND_ID = P60018835 is "TRUMP, THE MUSLIM DICTATOR" which then flags "TRUMP, DONALD J. / MICHAEL R. PENCE"
#          as a duplicate because they both have the same last name / party / etc
#          I am removing this one because it is of importance / I know who the presidetial candidates are
# In all, I am removing 212 dupes out of ~35K records
cnd_data1 = cnd_data.filter(cnd_data.CAND_ID != "P60018835")

# candidate data with multiple records
df_cnd_check = cnd_data1.groupBy(cnd_data1.CAND_PTY_AFFILIATION
                                , cnd_data1.CAND_ELECTION_YR
                                , cnd_data1.CAND_OFFICE_ST
                                , cnd_data1.CAND_OFFICE
                                , cnd_data1.CAND_OFFICE_DISTRICT
                                , cnd_data1.FILE_YR
                                , cnd_data1.last_name
                               ).agg(sf.countDistinct(cnd_data1.CAND_ID)).select(cnd_data1.CAND_PTY_AFFILIATION
                                                , cnd_data1.CAND_ELECTION_YR
                                                , cnd_data1.CAND_OFFICE_ST
                                                , cnd_data1.CAND_OFFICE
                                                , cnd_data1.CAND_OFFICE_DISTRICT
                                                , cnd_data1.FILE_YR
                                                , cnd_data1.last_name
                                                , sf.col("count(DISTINCT CAND_ID)").alias("cnt"))

df_dupes = df_cnd_check.filter(df_cnd_check.cnt>1)
df_non_dupes = df_cnd_check.filter(df_cnd_check.cnt==1)

#deduped list of candidates
cnd_data_deduped = cnd_data1.alias("a").join(df_non_dupes.alias("b"),
                                            (cnd_data1.CAND_PTY_AFFILIATION==df_non_dupes.CAND_PTY_AFFILIATION) &
                                            (cnd_data1.CAND_ELECTION_YR==df_non_dupes.CAND_ELECTION_YR) &
                                            (cnd_data1.CAND_OFFICE_ST==df_non_dupes.CAND_OFFICE_ST) &
                                            (cnd_data1.CAND_OFFICE==df_non_dupes.CAND_OFFICE) &
                                            (cnd_data1.CAND_OFFICE_DISTRICT==df_non_dupes.CAND_OFFICE_DISTRICT) &
                                            (cnd_data1.FILE_YR==df_non_dupes.FILE_YR) &
                                            (cnd_data1.last_name==df_non_dupes.last_name)).select("a.*").distinct()
                                            
cnd_data_deduped = cnd_data_deduped.withColumn("CAND_DISTRICT", cnd_data_deduped["CAND_OFFICE_DISTRICT"].cast(IntegerType()))

cnd_data_deduped = cnd_data_deduped.withColumn("PARTY", sf.when(cnd_data_deduped["CAND_PTY_AFFILIATION"] =="DEM","DEMOCRAT").when(cnd_data_deduped["CAND_PTY_AFFILIATION"]=="REP","REPUBLICAN").otherwise("NA"))

# candidate dedupe based on id and election yr
# same id / election yr can show up in multiple files so we are finding the max file it appears in and using the data from that
df = cnd_data_deduped.groupBy(cnd_data_deduped.CAND_ID, cnd_data_deduped.CAND_ELECTION_YR).agg(sf.max("FILE_YR")).select(cnd_data_deduped.CAND_ID, cnd_data_deduped.CAND_ELECTION_YR, sf.col("max(FILE_YR)").alias("FILE_YR"))

cnd_data_deduped_by_yr = cnd_data_deduped.alias("a").join(df.alias("b"), (cnd_data_deduped.CAND_ID==df.CAND_ID) &
                                                         (cnd_data_deduped.CAND_ELECTION_YR==df.CAND_ELECTION_YR) &
                                                         (cnd_data_deduped.FILE_YR==df.FILE_YR)).select("a.*")

# COMMAND ----------

# MAGIC %md ##### Aggregate contribution data by CMTE_ID and the file it is from

# COMMAND ----------

# Aggregate the data at this level so we can join it back to the candidate
fin_data_agg = fin_data_parq.groupBy(fin_data_parq.CMTE_ID, fin_data_parq.FILE_YR).agg(sf.sum("TRANSACTION_AMT")).select(fin_data_parq.CMTE_ID
                                                                                                         , fin_data_parq.FILE_YR
                                                                                                         , sf.col("sum(TRANSACTION_AMT)").alias("AMT"))

# COMMAND ----------

# MAGIC %md ##### Save the data as parquet

# COMMAND ----------

#fin_data_agg.write.parquet("dbfs:/autumn_2019/jaomorro/parq_files/fin_data_agg.parquet")  #run this the first time in order to save as parquet
fin_data_agg_parq = spark.read.parquet("dbfs:/autumn_2019/jaomorro/parq_files/fin_data_agg.parquet")

# COMMAND ----------

# MAGIC %md ##### Join the candidate data to the campaign linking data

# COMMAND ----------

"""
This will join the deduped candidate dataframe to the campaign linking dataframe so we tie the contributions to the candidate the contribution was for
"""

ccl_cnd_data_new = ccl_data.alias("a").join(cnd_data_deduped_by_yr.alias("b"), 
                                             (ccl_data.CAND_ID == cnd_data_deduped.CAND_ID) &
                                             #(ccl_data.FILE_YR == cnd_data_deduped.FILE_YR) &
                                             (ccl_data.CAND_ELECTION_YR == cnd_data_deduped.CAND_ELECTION_YR)).select("b.CAND_ID"
                                                                                                               , "b.CAND_NAME"
                                                                                                               , "b.CAND_PTY_AFFILIATION"
                                                                                                               , "b.CAND_ELECTION_YR"
                                                                                                               , "b.CAND_OFFICE_ST"
                                                                                                               , "b.CAND_OFFICE"
                                                                                                               , "b.CAND_OFFICE_DISTRICT"
                                                                                                               , "b.CAND_ICI"
                                                                                                               , "b.CAND_STATUS"
                                                                                                               , "b.CAND_PCC"
                                                                                                               , "b.CAND_ST1"
                                                                                                               , "b.CAND_ST2"
                                                                                                               , "b.CAND_CITY"
                                                                                                               , "b.CAND_ST"
                                                                                                               , "b.CAND_ZIP"
                                                                                                               , "b.first_name"
                                                                                                               , "b.last_name"
                                                                                                               , "b.full_name"
                                                                                                               , "b.CAND_DISTRICT"
                                                                                                               , "b.PARTY"
                                                                                                               , "a.FILE_YR"
                                                                                                               , "a.CMTE_ID"
                                                                                                               , "a.FEC_ELECTION_YR"
                                                                                                               , "a.LINKAGE_ID")

# COMMAND ----------

# MAGIC %md ##### Save the data as parquet

# COMMAND ----------

#ccl_cnd_data_new.write.parquet("dbfs:/autumn_2019/jaomorro/parq_files/ccl_cnd_data.parquet")
ccl_cnd_data_parq = spark.read.parquet("dbfs:/autumn_2019/jaomorro/parq_files/ccl_cnd_data.parquet")

# COMMAND ----------

# MAGIC %md ##### Join campaign contributions to the candidates

# COMMAND ----------

ccl_fin_agg = ccl_cnd_data_parq.alias("a").join(fin_data_agg_parq.alias("b"), 
                                           (ccl_cnd_data_parq.CMTE_ID==fin_data_agg_parq.CMTE_ID) &
                                           (ccl_cnd_data_parq.FILE_YR==fin_data_agg_parq.FILE_YR)).select("a.*", "b.AMT")

# COMMAND ----------

# MAGIC %md ##### Save the data as parquet

# COMMAND ----------

#ccl_fin_agg.write.parquet("dbfs:/autumn_2019/jaomorro/parq_files/ccl_fin_agg.parquet")
ccl_fin_agg_parq = spark.read.parquet("dbfs:/autumn_2019/jaomorro/parq_files/ccl_fin_agg.parquet")

# COMMAND ----------

# MAGIC %md ##### Aggregate contributions by candidate and election

# COMMAND ----------

fin_agg_election = ccl_fin_agg_parq.groupBy(ccl_fin_agg_parq.CAND_ID
                                       , ccl_fin_agg_parq.CAND_ELECTION_YR).agg(sf.sum("AMT")).select(ccl_fin_agg_parq.CAND_ID
                                                                                                 ,ccl_fin_agg_parq.CAND_ELECTION_YR
                                                                                                 , sf.col("sum(AMT)").alias("AMT"))

# COMMAND ----------

# MAGIC %md ##### Save the data as parquet

# COMMAND ----------

#fin_agg_election.write.parquet("dbfs:/autumn_2019/jaomorro/parq_files/fin_agg_election.parquet")
fin_agg_election_parq = spark.read.parquet("dbfs:/autumn_2019/jaomorro/parq_files/fin_agg_election.parquet")

# COMMAND ----------

# MAGIC %md ##### Join in all the info about candidate to aggregated data

# COMMAND ----------

"""
The parquet created above only has the CAND_ID so this will join to the candidate deduped dataframe to bring in all the additional metadata about the candidates
"""

fin_agg_and_cand = cnd_data_deduped_by_yr.alias("a").join(fin_agg_election_parq.alias("b"), (cnd_data_deduped_by_yr.CAND_ID==fin_agg_election_parq.CAND_ID) & (cnd_data_deduped_by_yr.CAND_ELECTION_YR==fin_agg_election_parq.CAND_ELECTION_YR)).select("a.*","b.AMT")

# COMMAND ----------

# MAGIC %md ##### Flag record that received the most contribution dollars

# COMMAND ----------

"""
Flag the record in each race that received the most contributions. In order to accomplish this we create a dataframe with the max amount contributed aggregated at: 
    CAND_OFFICE_ST
    CAND_OFFICE_DISTRICT
    CAND_ELECTION_YR
    CAND_OFFICE
And then join it back to itself and flag the record where the max amount from aggregated table is equal to the contribution dollars received
"""

fin_agg_and_cand_mx = fin_agg_and_cand.groupBy(fin_agg_and_cand.CAND_OFFICE_ST
                        , fin_agg_and_cand.CAND_OFFICE_DISTRICT
                        , fin_agg_and_cand.CAND_ELECTION_YR
                        , fin_agg_and_cand.CAND_OFFICE).agg(sf.max(fin_agg_and_cand["AMT"].cast(IntegerType()))).select(fin_agg_and_cand.CAND_OFFICE_ST
                                                                                      , fin_agg_and_cand.CAND_OFFICE_DISTRICT
                                                                                      , fin_agg_and_cand.CAND_ELECTION_YR
                                                                                      , fin_agg_and_cand.CAND_OFFICE
                                                                                      , sf.col("max(CAST(AMT AS INT))").alias("AMT"))

# add flag indicating if record had max amount
fin_agg_and_cand_with_mx = fin_agg_and_cand.alias("a").join(fin_agg_and_cand_mx.alias("b"),
                                (fin_agg_and_cand.CAND_OFFICE_ST==fin_agg_and_cand_mx.CAND_OFFICE_ST)
                                & (fin_agg_and_cand.CAND_OFFICE_DISTRICT==fin_agg_and_cand_mx.CAND_OFFICE_DISTRICT)
                                & (fin_agg_and_cand.CAND_ELECTION_YR==fin_agg_and_cand_mx.CAND_ELECTION_YR)
                                & (fin_agg_and_cand.AMT==fin_agg_and_cand_mx.AMT)
                                & (fin_agg_and_cand.CAND_OFFICE==fin_agg_and_cand_mx.CAND_OFFICE), how="left").select("a.*",
                                                                        sf.when(sf.col("b.AMT").isNull(),0).otherwise(sf.lit("1")).alias("is_max"))

# rename the columns otherwise we receive an error later on (some wierd pyspark error)
fin_agg_and_cand_renamed = fin_agg_and_cand_with_mx.withColumnRenamed('CAND_ID', 'cand_id').withColumnRenamed('CAND_NAME', 'cand_name').withColumnRenamed('CAND_PTY_AFFILIATION', 'cand_pty_affiliation').withColumnRenamed('CAND_ELECTION_YR', 'cand_election_yr').withColumnRenamed('CAND_OFFICE_ST', 'cand_office_st').withColumnRenamed('CAND_OFFICE', 'cand_office').withColumnRenamed('CAND_OFFICE_DISTRICT', 'cand_office_district').withColumnRenamed('CAND_ICI', 'cand_ici').withColumnRenamed('CAND_STATUS', 'cand_status').withColumnRenamed('CAND_PCC', 'cand_pcc').withColumnRenamed('CAND_ST1', 'cand_st1').withColumnRenamed('CAND_ST2', 'cand_st2').withColumnRenamed('CAND_CITY', 'cand_city').withColumnRenamed('CAND_ST', 'cand_st').withColumnRenamed('CAND_ZIP', 'cand_zip').withColumnRenamed('FILE_YR', 'file_yr').withColumnRenamed('first_name', 'cand_first_name').withColumnRenamed('last_name', 'cand_last_name').withColumnRenamed('full_name', 'cand_full_name').withColumnRenamed('CAND_DISTRICT', 'cand_district').withColumnRenamed('PARTY', 'party').withColumnRenamed('AMT', 'amt').withColumnRenamed('is_max', 'is_max')

# COMMAND ----------

# MAGIC %md ##### Join candidate data to house results

# COMMAND ----------

"""
This brings together house results and campaign contributions into one dataframe
"""

# join candidate data to house results
house_cnd_with_results = cnd_data_deduped_by_yr.alias("a").filter(cnd_data_deduped_by_yr.CAND_OFFICE=="H").join(house_results_deduped.alias("b"), 
                                            (cnd_data_deduped_by_yr.PARTY==house_results_deduped.party) &         
                                            (cnd_data_deduped_by_yr.CAND_ELECTION_YR==house_results_deduped.year) &
                                            (cnd_data_deduped_by_yr.CAND_OFFICE_ST==house_results_deduped.state_po) &
                                            (cnd_data_deduped_by_yr.CAND_OFFICE==house_results_deduped.cand_office) &
                                            (cnd_data_deduped_by_yr.CAND_DISTRICT==house_results_deduped.district) &
                                            (cnd_data_deduped_by_yr.last_name==house_results_deduped.cand_last_name)).select("a.*"
                                                                             , house_results_deduped.candidate
                                                                              , house_results_deduped.candidatevotes
                                                                              , house_results_deduped.totalvotes
                                                                              , house_results_deduped.district
                                                                              , house_results_deduped.year
                                                                              , house_results_deduped.year
                                                                              , house_results_deduped.special
                                                                              , house_results_deduped.office
                                                                              , house_results_deduped.stage
                                                                              , house_results_deduped.is_winner)

# rename columns
house_cnd_with_results_renamed = house_cnd_with_results.withColumnRenamed('CAND_ID', 'cand_id').withColumnRenamed('CAND_NAME', 'cand_name').withColumnRenamed('CAND_PTY_AFFILIATION', 'cand_pty_affiliation').withColumnRenamed('CAND_ELECTION_YR', 'cand_election_yr').withColumnRenamed('CAND_OFFICE_ST', 'cand_office_st').withColumnRenamed('CAND_OFFICE', 'cand_office').withColumnRenamed('CAND_OFFICE_DISTRICT', 'cand_office_district').withColumnRenamed('CAND_ICI', 'cand_ici').withColumnRenamed('CAND_STATUS', 'cand_status').withColumnRenamed('CAND_PCC', 'cand_pcc').withColumnRenamed('CAND_ST1', 'cand_st1').withColumnRenamed('CAND_ST2', 'cand_st2').withColumnRenamed('CAND_CITY', 'cand_city').withColumnRenamed('CAND_ST', 'cand_st').withColumnRenamed('CAND_ZIP', 'cand_zip').withColumnRenamed('FILE_YR', 'file_yr').withColumnRenamed('first_name', 'cand_first_name').withColumnRenamed('last_name', 'cand_last_name').withColumnRenamed('CAND_DISTRICT', 'cand_district').withColumnRenamed('PARTY', 'party').withColumnRenamed('candidate', 'candidate').withColumnRenamed('candidatevotes', 'candidatevotes').withColumnRenamed('totalvotes', 'totalvotes').withColumnRenamed('district', 'district').withColumnRenamed('year', 'year').withColumnRenamed('year', 'year').withColumnRenamed('special', 'special').withColumnRenamed('office', 'office').withColumnRenamed('stage', 'stage').withColumnRenamed('is_winner', 'is_winner')

# join votes and fin data
house_fin_and_votes = fin_agg_and_cand_renamed.alias("a").filter(fin_agg_and_cand_renamed.cand_office=="H").join(house_cnd_with_results_renamed.alias("b"), 
                                 (fin_agg_and_cand_renamed.cand_election_yr == house_cnd_with_results_renamed.cand_election_yr)
                                  & (fin_agg_and_cand_renamed.cand_id == house_cnd_with_results_renamed.cand_id),
                                                              how="left").select(fin_agg_and_cand_renamed.cand_id
                                                                                                  , house_cnd_with_results_renamed.candidate
                                                                                                  , fin_agg_and_cand_renamed.cand_first_name
                                                                                                  , fin_agg_and_cand_renamed.cand_last_name
                                                                                                  , fin_agg_and_cand_renamed.cand_full_name
                                                                                                  , fin_agg_and_cand_renamed.party
                                                                                                  , fin_agg_and_cand_renamed.cand_election_yr
                                                                                                  , fin_agg_and_cand_renamed.cand_office_st
                                                                                                  , fin_agg_and_cand_renamed.cand_office
                                                                                                  , fin_agg_and_cand_renamed.cand_district
                                                                                                  , fin_agg_and_cand_renamed.cand_ici
                                                                                                  , fin_agg_and_cand_renamed.cand_status
                                                                                                  , fin_agg_and_cand_renamed.cand_city
                                                                                                  , fin_agg_and_cand_renamed.cand_zip
                                                                                                  , fin_agg_and_cand_renamed.amt
                                                                                                  , house_cnd_with_results_renamed.candidatevotes
                                                                                                  , house_cnd_with_results_renamed.totalvotes
                                                                                                  , house_cnd_with_results_renamed.is_winner
                                                                                                  , fin_agg_and_cand_renamed.is_max)

# # matrix of winners and if they received mx amount
# display(house_fin_and_votes.groupBy(house_fin_and_votes.is_winner
#                                     , house_fin_and_votes.is_max).count())

# COMMAND ----------

# MAGIC %md ##### Join candidate data to senate results

# COMMAND ----------

"""
This brings together senate results and campaign contributions into one dataframe
"""

# join candidate data to senate results
senate_cnd_with_results = cnd_data_deduped_by_yr.alias("a").filter(cnd_data_deduped_by_yr.CAND_OFFICE=="S").join(senate_results_deduped.alias("b"), 
                                            (cnd_data_deduped_by_yr.PARTY==senate_results_deduped.party) &         
                                            (cnd_data_deduped_by_yr.CAND_ELECTION_YR==senate_results_deduped.year) &
                                            (cnd_data_deduped_by_yr.CAND_OFFICE_ST==senate_results_deduped.state_po) &
                                            (cnd_data_deduped_by_yr.CAND_OFFICE==senate_results_deduped.cand_office) &
                                            (cnd_data_deduped_by_yr.last_name==senate_results_deduped.cand_last_name)).select("a.*"
                                                                             , senate_results_deduped.candidate
                                                                              , senate_results_deduped.candidatevotes
                                                                              , senate_results_deduped.totalvotes
                                                                              , senate_results_deduped.year
                                                                              , senate_results_deduped.special
                                                                              , senate_results_deduped.office
                                                                              , senate_results_deduped.stage
                                                                              , senate_results_deduped.is_winner)

# rename columns
senate_cnd_with_results_renamed = senate_cnd_with_results.withColumnRenamed('CAND_ID', 'cand_id').withColumnRenamed('CAND_NAME', 'cand_name').withColumnRenamed('CAND_PTY_AFFILIATION', 'cand_pty_affiliation').withColumnRenamed('CAND_ELECTION_YR', 'cand_election_yr').withColumnRenamed('CAND_OFFICE_ST', 'cand_office_st').withColumnRenamed('CAND_OFFICE', 'cand_office').withColumnRenamed('CAND_OFFICE_DISTRICT', 'cand_office_district').withColumnRenamed('CAND_ICI', 'cand_ici').withColumnRenamed('CAND_STATUS', 'cand_status').withColumnRenamed('CAND_PCC', 'cand_pcc').withColumnRenamed('CAND_ST1', 'cand_st1').withColumnRenamed('CAND_ST2', 'cand_st2').withColumnRenamed('CAND_CITY', 'cand_city').withColumnRenamed('CAND_ST', 'cand_st').withColumnRenamed('CAND_ZIP', 'cand_zip').withColumnRenamed('FILE_YR', 'file_yr').withColumnRenamed('first_name', 'cand_first_name').withColumnRenamed('last_name', 'cand_last_name').withColumnRenamed('CAND_DISTRICT', 'cand_district').withColumnRenamed('PARTY', 'party').withColumnRenamed('candidate', 'candidate').withColumnRenamed('candidatevotes', 'candidatevotes').withColumnRenamed('totalvotes', 'totalvotes').withColumnRenamed('year', 'year').withColumnRenamed('year', 'year').withColumnRenamed('special', 'special').withColumnRenamed('office', 'office').withColumnRenamed('stage', 'stage').withColumnRenamed('is_winner', 'is_winner')

# join votes and fin data
senate_fin_and_votes = fin_agg_and_cand_renamed.alias("a").filter(fin_agg_and_cand_renamed.cand_office=="S").join(senate_cnd_with_results_renamed.alias("b"), 
                                 (fin_agg_and_cand_renamed.cand_election_yr == senate_cnd_with_results_renamed.cand_election_yr)
                                  & (fin_agg_and_cand_renamed.cand_id == senate_cnd_with_results_renamed.cand_id),
                                                              how="left").select(fin_agg_and_cand_renamed.cand_id
                                                                                                  , senate_cnd_with_results_renamed.candidate
                                                                                                  , fin_agg_and_cand_renamed.cand_first_name
                                                                                                  , fin_agg_and_cand_renamed.cand_last_name
                                                                                                  , fin_agg_and_cand_renamed.cand_full_name
                                                                                                  , fin_agg_and_cand_renamed.party
                                                                                                  , fin_agg_and_cand_renamed.cand_election_yr
                                                                                                  , fin_agg_and_cand_renamed.cand_office_st
                                                                                                  , fin_agg_and_cand_renamed.cand_office
                                                                                                  , fin_agg_and_cand_renamed.cand_district
                                                                                                  , fin_agg_and_cand_renamed.cand_ici
                                                                                                  , fin_agg_and_cand_renamed.cand_status
                                                                                                  , fin_agg_and_cand_renamed.cand_city
                                                                                                  , fin_agg_and_cand_renamed.cand_zip
                                                                                                  , fin_agg_and_cand_renamed.amt
                                                                                                  , senate_cnd_with_results_renamed.candidatevotes
                                                                                                  , senate_cnd_with_results_renamed.totalvotes
                                                                                                  , senate_cnd_with_results_renamed.is_winner
                                                                                                  , fin_agg_and_cand_renamed.is_max)

# # matrix of winners and if they received mx amount
# display(senate_fin_and_votes.groupBy(senate_fin_and_votes.is_winner
#                                     , senate_fin_and_votes.is_max).count())

# COMMAND ----------

# MAGIC %md ##### Join candidate data to president results

# COMMAND ----------

"""
This brings together president results and campaign contributions into one dataframe
"""

# join candidate data to president results
president_cnd_with_results = cnd_data_deduped_by_yr.alias("a").filter(cnd_data_deduped_by_yr.CAND_OFFICE=="P").join(president_results_deduped.alias("b"),        
                                            (cnd_data_deduped_by_yr.CAND_ELECTION_YR==president_results_deduped.year) &
                                            (cnd_data_deduped_by_yr.last_name==president_results_deduped.cand_last_name)).select("a.*"
                                                                             , president_results_deduped.candidate
                                                                              , president_results_deduped.votes
                                                                              , president_results_deduped.year
                                                                              , president_results_deduped.cand_office
                                                                              , president_results_deduped.is_winner)


# rename columns
president_cnd_with_results_renamed = president_cnd_with_results.withColumnRenamed('CAND_ID', 'cand_id').withColumnRenamed('CAND_NAME', 'cand_name').withColumnRenamed('CAND_PTY_AFFILIATION', 'cand_pty_affiliation').withColumnRenamed('CAND_ELECTION_YR', 'cand_election_yr').withColumnRenamed('CAND_OFFICE_ST', 'cand_office_st').withColumnRenamed('CAND_OFFICE', 'cand_office').withColumnRenamed('CAND_OFFICE_DISTRICT', 'cand_office_district').withColumnRenamed('CAND_ICI', 'cand_ici').withColumnRenamed('CAND_STATUS', 'cand_status').withColumnRenamed('CAND_PCC', 'cand_pcc').withColumnRenamed('CAND_ST1', 'cand_st1').withColumnRenamed('CAND_ST2', 'cand_st2').withColumnRenamed('CAND_CITY', 'cand_city').withColumnRenamed('CAND_ST', 'cand_st').withColumnRenamed('CAND_ZIP', 'cand_zip').withColumnRenamed('FILE_YR', 'file_yr').withColumnRenamed('first_name', 'cand_first_name').withColumnRenamed('last_name', 'cand_last_name').withColumnRenamed('CAND_DISTRICT', 'cand_district').withColumnRenamed('PARTY', 'party').withColumnRenamed('candidate', 'candidate').withColumnRenamed('votes', 'votes').withColumnRenamed('year', 'year').withColumnRenamed('year', 'year').withColumnRenamed('cand_office', 'cand_office').withColumnRenamed('is_winner', 'is_winner')

# join votes and fin data
president_fin_and_votes = fin_agg_and_cand_renamed.alias("a").filter(fin_agg_and_cand_renamed.cand_office=="P").join(president_cnd_with_results_renamed.alias("b"), 
                                 (fin_agg_and_cand_renamed.cand_election_yr == president_cnd_with_results_renamed.cand_election_yr)
                                  & (fin_agg_and_cand_renamed.cand_id == president_cnd_with_results_renamed.cand_id),
                                                              how="left").select(fin_agg_and_cand_renamed.cand_id
                                                                                                  , president_cnd_with_results_renamed.candidate
                                                                                                  , fin_agg_and_cand_renamed.cand_first_name
                                                                                                  , fin_agg_and_cand_renamed.cand_last_name
                                                                                                  , fin_agg_and_cand_renamed.cand_full_name
                                                                                                  , fin_agg_and_cand_renamed.party
                                                                                                  , fin_agg_and_cand_renamed.cand_election_yr
                                                                                                  , fin_agg_and_cand_renamed.cand_office_st
                                                                                                  , fin_agg_and_cand_renamed.cand_office
                                                                                                  , fin_agg_and_cand_renamed.cand_district
                                                                                                  , fin_agg_and_cand_renamed.cand_ici
                                                                                                  , fin_agg_and_cand_renamed.cand_status
                                                                                                  , fin_agg_and_cand_renamed.cand_city
                                                                                                  , fin_agg_and_cand_renamed.cand_zip
                                                                                                  , fin_agg_and_cand_renamed.amt
                                                                                                  , president_cnd_with_results_renamed.votes
                                                                                                  , president_cnd_with_results_renamed.is_winner
                                                                                                  , fin_agg_and_cand_renamed.is_max)

# # matrix of winners and if they received mx amount
# display(president_fin_and_votes.groupBy(president_fin_and_votes.is_winner
#                                     , president_fin_and_votes.is_max).count())

# COMMAND ----------

# MAGIC %md ##### Output to csv files

# COMMAND ----------

house_fin_and_votes.coalesce(1).write.option("header", "true").csv("dbfs:/autumn_2019/jaomorro/csv_output/house_fin_and_votes2.csv")
senate_fin_and_votes.coalesce(1).write.option("header", "true").csv("dbfs:/autumn_2019/jaomorro/csv_output/senate_fin_and_votes2.csv")
president_fin_and_votes.coalesce(1).write.option("header", "true").csv("dbfs:/autumn_2019/jaomorro/csv_output/president_fin_and_votes2.csv")

# COMMAND ----------


