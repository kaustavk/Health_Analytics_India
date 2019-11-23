> # Introduction
>
> The dataset comprises a survey conducted in Empowered Action Group (EAG) states Uttarakhand, Rajasthan, Uttar Pradesh, Bihar, Jharkhand, Odisha, Chhattisgarh & Madhya Pradesh and Assam. These nine states, which account for about 48 percentage of the total population, 59 percentage of Births, 70 percentage of Infant Deaths, 75 percentage of Under 5 Deaths and 62 percentage of Maternal Deaths in the country, are the high focus States in view of their relatively higher fertility and mortality.
>
>  A representative sample of about 21 million population and 4.32 million households were covered which is spread across the rural and urban area of these 9 states.
>
>  The objective of the AHS is to yield a comprehensive, representative and reliable dataset on core vital indicators including composite ones like Infant Mortality Rate, Maternal Mortality Ratio and Total Fertility Rate along with their covariates (process and outcome indicators) at the district level and map the changes therein on an annual basis. These benchmarks would help in better and holistic understanding and timely monitoring of various determinants on well-being and health of population particularly Reproductive and Child Health. [[Source\]](http://pib.nic.in/newsite/mbErel.aspx?relid=85350)



## Reference

 [Kaggle](https://www.kaggle.com/rajanand/key-indicators-of-annual-health-survey) 

# Project Objective

- [ ] Ingest data from AWS RDS to HDFS through Sqoop.
- [ ] Create a Hive external table and load data from HDFS.
- [ ] Create two subset schema in Hive with different storage format as default format and ORC format.
- [ ]  Compare the runtimes of the queries  in these two tables and benchmark the performance.
- [ ] Create  Hive-Hbase integrated table and benchmark the performance as well.
- [ ] Create final sunset schema table (partition table) in above with the chosen storage format as analysed above.
- [ ] Perform below analysis on the partition table
  1.  The child mortality rate of Uttar Pradesh 
  2.  The fertility rate of Bihar 
  3.  State wise child mortality rate and state wise fertility rate and does high fertility correlate with high child mortality? 
  4.  Find top 2 districts per state with the highest population per household 
  5.  Find top 2 districts per state with the lowest sex ratios 



# Sqoop Import

```bash
sqoop import --connect <DB Connection> --username <username> --password <password> --table <RDS table name> --null-string 'NA' --null-non-string '\\N' --warehouse-dir <Directory Name>
```

![sqoop_1](/images/sqoop_1.jpg)

## Verify Import through CLI

![Sqoop_2](/images/Sqoop_2.jpg)

## Verify Import through Hue

![Sqoop_3](/images/Sqoop_3.jpg)

# Create External Table and Load data

## Create Table

Create table command is available [Here](https://github.com/kaustavk/Health_Analytics_India/blob/master/Queries/Create_Table.txt)

## Load Data

Load the data into the Hive table you just created in above step.

```bash
LOAD DATA INPATH '/user/ec2-user/key_indicator/Key_indicator_districtwise' OVERWRITE INTO TABLE Key_Indicator_ext_Full;
```

# Verify Ingestion

Run below queries in MySQL WorkBench and Hue respectively and ensure output should be same.

- **Count total number of rows** 

  ```bash
  select count(*) from Key_Indicator_ext_Full;
  ```

  MySQL Workbench Output:

  ![VerifyIngestion_1](/images/VerifyIngestion_1.jpg)

  Hue Output:

  ![VerifyIngestion_2](/images/VerifyIngestion_2.jpg)

- **Select the top 10 rows and first 8 columns**

  Query in MySQL Workbench:

  ```bash
  SELECT State_Name,State_District_Name,AA_Sample_Units_Total,AA_Sample_Units_Rural,
  AA_Sample_Units_Urban,AA_Households_Total,AA_Households_Rural,AA_Households_Urban
  FROM Key_indicator_districtwise LIMIT 10;
  ```

  MySQL Workbench Output:

  ![VerifyIngestion_3](/images/VerifyIngestion_3.jpg)

  Query in Hue:

  ```bash
  SELECT State_Name,State_District_Name,AA_Sample_Units_Total,AA_Sample_Units_Rural,
  AA_Sample_Units_Urban,AA_Households_Total,AA_Households_Rural,AA_Households_Urban
  FROM Key_Indicator_ext_Full LIMIT 10;

  ```

   Hue Output:

   ![VerifyIngestion_4](/images/VerifyIngestion_4.jpg)



# **Subset schema creation in Hive**

##     **Columns used in the subset schema**

- id
- state_name
- state_district_name
- YY_Under_Five_Mortality_Rate_U5MR_Total_Person
- LL_Total_Fertility_Rate_Total
- AA_Households_Total, AA_Population_Total
- CC_Sex_Ratio_All_Ages_Total

##      **Storage format used**

- Default Format
- ORC

##      **Create and insert command with default format**

###           Create Command:       

```bash
CREATE EXTERNAL TABLE IF NOT EXISTS Key_Indicator_ext_default(
           ID int,
           State_Name string,
           State_District_Name	 string,
		   YY_Under_Five_Mortality_Rate_U5MR_Total_Person double,
           LL_Total_Fertility_Rate_Total double,
           AA_Households_Total double,
           AA_Population_Total double,
           CC_Sex_Ratio_All_Ages_Total double
          )
           LOCATION '/user/ec2-user/key_indicator/key_indicator_default';
```

###            Insert Command:

```bash
INSERT INTO Key_Indicator_ext_default
SELECT ID,State_Name,State_District_Name,
       YY_Under_Five_Mortality_Rate_U5MR_Total_Person,
       LL_Total_Fertility_Rate_Total,
       AA_Households_Total,
       AA_Population_Total,
       CC_Sex_Ratio_All_Ages_Total
FROM Key_Indicator_ext_full

```



##   Create and insert command with ORC format

###   Create Command:

```
CREATE EXTERNAL TABLE IF NOT EXISTS Key_Indicator_ext_orc(
ID int,
State_Name string,
State_District_Name	 string,
YY_Under_Five_Mortality_Rate_U5MR_Total_Person double,
LL_Total_Fertility_Rate_Total double,
AA_Households_Total double,
AA_Population_Total double,
CC_Sex_Ratio_All_Ages_Total double
)
STORED AS ORC
LOCATION '/user/ec2-user/key_indicator/key_indicator_orc'
tblproperties("orc.compress"="SNAPPY")

```

###    Insert Command:

```
INSERT INTO Key_Indicator_ext_orc
SELECT ID,State_Name,State_District_Name,
       YY_Under_Five_Mortality_Rate_U5MR_Total_Person,
       LL_Total_Fertility_Rate_Total,
       AA_Households_Total,
       AA_Population_Total,
       CC_Sex_Ratio_All_Ages_Total
FROM Key_Indicator_ext_full;

```



##  Create and insert command for the Hive-HBase  integrated table

###   Create Command:

```
create table Key_Indicator_ext_hive(
`ID` int, 
`State_Name` string,
`State_District_Name` string,
`Mortality_Rate` double,
`Fertility_Rate` double,
`Households` double,
`Population` double,
`Sex_Ratio` double
 )
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,district:State_Name,district:State_District_Name,
serveydata:Mortality_Rate,serveydata:Fertility_Rate,serveydata:Households,serveydata:Population,serveydata:Sex_Ratio")
TBLPROPERTIES ("hbase.table.name" = "Key_Indicator_ext__hive_hbase");

```

###    Insert Command:

```
insert overwrite table Key_Indicator_ext_hive 
select Key_Indicator_ext_full.ID, Key_Indicator_ext_full.State_Name, Key_Indicator_ext_full.State_District_Name,
       Key_Indicator_ext_full.YY_Under_Five_Mortality_Rate_U5MR_Total_Person,
	   Key_Indicator_ext_full.LL_Total_Fertility_Rate_Total,
	   Key_Indicator_ext_full.AA_Households_Total,
	   Key_Indicator_ext_full.AA_Population_Total,
	   Key_Indicator_ext_full.CC_Sex_Ratio_All_Ages_Total
from Key_Indicator_ext_full;

```



# Performance Analysis

Run below three queries in the each of the three tables and observe the execution time. Based on analysis choose the format (Default/ORC) to create partition table 

- select count(*) from < Table Name >;
- select State_Name, count(*) from < Table Name > group by State_Name;
- select * from < Table Name > where State_Name = ‘Uttar Pradesh’; 

###   Runtime of First Query

​    **Table with default format**:
​     ![q1_default](/images/q1_default.jpg)

​    **Table with ORC format:**
​     ![q1_ORC](/images/q1_ORC.jpg)

​     **Hive-HBase integrated table:**
​      ![q1_integrated](/images/q1_integrated.jpg)

###    Runtime of Second Query

​      **Table with default format**:
​       ![q2_default](/images/q2_default.jpg)

​      **Table with ORC format:**
​      ![q2_ORC](/images/q2_ORC.jpg)

​       **Hive-HBase integrated table:**
​       ![q2_integrated](/images/q2_integrated.jpg)

###    Runtime of Third Query

​      **Table with default format**:
​      ![q3_default](/images/q3_default.jpg)

​     **Table with ORC format:**
​      ![q3_ORC](/images/q3_ORC.jpg)

​     **Hive-HBase integrated table:**
​      ![q3_integrated](/images/q3_integrated.jpg)



# Create and insert command for the partition table

Based on above analysis we have seen query works fast in ORC format table. In small dataset it may be not significant but in big dataset it can improve quite a lot. S we are going to create our final table and will use partition for more better performance. 

We will perform all the subsequence analysis in this table only

### Create Command

```bash
CREATE EXTERNAL TABLE IF NOT EXISTS Key_Indicator_ext_partition(
ID int,
State_District_Name	 string,
YY_Under_Five_Mortality_Rate_U5MR_Total_Person double,
LL_Total_Fertility_Rate_Total double,
AA_Households_Total double,
AA_Population_Total double,
CC_Sex_Ratio_All_Ages_Total double
)
PARTITIONED BY (State_Name string)
STORED AS ORC
LOCATION '/user/ec2-user/key_indicator/key_indicator_partition'
tblproperties("orc.compress"="SNAPPY");

```

### Insert Command

```bash
INSERT INTO Key_Indicator_ext_partition
PARTITION (State_Name)
SELECT ID,State_District_Name,YY_Under_Five_Mortality_Rate_U5MR_Total_Person,LL_Total_Fertility_Rate_Total,AA_Households_Total,AA_Population_Total,CC_Sex_Ratio_All_Ages_Total,State_Name
FROM Key_Indicator_ext_orc;

```



# Perform Final Analysis

From the partition table we have to find out below 

- Child Mortality rate of Uttar Pradesh
- Fertility rate of Bihar
- Correlation between Child Mortality rate and Fertility Rate
- Highest population per household
- Lowest Sex Ratio per state



### Child Mortality rate of Uttar Pradesh

  **Requirement:**  Find out the child mortality rate of Uttar Pradesh state

  **Query:**

```
SELECT State_Name,
AVG(YY_Under_Five_Mortality_Rate_U5MR_Total_Person) AS Child_Mortality_Rate_UP
FROM Key_Indicator_ext_partition
WHERE State_Name='Uttar Pradesh'
GROUP BY State_Name;

```

  **Output:**

  ![Mortality_Rate](/images/Mortality_Rate.jpg)



### Fertility rate of Bihar

  **Requirement:** Find out the fertility rate of Bihar state.

  **Query:**

```bash
SELECT State_Name,
AVG(LL_Total_Fertility_Rate_Total) AS Fertility_Rate_Bihar
from Key_Indicator_ext_partition 
WHERE TRIM(State_Name) = 'Bihar'
GROUP BY State_Name;
```

  **Output:**

  <img src="/images/Fertility_Rate.jpg" alt="Fertility_Rate" style="zoom:80%;" />



### Correlation between Child Mortality rate and Fertility rate

  **Requirement:** Find out State wise child mortality rate and state wise fertility rate. Does 
                             high fertility correlate with high child mortality.

####   State wise Child Mortality Rate:

  **Query:**

```bash
SELECT State_Name ,
AVG(YY_Under_Five_Mortality_Rate_U5MR_Total_Person)  AS Child_Mortality_Rate
from Key_Indicator_ext_partition
GROUP BY State_Name;
```

 **Output:**

  <img src="/images/Child_Mortaity.jpg" alt="Child_Mortaity" style="zoom:80%;" />

#### State wise Fertility Rate:

**Query:**

```bash
SELECT
State_Name ,
AVG(LL_Total_Fertility_Rate_Total) AS Fertility_Rate
from Key_Indicator_ext_partition
GROUP BY State_Name;
```

**Output:**

<img src="/images/Fertility.jpg" alt="Fertility" style="zoom:80%;" />

#### Correlation between High Child Mortality and High Fertility:

**Query:**

```bash
SELECT CORR(Child_Mortality_Rate,Fertility_Rate) AS Correlation
FROM 
(
SELECT
State_Name ,
AVG(YY_Under_Five_Mortality_Rate_U5MR_Total_Person)  AS Child_Mortality_Rate,
AVG(LL_Total_Fertility_Rate_Total) AS Fertility_Rate
from Key_Indicator_ext_partition
GROUP BY State_Name)a;
```

**Output:**

![correlation](/images/correlation.jpg)



### Highest population per household

 **Requirement:** Find top 2 districts per state with the highest population per household

 **Query:**

```bash
SELECT State_Name AS State_Name,
       State_District_Name AS State_District_Name,
       pp_per_hh As Population_Per_Household 
FROM (
      SELECT State_Name ,
             State_District_Name ,
             (AA_Population_Total/AA_Households_Total) as pp_per_hh,
             RANK() OVER(PARTITION BY State_Name ORDER BY (AA_Population_Total/AA_Households_Total) DESC) AS RNK
      FROM Key_Indicator_ext_orc) a
WHERE RNK IN(1,2);
```

 **Output:**

 <img src="/images/Population_per_household.jpg" alt="Population_per_household" style="zoom:80%;" />

### Lowest Sex Ratio per State

 **Requirement:** Find top 2 districts per state with the lowest sex ratios.

 **Query:**

```bash
SELECT State_Name,District_Name,Sex_Ratio
FROM(
SELECT
State_Name ,
State_District_Name AS District_Name,
CC_Sex_Ratio_All_Ages_Total AS Sex_Ratio,
DENSE_RANK() OVER(PARTITION BY State_Name ORDER BY CC_Sex_Ratio_All_Ages_Total) AS RNK
from Key_Indicator_ext_orc)a
WHERE a.RNK IN(1,2);
```

 **Output:**

 <img src="/images/lowest_sex_ratio.jpg" alt="lowest_sex_ratio" style="zoom:80%;" />