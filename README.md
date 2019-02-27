# ETL pipeline using Apache Spark SQL

##Why Spark SQL?

Spark SQL brings native support for SQL to Spark and streamlines the process of querying data stored both in RDDs and in external sources. Spark SQL conveniently blurs the lines between RDDs and relational tables. Unifying these powerful abstractions makes it easy for developers to intermix SQL commands querying external data with complex analytics, all within in a single application.
Spark SQL is used in this case study to:

- Run SQL queries over imported JSON data and existing RDDs
- Easily write RDDs out to Hive tables

##Scalability

Spark SQL also includes a cost-based optimizer, columnar storage, and code generation to make queries fast. At the same time, it scales to thousands of nodes and multi-hour queries using the Spark engine, which provides full mid-query fault tolerance, without having to worry about using a different engine for historical data.

##Implementation

This project aims to do 3 sets of transformations. A cleansing task is done initially inorder to maintain uniformity in data pattern. (Urls were not following a uniform pattern where https:// is missing for a few records)

- **Transformation at user level**
*Table: user_details*

| Field | Description |
| --- | --- |
| user_id | identifier for the user |
| time_stamp | UTC timestamp of when the event was fired |
| url_level1 | first piece of the url (ie. the domain from the url where this action took place) |
| url_level2 | second level of the url path |
| url_level3 | third level of the url path |
| activity | activity that took place |

- **Count of events for an activity at hourly level**
*Table: activity_details*

| Field | Description |
| --- | --- |
| time_bucket | hourly time granularity |
| url_level1 | first piece of the url (ie. the domain from the url where this action took place) |
| url_level2 | second level of the url path |
| activity | activity that took place |
| activity_count | count of events for the activity |
| user_count | unique number of users that performed the activity |

- **Most frequently searched tradie in a month**

This will help the business to understand the demand for a particular tradie in a month. If more information is provided in the data such as location etc, then we can understand the localised demand for a tradie at a granular level of day or hour.

*Table: activity_details*

| Field | Description |
| --- | --- |
| month_bucket | monthly granularity |
| trade | tradie/trade |
| search_frequency | number of times the corresponding tradie was searched |

##Sample Output

- user_details


|user_id|         time_stamp| url_level1|          url_level2|  url_level3|      activity|
| --- | --- | --- | --- | --- | --- |
|  56456|02/02/2017 20:22:00|hipages.com|            articles|        NULL|     page_view|
|  56456|02/02/2017 20:23:00|hipages.com|             connect| sfelectrics|     page_view|
|  56456|02/02/2017 20:26:00|hipages.com|get_quotes_simple...|        NULL|     page_view|
|  56456|01/03/2017 20:21:00|hipages.com|           advertise|        NULL|  button_click|
|  56456|02/02/2017 20:12:34|hipages.com|              photos|   bathrooms|     page_view|
|  56456|01/01/2017 20:22:00|hipages.com|                find|electricians|list_directory|
|  56456|02/02/2017 20:26:07|hipages.com|            articles|        NULL|     page_view|
|  56456|02/02/2017 20:22:00|hipages.com|             connect| sfelectrics|  button_click|

- activity_details

|time_bucket|     url_level1|          url_level2|      activity|activity_count|user_count|
| --- | --- | --- | --- | --- | --- |
| 2017010120|    hipages.com|get_quotes_simple...|     page_view|            30|        30|
| 2017020220|    hipages.com|                find|     page_view|            26|        26|
| 2017030120|    hipages.com|            articles|     page_view|            29|        29|
| 2017020220|    hipages.com|get_quotes_simple...|     page_view|             1|         1|
| 2017020220|  somepages.com|                find|     page_view|             1|         1|

- trade_demand

|month_bucket|       trade|search_frequency|
| --- | --- | --- |
|      201701|electricians|              29|
|      201702|electricians|              32|
|      201703|    plumbers|               1|
|      201704|    plumbers|              28|
|      201705|electricians|              28|
|      201712|electricians|              28|

##How to run the code?

1. Install Spark : [Reference](https://medium.freecodecamp.org/installing-scala-and-apache-spark-on-mac-os-837ae57d283f)
2. Install Hive : [Reference](https://qiita.com/giwa/items/dabf0bb21ae242532423)
3. Clone the project
   *git clone {project_url}*
4. Build the project
   *cd etl-spark*
   Run sbt. This will open up the sbt console. Execute the below commands.
   ```
   clean
   reload
   compile
   assembly
   ```
   Jar file is now generated in the target folder(/etl-spark/target/scala-2.11/etl-spark-assembly-0.1.jar).
5. Execute the command in terminal to run the spark job. Sample input file is present in the src/main/resources folder.

spark-submit --class com.etl.exec.EventTransform {jar_location} {input_data_location} {output_table_location}

Example:
spark-submit --class com.etl.exec.EventTransform /Users/mathewir/Desktop/Irene/hobby-projects/etl-spark/target/scala-2.11/etl-spark-assembly-0.1.jar /Users/mathewir/Desktop/source_event_data.json /Users/mathewir/Desktop/output/