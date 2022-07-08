# Real-time Data Warehouse Based on Flink

## Description
This project is a demonstration of how to build a real-time data warehouse for a 
typical e-commerce site using Flink. The e-commerce site generates two types of data:
the user action log data from its frontend and the business process data stored 
and updated in MySQL

### ODS

User action data generated from the e-commerce frontend website are sent to 
the log-data collection module ```micromall-logger```, which is built using
SpringBoot. The controller ```LoggerController``` writes the log data onto disk
and sends them to the Kafka topic ```ods_base_log```. Nginx is used here as the 
reverse proxy.

Real-time business process data are collected using FlinkCDC 
(```micromall-realtime/ods/FlinkCDC```), which is based on
binlog and can be considered as a distributed Maxwell. The output is written
into the Kafka topic ```ods_base_db```. To convert the data into json string
before writing into Kafka, I wrote ```CustomDeserialization``` and used it
to build the ```MySQLSource``` as the Flink source.

### DWD and DIM

The log data stream in Kafka topic ```ods_base_log``` is separated into 3 sub-streams using Flink
side-output (```dwd/BaseLogApp```). The 3 sub-streams are written back into
Kafka under 3 different DWD topics. Dirty data that cannot be parsed into normal
json format are written into a separate side output stream with the tag ```Dirty```.
This dirty stream should be processed or persisted, but I have not implemented it.
New and old visitors are distinguished by using Flink ValueState.

The business process data stream in Kafka topic ```ods_base_db``` is separated into
DWD data, which are written back into Kafka under DWD topics, and DIM data, which
are written into HBase. The code is in ```dwd/BaseDBApp```. Because there are 47 MySQL tables,
it is not practical to hard-wire the logic for each table individually, I adopted
a dynamic data separation scheme by using a Flink BroadcastStream, which has a MySQLSource
synced with a separate MySQL table ```gmall-realtime.table_process``` using FlinkCDC.

The ```gmall-realtime.table_process``` table stores the names of the source tables, 
the names and types (Kafka/HBase) of the sink tables and also the fields that go
into the sink tables. The Flink Broadcast stream reads ```gmall-realtime.table_process```, 
which can be changing in realtime without restarting the application, connects with
the main stream that reads the Kafka topic ```ods_base_db``` and is processed using
```function/TableProcessFunction```, which splits the stream and writes fact tables
into the Kafka stream and dimension tables into the side output stream. The Kafka stream
is then separated into topics determined dynamically based on the input json string.
The side output stream is written into HBase (through Phoenix) using the customized
sink ```function/DimSinkFunction```, which determines the name of the dimension table
dynamically from the input json string.

### DWM
- Unique Visitor (UV) for DAU: ```dwm/UniqueVisitApp```
- Bounce Rate: ```dwm/UserJumpDetailApp``` (Flink CEP)
- Order Wide Table: ```dwm/OrderWideApp``` (intervalJoin between order info and order detail fact tables: much simpler than Spark with Redis; ```DimUtil```: cache-aside-pattern using Redis; ```DimAsyncFunction```: Async I/O)
- Payment Wide Table: ```PaymentWideApp``` (intervalJoin with order wide table)

### DWS 
All output data are written into ClickHouse, which is used as OLAP.
  - Visitor Statistics: ```dws/VisitorStatsApp``` (collect uv, page-view, session-visit, bounce-rate statistics based on dimension data for every 10 s)
  - Product Statistics: ```dws/ProductStatsApp```
  - Region Statistics: ```dws/ProvinceStatsSqlApp``` (Flink SQL)
  - User Search Statistics: ```dws/KeywordStatsApp``` (Flink SQL, Table Function or UDTF)


## Built-with

- SpringBoot
- Nginx
- MySQL
- Kafka
- FlinkCDC
- HBase
- Phoenix
- Redis
- ClickHouse