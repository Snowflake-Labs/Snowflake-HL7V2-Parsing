# Setup

This page will walk through the initial steps of staging the jar in an internal stage or external stage.

## Compiling and Packaging
The project is maven based, hence compiling and building the package is done using [maven](https://maven.apache.org/). It is expected that you are knowledgeable on Java and its development lifecycle.

```sh
mvn clean package
```

Once completed, the jar (sf-hl7v2-parser-1.0-SNAPSHOT-jar-with-dependencies.jar) will be present in target directory. The target directory gets created as part of the maven execution.

**NOTE:** For convienience the jar is pre-built and hosted here [target/sf-hl7v2-parser-1.0-SNAPSHOT-jar-with-dependencies.jar](../target/sf-hl7v2-parser-1.0-SNAPSHOT-jar-with-dependencies.jar)

## Staging the library
I am staging the library in an internal stage, though you can use the external stage too. I am able to stage using SnowSQL.

```sql
use role sysadmin;
use schema stage_db.public;
use warehouse lab_wh;

-- create the stage
create or replace stage data_lib_stage
    directory = ( enable = true )
    comment = 'used for staging data & libraries'
    ;

-- upload the jar file to the stage
put file://./target/sf-hl7v2-parser-1.0-SNAPSHOT-jar-with-dependencies.jar @data_lib_stage/jar_lib;

-- ensure to refresh the stage
alter stage data_lib_stage refresh;

-- run this to verify the jar shows up in the list
select *
from directory(@data_lib_stage);
```

Alter Refresh is a must before defining the UDTFs.

## Defining the UDTF
The following commands are used to define the UDTFs.

### JAVA UDTF: hl7_hapi_parser
The UDTF implementation: [Doc](hl7/HL7UDTF.md).

```sql
create or replace function hl7_hapi_parser(hl7_fl_url varchar ,validate_message boolean)
    RETURNS TABLE ( 
        parsed_status boolean, 
        raw_msg varchar, 
        hl7_xml varchar,  
        hl7_json variant,  
        message_type varchar, 
        message_version varchar, 
        message_sequence_num integer, 
        error_msg varchar 
    )
  language JAVA
  imports = ('@data_lib_stage/jar_lib/sf-hl7v2-parser-1.0-SNAPSHOT-jar-with-dependencies.jar')
  handler = 'com.snowflake.labs.hl7.HL7UDTF'
  comment = 'Java based UDTF for parsing HL7v2 files.'
;
```

### JAVA UDF: hl7_hapi_udf_parser
The UDF implementation: [Doc](hl7/HL7UDF.md).

```sql
create or replace function hl7_hapi_udf_parser(hl7_msg varchar ,validate_message boolean)
returns variant
language java
imports = ('@data_lib_stage/jar_lib/sf-hl7v2-parser-1.0-SNAPSHOT-jar-with-dependencies.jar')
handler = 'com.snowflake.labs.hl7.HL7UDF.process'
comment = 'Java based UDF for parsing HL7v2 messages.'
;
```

## Staging data file
While this is not necessarily a setup, but I am covering here to demonstrate the steps i took to stage the HL7 file. For my demo, i am using the sample file from here [src/test/data/hl7/hl7_2-3_samples.txt](../src/test/data/hl7/hl7_2-3_samples.txt). 

### Upload to stage

```sql
use role sysadmin;
use schema stage_db.public;
use warehouse lab_wh;

-- upload the sample to stage
put file://./src/test/data/hl7/hl7_2-3_samples.txt @data_lib_stage/data AUTO_COMPRESS = FALSE;

-- refresh should be done
alter stage data_lib_stage refresh;

-- verify the staged data file is present 
select *
from directory(@data_lib_stage);

```
