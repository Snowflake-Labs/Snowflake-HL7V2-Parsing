# HL7UDF 

[HL7UDF](../../src/main/java/com/snowflake/labs/hl7/HL7UDF.java) is a java based UDF, which uses 
the popular [HL7 HAPI parser](https://hapifhir.github.io/hapi-hl7v2/), to parse HL7 v2.3 messages.

The UDF implementation uses the same set of class, as used by the HL7UDTF counter part. This
differs from the [HL7UDTF](../../src/main/java/com/snowflake/labs/hl7/HL7UDTF.java) implementation as noted below:

- Parses HL7 raw message and returns the only 1 record.
- Does not parse a file.
- Meant to be used as part of inline parsing/conversion.
- Each input row will return 1 json doc with all the  columns (errors, hl7_xml, hl7_json etc...)
 

## Installation 

### Building and Packaging the jar
Refer to [setup doc](../setup_java.md) section: Compiling and Packaging

### Deployment of jar
Refer to [setup doc](../setup_java.md) section: Staging the library

### Registering the UDF

The UDF can be defined in Snowflake as below:
```sql
create or replace function hl7_hapi_udf_parser(hl7_msg varchar ,validate_message boolean)
returns variant
language java
imports = ('@data_lib_stage/jar_lib/sf-hl7v2-parser-1.0-SNAPSHOT-jar-with-dependencies.jar')
handler = 'com.snowflake.labs.hl7.HL7UDF.process'
comment = 'Java based UDF for parsing HL7v2 messages.'
;
``` 

## Sample run

### Parsing the HL7 data

Assuming that a table 'hl7_msg' exists in the database, in which each record has a single hl7v2 message in the raw_msg column.
The following query invokes the function, to parse the HL7 message file :

```sql
select 
    raw_msg, -- column holding the 1 HL7v2.x message in pipe delimited format
    hl7_hapi_udf_parser(raw_msg)
from hl7_msg
;
``` 

#### Retrieving the hl7_json column
To get hl7_json column, we have to issue the sql statement as :

```sql
 with base as (
    select
        raw_msg, -- column holding the 1 HL7v2.x message in pipe delimited format
        hl7_hapi_udf_parser(raw_msg, false) as parsed
    from raw_hl7
    limit 10
)
select
    parse_json(parsed:"hl7_json") as hl7_json
from base;
```

