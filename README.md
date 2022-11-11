# Ingestion and Processing of HL7 V2.x messages using Snowflake Demo's

## Why this Demo
### Problem Statement
With the support for unstructured data in Snowflake, customers no longer need to run a processing pipeline of HL7 V2.x outside Snowflake. Now customers can directly store the HL7 V2.x messages in Snowflake, process them using Java UDF's and run analytics on the fly. 


## Technical Deep-Dive

[Medium Blog](https://medium.com/snowflake/hl7-ingestion-and-processing-architectural-patterns-with-snowflake-3703b8c08ea4)

# Processing of HL7 V2.x messages

Prototype implementations for parsing HL7 V2.x messages.

---
## Implementations

| Type          | Implementation Class                                                             | 
|---------------|----------------------------------------------------------------------------------|
| Java UDTF     | [HL7UDTF](./src/main/java/com/snowflake/labs/hl7/HL7UDTF.java)                   |
| Java UDF      | [HL7UDF](./src/main/java/com/snowflake/labs/hl7/HL7UDF.java)                     |
| Python UDF    | [hl7pyparserUDF](./src/main/python/hl7pyparserUDF.py)                            |


## Detailed documentation/QuickStart for Types that are in PuPr
[Quickstart](doc/hl7/ProcessingHL7V2MessageswithSnowflake-PuPrFeatures.md)

### Installation / Deployment
* Refer to the respective type documentation for setup, installations, sample runs etc..

### Note 
The implementations provided here are at best MVP/Prototype versions, they are not of
production quality. You are free to extend the functionality and improve the code as it fits your functionality.

### Reference
- HL7 sample file [hl7_2-3_samples.txt](src/test/data/hl7/hl7_2-3_samples.txt) was sourced from [simhospital](https://github.com/google/simhospital/blob/master/docs/sample.md).
