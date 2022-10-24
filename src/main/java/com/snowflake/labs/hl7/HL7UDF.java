package com.snowflake.labs.hl7;

import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Implementation wrapper class for Snowflake Java UDF. 
 * 
 * @author : Venkatesh Sekar
 */
public class HL7UDF
        extends HL7Parser {
    static Logger logger = java.util.logging.Logger.getLogger(HL7UDF.class.getName());

    /**
     * Example invocation :
     * <br>
     * <code>
     *  select raw_msg, hl7_hapi_udf_parser(raw_msg)
     *  from hl7_msg;
     * </code>
     * 
     * @param p_filestream
     * @param p_validate_message
     * @return 
     */
    public String process(String p_hl7_raw, Boolean p_validate_message) 
        throws JsonProcessingException {

        HL7ParsedRecord hl7_row = new HL7ParsedRecord(false, p_hl7_raw, 
            INVALID_MESSAGE_XML, super.INVALID_MESSAGE, 
            null, null, -1, null);

        hl7_row = super.extractMSHAttributes(hl7_row);
        hl7_row = super.parse_and_encodeto_XML(p_hl7_raw, hl7_row ,p_validate_message);

        if(hl7_row.parsed_status) {
            hl7_row = super.xml_to_json(hl7_row.hl7_xml, hl7_row);
        }

        final ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(hl7_row);
    }

}