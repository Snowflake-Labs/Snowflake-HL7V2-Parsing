package com.snowflake.labs.hl7;

import java.io.InputStream;
import java.util.logging.Logger;
import java.util.List;
import java.util.stream.Stream;

/**
 * Implementation wrapper class for Snowflake Java UDTF. 
 * 
 * @author : Venkatesh Sekar
 */
public class HL7UDTF
        extends HL7Parser {
    static Logger logger = java.util.logging.Logger.getLogger(HL7UDTF.class.getName());

    public static Class getOutputClass() {
        return HL7ParsedRecord.class;
    }

    /**
     * Snowflake would be invoking this method for each partition or a file. The path
     * to the file is the first parameter, Snowflake takes the effort of reading the file
     * from the stage and passing the same as an input stream.
     * 
     * Example invocation :
     * <br>
     * <code>
     *  select *
     *   from table(hl7_hapi_parser('@stg_hl7/datasets/hl7/sample_2.txt'));
     * </code>
     * 
     * @param p_filestream
     * @param p_validate_message
     * @return 
     */
    public Stream<HL7ParsedRecord> process(InputStream p_filestream, Boolean p_validate_message) {
        List<HL7ParsedRecord> hl7_rows = super.process_filestream(p_filestream, p_validate_message);
        return hl7_rows.stream();
    }

}
