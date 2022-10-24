package com.snowflake.labs.hl7;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.parser.XMLParser;
import ca.uhn.hl7v2.util.Hl7InputStreamMessageStringIterator;
import ca.uhn.hl7v2.validation.impl.ValidationContextFactory;

/**
 * The core parser program which will be called by warapper classes.
 * 
 * @author : Venkatesh Sekar 
 */
public class HL7Parser {
    static Logger logger = java.util.logging.Logger.getLogger(HL7Parser.class.getName());

    protected static String INVALID_MESSAGE = null;
    protected static String INVALID_MESSAGE_XML = null;

    protected XmlMapper xmlMapper = new XmlMapper();
            
    /*
     * The HapiContext holds all configuration and provides factory methods for
     * obtaining all sorts of HAPI objects, e.g. parsers.
     */
    HapiContext c_context = new DefaultHapiContext();

    public HL7Parser() {

        /*
        * This is actually redundant, since this is the default
        * validator. The default validation includes a number of sensible
        * defaults including maximum lengths on string types, formats for
        * telephone numbers and timestamps, etc.
        */
        c_context.setValidationContext(ValidationContextFactory.defaultValidation());
    }

    /**
     * Encode the HL7 xml to json format
     * @param p_hl7_xml
     * @param p_record
     * @return
     */
    protected HL7ParsedRecord xml_to_json(String p_hl7_xml, HL7ParsedRecord p_record) {
        HL7ParsedRecord ret = p_record;

        if (p_hl7_xml == INVALID_MESSAGE_XML)
            return ret;

        try {
            JsonNode node = xmlMapper.readTree(p_hl7_xml.getBytes());

            JsonNode msh_9 = node.get("MSH").get("MSH.9");
            ret.message_type = msh_9.get("CM_MSG.1").asText() 
                + "_" + msh_9.get("CM_MSG.2").asText();

            ret.message_sequence_num = node.get("MSH").get("MSH.10").asInt();
            ret.message_version = node.get("MSH").get("MSH.12").asText();

            ObjectMapper jsonMapper = new ObjectMapper();
            ret.hl7_json = jsonMapper.writeValueAsString(node);
        } catch (IOException ex) {
            ret.error_msg = ex.getLocalizedMessage();
            logger.severe("Unable to parse message : " + ex.getLocalizedMessage());
            ex.printStackTrace();
        }

        return ret;
    }

    /**
     * Parse the Hl7 message and encode to XML.
     * 
     * @param p_message
     * @return
     */
    protected HL7ParsedRecord parse_and_encodeto_XML(String p_message, HL7ParsedRecord p_record ,Boolean p_validate_message) {
        logger.fine("Parsing message ...");
        HL7ParsedRecord ret = p_record;
        try {

            // Get the reference to the context parser, which handles the "traditional or
            // default encoding"
            PipeParser ourPipeParser = c_context.getPipeParser();

            c_context.getParserConfiguration().setValidating(p_validate_message);
            
            // parse the string format message into a Java message object.
            Message hl7Message = ourPipeParser.parse(p_message);

            // Get referemce to the context XML parser
            XMLParser xmlParser = c_context.getXMLParser();

            // convert from default encoded message into XML format
            ret.hl7_xml = xmlParser.encode(hl7Message);
            ret.parsed_status = true;
        } catch (HL7Exception ex) {
            ret.error_msg = ex.getLocalizedMessage();
            logger.severe("Unable to parse message : " + ex.getLocalizedMessage());
            ex.printStackTrace();
            logger.info(p_message);

        }

        return ret;
    }

    /**
     * Extract core attributes from message header
     * @param p_record
     * @return
     */
    protected HL7ParsedRecord extractMSHAttributes(HL7ParsedRecord p_record) {
        logger.fine("Parsing message ...");
        HL7ParsedRecord ret = p_record;

        //get the message header line
        List<String> msh_ln_1 =  ret.raw_msg
            .lines()
            .filter(ln -> ln.trim().toUpperCase().startsWith("MSH"))
            .collect(Collectors.toList())
            ;

        if (msh_ln_1.isEmpty() == false) {
            return ret;
        }

        try {
            //There could be error with the MSH data itself, hence
            //enclosing in a try/catch
            //get the 7 (msg type), 10 (msg version) ,8 (seq num)
            String msh_ln = msh_ln_1.get(0);
            String[] splits = msh_ln.split("\\|");
            
            ret.message_version = splits[11];

            if (splits[8].trim().length() > 0)
                ret.message_type = splits[8].replace('^','_');

            if (splits[9].trim().length() > 0)
                ret.message_sequence_num = Integer.parseInt(splits[9]);
            
        } catch (Exception ex) {
            logger.warning("MSH record is not correctly formatted : " + ex.getLocalizedMessage());
        }
            
        return ret;
    }

    /**
     * Process the HL7 filestream.
     * @param p_filestream
     * @return
     */
    protected List<HL7ParsedRecord> process_filestream(InputStream p_filestream ,Boolean p_validate_message) {
        // Adoption of this allows to identify records splits based on MSH
        Hl7InputStreamMessageStringIterator hl7_iter = 
            new Hl7InputStreamMessageStringIterator(p_filestream);

        List<HL7ParsedRecord> rows = new ArrayList<>(10);

        while (hl7_iter.hasNext()) {
            logger.finest(">>>> Message ...");
            String hl7_raw = hl7_iter.next();

            HL7ParsedRecord hl7_row = 
                new HL7ParsedRecord(false, hl7_raw, 
                    INVALID_MESSAGE_XML, INVALID_MESSAGE, 
                    null, null, -1, null);

            hl7_row = extractMSHAttributes(hl7_row);
            hl7_row = parse_and_encodeto_XML(hl7_raw, hl7_row ,p_validate_message);

            if(hl7_row.parsed_status) {
                hl7_row = xml_to_json(hl7_row.hl7_xml, hl7_row);
            }
            
            rows.add(hl7_row);
        }

        return rows;
    }

    /**
     * Process the HL7 data file.
     * 
     * @param p_dataFilePath
     * @return
     * @throws IOException
     */
    private List<HL7ParsedRecord> readFile(String p_dataFilePath ,Boolean p_validate_message)
            throws IOException {
        logger.info(
                String.format("Parsing file %s ...", p_dataFilePath));

        InputStream is = new BufferedInputStream(
                new FileInputStream(p_dataFilePath));

        return process_filestream(is ,p_validate_message);
    }

    /**
     * Convienence method to parse file from local directory.
     * 
     * @param p_dataFileName
     * @return
     * @throws IOException
     */
    public List<HL7ParsedRecord> handle(String p_dataFileName ,Boolean p_validate_message)
            throws IOException {
        String fPath = p_dataFileName;

        List<HL7ParsedRecord> hl7_rows = readFile(fPath,p_validate_message);
        return hl7_rows;
    }
}
