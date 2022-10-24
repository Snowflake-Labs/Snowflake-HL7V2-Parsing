package com.snowflake.labs.hl7;

/**
 * Represents an individual HL7 record that was parsed.
 * @author : Venkatesh Sekar
 */
public class HL7ParsedRecord {

  /**
   * Indicates any parsing error.
   */
  public String error_msg = null;

  /**
   * Status of parsing the message. True for successful parsing, False for failure.
   */
  public Boolean parsed_status = false;

  /**
   * Raw message
   */
  public String raw_msg = null;

  /**
   * Parsed message in XML format
   */
  public String hl7_xml = null;

  /**
   * Parsed message in JSON format
   */
  public String hl7_json = null;
  
  /**
   * Message type, retrieved from the message header
   */
  public String message_type = null;

  /**
   * Message version, retrieved from the message header
   */
  public String message_version = null;

  /**
   * Message sequence number, retrieved from the message header
   */
  public int message_sequence_num = -1;

  public HL7ParsedRecord(Boolean parsed_status, String raw_msg, String hl7_xml, String hl7_json, 
      String message_type, String message_version, int message_sequence_num, String error_msg) {
    this.raw_msg = raw_msg;
    this.hl7_xml = hl7_xml;
    this.hl7_json = hl7_json;
    this.error_msg = error_msg;
    this.parsed_status = parsed_status;
    this.message_type = message_type;
    this.message_version = message_version;
    this.message_sequence_num = message_sequence_num;
  }

  public HL7ParsedRecord(HL7ParsedRecord p_src) {
    this.raw_msg = p_src.raw_msg;
    this.hl7_xml = p_src.hl7_xml;
    this.hl7_json = p_src.hl7_json;
    this.error_msg = p_src.error_msg;
    this.parsed_status = p_src.parsed_status;
    this.message_type = p_src.message_type;
    this.message_version = p_src.message_version;
    this.message_sequence_num = p_src.message_sequence_num;
  }

}