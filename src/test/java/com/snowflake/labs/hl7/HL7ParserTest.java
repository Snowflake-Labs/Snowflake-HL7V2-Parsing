package com.snowflake.labs.hl7;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.List;

import org.junit.jupiter.api.Test;

public class HL7ParserTest {
    
    @Test
    void parseMessageWithNoValidation() throws Exception {
        String hl7_message = ""
            + "MSH|^~\\&|SIMHOSP|SFAC|RAPP|RFAC|20200502220643||ORU^R01|2|T|2.3|||AL||44|ASCII"
            + "PID|1|2590157853^^^SIMULATOR MRN^MRN|2590157853^^^SIMULATOR MRN^MRN~2478684691^^^NHSNBR^NHSNMBR||Esterkin^AKI Scenario 6^^^Miss^^CURRENT||19890118000000|F|||170 Juice Place^^London^^RW21 6KC^GBR^HOME||020 5368 1665^HOME|||||||||R^Other - Chinese^^^||||||||"
            + "PV1|1|O|ED^^^Simulated Hospital^^ED^^|28b|||C006^Woolfson^Kathleen^^^Dr^^^DRNBR^PRSNL^^^ORGDR|||MED||||||||||||||||||||||||||||||||||20200501140643||"
            + "ORC|RE|3259758581|3281433988||CM||||20200502220643"
            + "OBR|1|3259758581|3281433988|us-0003^UREA AND ELECTROLYTES^WinPath^^||20200502220643|20200502220643|||||||20200502220643||||||||20200502220643|||F||1"
            + "OBX|1|NM|tt-0003-01^Creatinine^WinPath^^||83.00|UMOLL|49 - 92||||F|||20200502220643||";

        HL7Parser parser = new HL7Parser();

        //there are some invalid messages, hence you will see some invalid exceptions
        List<HL7ParsedRecord> records = parser.process_filestream(
            new ByteArrayInputStream(hl7_message.getBytes())
            ,false);

        assertFalse(records.isEmpty());
        assertTrue(records.size() == 1);

        HL7ParsedRecord rec = records.get(0);
        assertTrue(rec.parsed_status);
        assertTrue(rec.hl7_json != null);
        assertEquals("2.3" ,rec.message_version);
        assertEquals(2 ,rec.message_sequence_num);
        assertEquals("ORU_R01" ,rec.message_type);
    }

    /**
     * Parsing message which has an parsing error
     * 
     * @throws Exception
     */
    @Test
    void parseMessageWithError() throws Exception {
        String hl7_message = ""
        +"MSH|^~\\&|SIMHOSP|SFAC|RAPP|RFAC|20200508130906||ORU^R01|340|T|2.3|||AL||44|ASCII\r\n"
        +"PID|1|1000867336^^^SIMULATOR MRN^MRN|1000867336^^^SIMULATOR MRN^MRN~0468393242^^^NHSNBR^NHSNMBR||Wylam^Lewis^^^Mr^^CURRENT||20180424000000|M|||175 Spoon Lane^^Wembley^^SS71 8VH^GBR^HOME||074 0182 3746^HOME|||||||||A^White - British^^^||||||||\r\n"
        +"PV1|1|I|OtherWard^MainRoom^Bed 124^Simulated Hospital^^BED^Main Building^4|28b|||C005^Whittingham^Sylvia^^^Dr^^^DRNBR^PRSNL^^^ORGDR|||CAR|||||||||7128287312183039020^^^^visitid||||||||||||||||||||||ARRIVED|||20200508130906||\r\n"
        +"ORC|RE|1452504149|2086052792||CM||||20200508130906\r\n"
        +"OBR|1|1452504149|2086052792|us-0005^Vital Signs^WinPath^^||20200508130906|20200508130906|||||||20200508130906||||||||20200508130906|||F||1\r\n"
        +"OBX|1||tt-0005-03^InsOxy^WelchAllyn^^||28|MDC_DIM_PERCENT|20-40||||F|||20200508130906||\r\n"
        +"OBX|2|NM|tt-0005-05^MDC_PRESS_BLD_NONINV_SYS^MDC^^||98.57|MDC_DIM_MMHG|70-190||||F|||20200508130906||\r\n"
        +"OBX|3||tt-0005-10^OxyL^WelchAllyn^^||5|MDC_DIM_X_L_PER_MIN|1-15||||F|||20200508130906||\r\n"
        +"NTE|0||Bet klomps read scow|\r\n"
        +"NTE|1||Maybe pseudoscience measure stage lute wrestler yellow patriot|\r\n"
        +"OBX|4|NM|tt-0005-09^MDC_TEMP^MDC^^||36.51|MDC_DIM_DEGC|36-38||||F|||20200508130906||\r\n"
        +"NTE|0||Success astrologer diadem athlete congressperson opposite|\r\n"
        +"OBX|5|TX|tt-0005-11^OxygenDev^WelchAllyn^^||Venturi||||||F|||20200508130906||\r\n"
        +"NTE|0||Shrimp altitude hatred stay gym affair|\r\n"
        +"OBX|6|TX|tt-0005-02^BowelMovement^WelchAllyn^^||Yes||||||F|||20200508130906||\r\n"
        +"OBX|7|NM|tt-0005-06^MDC_PULS_OXIM_SAT_O2^MDC^^||86.36|MDC_DIM_PERCENT|30-100||||F|||20200508130906||\r\n"
        +"OBX|8||tt-0005-08^MDC_RESP_RATE^MDC^^||||10-50||||F|||20200508130906||\r\n"
        +"NTE|0||Thongs remote|\r\n"
        +"OBX|9|NM|tt-0005-07^MDC_PULS_RATE_NON_INV^MDC^^||150.11|MDC_DIM_BEAT_PER_MIN|50-200||||F|||20200508130906||\r\n"
        +"OBX|10|NM|tt-0005-12^PAIN LEVEL^L^^||5.23|/10|1-10||||F|||20200508130906||\r\n"
        +"NTE|0||Bread adjustment special mocha film press pineapple|\r\n"
        +"OBX|11|TX|tt-0005-13^TemperatureSite^WelchAllyn^^||Axillary||||||F|||20200508130906||\r\n"
        +"NTE|0||Centurion bagpipe pusher pear patina cameo leprosy|\r\n"
        +"OBX|12|NM|tt-0005-04^MDC_PRESS_BLD_NONINV_DIA^MDC^^||57.46|MDC_DIM_MMHG|40-100||||F|||20200508130906||\r\n"
        +"NTE|0||Tunic advice pharmacopoeia cash vanity laparoscope belief|\r\n"
        +"NTE|1||Go-Kart spare republic enthusiasm worklife solid fencing lock|\r\n"
        +"OBX|13|TX|tt-0005-01^AVPU^LOINC^^||Alert||||||F|||20200508130906||\r\n"
        +"NTE|0||Character decade chaos pumpkin freckle dragon sleuth session broccoli|\r\n"
        +"OBX|14|TX|tt-0005-14^UrineOutput^WelchAllyn^^||Yes||||||F|||20200508130906||\r\n"
        +"NTE|0||Graph shrimp straw mammoth bloomers step-daughter simple|\r\n"
        ;

        HL7Parser parser = new HL7Parser();

        //there are some invalid messages, hence you will see some invalid exceptions
        List<HL7ParsedRecord> records = parser.process_filestream(
            new ByteArrayInputStream(hl7_message.getBytes())
            ,false);

        assertFalse(records.isEmpty());
        assertTrue(records.size() == 1);

        HL7ParsedRecord rec = records.get(0);
        assertFalse(rec.parsed_status);
        assertTrue(rec.hl7_json == null);
        assertTrue(rec.error_msg != null);
    }

    @Test
    void parseMessageWithValidationError() throws Exception {
        String hl7_message = ""
        +"MSH|^~\\&|SIMHOSP|SFAC|RAPP|RFAC|20200508130835||ADT^A01|266|T|2.3|||AL||44|ASCII\r\n"
        +"EVN|A01|20200508130835|||C001^Forster^Richard^^^Dr^^^DRNBR^PRSNL^^^ORGDR|\r\n"
        +"PID|1|1196424284^^^SIMULATOR MRN^MRN|1196424284^^^SIMULATOR MRN^MRN~5192725445^^^NHSNBR^NHSNMBR||Dowd^Alison^^^Mrs^^CURRENT||19631206000000|F|||98 Attention House^Radiosonde Lane^London^^LB55 7IO^GBR^HOME||076 7360 9349^HOME|||||||||G^Mixed - Other^^^||||||||\r\n"
        +"PD1|||FAMILY PRACTICE^^12345|\r\n"
        +"PV1|1|I|OtherWard^MainRoom^Bed 94^Simulated Hospital^^BED^Main Building^4|28b|||C001^Forster^Richard^^^Dr^^^DRNBR^PRSNL^^^ORGDR|||MED|||||||||7894495905899764246^^^^visitid||||||||||||||||||||||ARRIVED|||20200508130835||\r\n"
        ;

        HL7Parser parser = new HL7Parser();

        List<HL7ParsedRecord> records = parser.process_filestream(
            new ByteArrayInputStream(hl7_message.getBytes())
            ,true);

        assertFalse(records.isEmpty());
        assertTrue(records.size() == 1);

        HL7ParsedRecord rec = records.get(0);
        assertFalse(rec.parsed_status);
        assertTrue(rec.hl7_json == null);
        assertTrue(rec.error_msg != null);
    }

    @Test
    void parseMessageSequenceNumber() throws Exception {
        String hl7_message = ""
        +"MSH|^~\\&|SIMHOSP|SFAC|RAPP|RFAC|20200508130835||ADT^A01|266|T|2.3|||AL||44|ASCII\r\n"
        +"EVN|A01|20200508130835|||C001^Forster^Richard^^^Dr^^^DRNBR^PRSNL^^^ORGDR|\r\n"
        +"PID|1|1196424284^^^SIMULATOR MRN^MRN|1196424284^^^SIMULATOR MRN^MRN~5192725445^^^NHSNBR^NHSNMBR||Dowd^Alison^^^Mrs^^CURRENT||19631206000000|F|||98 Attention House^Radiosonde Lane^London^^LB55 7IO^GBR^HOME||076 7360 9349^HOME|||||||||G^Mixed - Other^^^||||||||\r\n"
        +"PD1|||FAMILY PRACTICE^^12345|\r\n"
        +"PV1|1|I|OtherWard^MainRoom^Bed 94^Simulated Hospital^^BED^Main Building^4|28b|||C001^Forster^Richard^^^Dr^^^DRNBR^PRSNL^^^ORGDR|||MED|||||||||7894495905899764246^^^^visitid||||||||||||||||||||||ARRIVED|||20200508130835||\r\n"
        ;

        HL7Parser parser = new HL7Parser();
        List<HL7ParsedRecord> records = parser.process_filestream(
            new ByteArrayInputStream(hl7_message.getBytes())
            ,true);

        assertFalse(records.isEmpty());
        assertTrue(records.size() == 1);

        HL7ParsedRecord rec = records.get(0);
        assertFalse(rec.parsed_status);
        assertTrue(rec.hl7_json == null);
        assertTrue(rec.error_msg != null);

        assertEquals("2.3", rec.message_version);
        assertEquals(266, rec.message_sequence_num);
        assertEquals("ADT_A01", rec.message_type);
    }

    @Test
    void parseSampleFiles() throws Exception {
        HL7Parser parser = new HL7Parser();

        //there are some invalid messages, hence you will see some invalid exceptions
        List<HL7ParsedRecord> records = parser.handle("src/test/data/hl7/hl7_2-3_samples.txt", false);
        assertFalse(records.isEmpty());
    }
}
