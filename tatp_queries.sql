-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile


-- Creation commands
CREATE TABLE Subscriber (s_id INTEGER NOT NULL, sub_nbr VARCHAR(15) NOT NULL, bit_1 SMALLINT, bit_2 SMALLINT, bit_3 SMALLINT, bit_4 SMALLINT, bit_5 SMALLINT, bit_6 SMALLINT, bit_7 SMALLINT, bit_8 SMALLINT, bit_9 SMALLINT, bit_10 SMALLINT, hex_1 SMALLINT, hex_2 SMALLINT, hex_3 SMALLINT, hex_4 SMALLINT, hex_5 SMALLINT, hex_6 SMALLINT, hex_7 SMALLINT, hex_8 SMALLINT, hex_9 SMALLINT, hex_10 SMALLINT, byte2_1 SMALLINT, byte2_2 SMALLINT, byte2_3 SMALLINT, byte2_4 SMALLINT, byte2_5 SMALLINT, byte2_6 SMALLINT, byte2_7 SMALLINT, byte2_8 SMALLINT, byte2_9 SMALLINT, byte2_10 SMALLINT, msc_location INTEGER, vlr_location INTEGER);
CREATE TABLE Access_Info (s_id INTEGER NOT NULL, ai_type SMALLINT NOT NULL, data1 SMALLINT, data2 SMALLINT, data3 CHAR(3), data4 CHAR(5));
CREATE TABLE Special_Facility (s_id INTEGER NOT NULL, sf_type SMALLINT NOT NULL, is_active SMALLINT NOT NULL, error_cntrl SMALLINT, data_a SMALLINT, data_b CHAR(5));
CREATE TABLE Call_Forwarding (s_id INTEGER NOT NULL, sf_type SMALLINT NOT NULL, start_time SMALLINT NOT NULL, end_time SMALLINT, numberx VARCHAR(15));


-- TODO: Population commands


-- Selection commands

/*
 * GET_SUBSCRIBER_DATA
 *  - <s_id> is randomly selected from [1, size of Subscriber table]
 *  - The probability for the transaction to succeed is 100%
 */
SELECT s_id, sub_nbr, bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7, bit_8, bit_9, bit_10, hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7, hex_8, hex_9, hex_10, byte2_1, byte2_2, byte2_3, byte2_4, byte2_5, byte2_6, byte2_7, byte2_8, byte2_9, byte2_10, msc_location, vlr_location FROM Subscriber WHERE s_id = <s_id>;

/*
 * GET_NEW_DESTINATION
 *  - <s_id> is randomly selected from [1, size of Subscriber table]
 *  - <sf_type> is randomly selected from [1, 4]
 *  - <start_time> is randomly selected from {0, 8, 16}
 *  - <end_time> is randomly selected from [1, 24]
 *  - The probability for the transaction to succeed is 23.9%
 */
SELECT cf.numberx FROM Special_Facility AS sf, Call_Forwarding AS cf WHERE (sf.s_id = <s_id> AND sf.sf_type = <sf_type> AND sf.is_active = 1) AND (cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type) AND (cf.start_time <= <start_time> AND <end_time> < cf.end_time);

/*
 * GET_ACCESS_DATA
 *  - <s_id> is randomly selected from [1, size of Subscriber table]
 *  - <ai_type> is randomly selected from [1, 4]
 *  - The probability for the transaction to succeed is 62.5%
 */
SELECT data1, data2, data3, data4 FROM Access_Info WHERE s_id = <s_id> AND ai_type = <ai_type>;

/*
 * UPDATE_LOCATION
 *  - vlr_location is randomly selected from [1, 2^32 - 1]
 *  - <sub_nbr> is the string representation of a random number from [1, size of Subscriber table]
 *  - The probability for the transaction to succeed is 100%
 */
UPDATE Subscriber SET vlr_location = <vlr_location> WHERE sub_nbr = <sub_nbr>;

/*
 * INSERT_CALL_FORWARDING
 *  - <sub_nbr> is the string representation of a random number from [1, size of Subscriber table]
 *  - <sf_type> is randomly selected from [1, 4]
 *  - <start_time> is randomly selected from {0, 8, 16}
 *  - <end_time> is randomly selected from [1, 24]
 *  - <numberx> is a string of length 15 characters. A number between [1, size of Subscriber table] is randomly generated, converted to string representation and padded with the character zero.
 *  - The probability for the transaction to succeed is 31.25%
 */
SELECT <s_id bind subid> FROM Subscriber WHERE sub_nbr = <sub_nbr>;
SELECT <sf_type bind sfid> FROM Special_Facility WHERE s_id = <subid>;
INSERT INTO Call_Forwarding VALUES (<s_id>, <sf_type>, <start_time>, <end_time>, <numberx>);

/*
 * DELETE_CALL_FORWARDING
 *  - <s_id> is randomly selected from [1, size of Subscriber table]
 *  - sf_type is randomly selected from [1, 4]
 *  - start_time is randomly selected from {0, 8, 16}
 *  - The probability for the transaction to succeed is 31.25%
 */
SELECT <s_id bind subid> FROM Subscriber WHERE sub_nbr = <sub_nbr>;
DELETE FROM Call_Forwarding WHERE s_id = <s_id> AND sf_type = <sf_type> AND start_time = <start_time>;