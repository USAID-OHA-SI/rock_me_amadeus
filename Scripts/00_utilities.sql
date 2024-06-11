/* Procedure for Type ID Lookup */

DELIMITER //

DROP PROCEDURE IF EXISTS type_ids//

CREATE PROCEDURE type_ids(IN tbln VARCHAR(255), IN coln VARCHAR(255))
BEGIN
	SELECT id_type_lookup as lookup_id, id_type_desc as lookup_desc from type_id_lookup 
    WHERE table_name = tbln and column_name = coln
    ORDER BY lookup_id ASC;
END//

DELIMITER ;

CALL type_ids("FORM", "FORM_ID");
CALL type_ids("MEDICATION", "REGIMEN_ID");