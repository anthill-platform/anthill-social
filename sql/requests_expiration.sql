CREATE EVENT `requests_expiration`
ON SCHEDULE EVERY 5 MINUTE STARTS CURRENT_TIMESTAMP
DO
   DELETE FROM `requests`
    WHERE NOW() > `requests`.`request_expire`;