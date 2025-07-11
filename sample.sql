
CREATE OR REPLACE PROCEDURE Athena_1_get_cpm_data_based_on_campaign_name(
    source_db STRING,
    source_schema STRING,
    target_table STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    sql_command STRING;
    result_msg STRING;
    truncate_command STRING;
BEGIN
    BEGIN
      LET truncate_command := 'TRUNCATE TABLE ' || target_table || ';';
        EXECUTE IMMEDIATE truncate_command;
        LET sql_command := '
       
            INSERT INTO ' || target_table || '
            
                SELECT
                DATA.DATE,
                DATA.CREATIVE_ID_DCM,
                DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME,
                DATA.CREATIVE_NAME_UP,
                COALESCE(SUM(DATA.CLICKS), 0) AS CLICKS,
                COALESCE(SUM(DATA.IMPRESSIONS), 0) AS IMPRESSIONS,
                COALESCE(SUM(DATA.IMPRESSIONS * CPM.TOTAL_CPM), 0) AS TOTAL_SPEND
            FROM
                 ' || source_db || '.' || source_schema || '.ATHENA_EXECUTIVESUMMARY DATA
            LEFT JOIN (
                    SELECT
                        DATE,
                        PLACEMENT_CHANNEL_CAMPAIGN_NAME,
                        COALESCE(SUM(TTD_SPEND), 0) AS TOTAL_SPEND,
                        COALESCE(SUM(IMPRESSIONS), 0) AS TOTAL_IMPRESSIONS,
                        DIV0(TOTAL_SPEND, TOTAL_IMPRESSIONS) AS TOTAL_CPM
                    FROM
                        ' || source_db || '.' || source_schema || '.ATHENA_EXECUTIVESUMMARY
                    WHERE
                        DATE >= DATEADD(day, -45, CURRENT_DATE)
                        AND SITE_NAME IN (''The Trade Desk'', ''DV360'')
                        AND (SITE_NAME != ''The Trade Desk'' OR UNIT_TYPE != ''Native-Custom'')
                    GROUP BY
                        DATE,
                        PLACEMENT_CHANNEL_CAMPAIGN_NAME
                ) CPM ON CPM.DATE = DATA.DATE
                AND CPM.PLACEMENT_CHANNEL_CAMPAIGN_NAME = DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME
                WHERE
                    DATA.DATE >= DATEADD(day, -45, CURRENT_DATE)
                    AND DATA.SITE_NAME IN (''The Trade Desk'', ''DV360'')
                    AND (DATA.SITE_NAME != ''The Trade Desk'' OR DATA.UNIT_TYPE != ''Native-Custom'') 
                GROUP BY
                    DATA.DATE,
                    DATA.CREATIVE_ID_DCM,
                    DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME,
                    DATA.CREATIVE_NAME_UP
                ORDER BY
                    DATA.DATE,
                    DATA.CREATIVE_ID_DCM,
                    DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME,
                    DATA.CREATIVE_NAME_UP;';

        
        EXECUTE IMMEDIATE sql_command;
        

        
        result_msg := 'Success: Data loaded successfully';


    EXCEPTION
        WHEN STATEMENT_ERROR THEN
            result_msg := 'Statement error: ' || ERROR_MESSAGE();

        WHEN OTHER THEN
            result_msg := 'Unexpected error: ' || ERROR_MESSAGE();
    END;

    RETURN result_msg;

END;
$$;

