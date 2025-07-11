CREATE OR REPLACE PROCEDURE Athena_2_get_creative_start_date_based_on_impressions(
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
        -- Build the dynamic SQL
        LET truncate_command := 'TRUNCATE TABLE ' || target_table || ';';
        EXECUTE IMMEDIATE truncate_command;
        LET sql_command := '
            INSERT INTO ' || target_table || '
            WITH CREATIVE_STARTS AS (
                SELECT
                    CREATIVE_ID_DCM,
                    PLACEMENT_CHANNEL_CAMPAIGN_NAME,
                    MIN(DATE) AS CREATIVE_START_DATE
                FROM
                    ' || source_db || '.' || source_schema || '.ATHENA_EXECUTIVESUMMARY
                WHERE
                    IMPRESSIONS > 50
                    AND DATE >= DATEADD(day, -45, CURRENT_DATE)
                    AND CHANNEL IN (''Display'', ''Online Digital Video'', ''Native'', ''Paid Social'', ''CTV'')
                GROUP BY
                    CREATIVE_ID_DCM,
                    PLACEMENT_CHANNEL_CAMPAIGN_NAME
            ),
            CPM AS (
                SELECT
                    DATE,
                    PLACEMENT_CHANNEL_CAMPAIGN_NAME,
                    COALESCE(SUM(CASE WHEN CHANNEL = ''Paid Social'' THEN SPEND ELSE TTD_SPEND END), 0) AS TOTAL_SPEND,
                    COALESCE(SUM(IMPRESSIONS), 0) AS TOTAL_IMPRESSIONS,
                    DIV0(SUM(CASE WHEN CHANNEL = ''Paid Social'' THEN SPEND ELSE TTD_SPEND END), SUM(IMPRESSIONS)) AS TOTAL_CPM
                FROM
                    ' || source_db || '.' || source_schema || '.ATHENA_EXECUTIVESUMMARY
                WHERE
                    DATE >= DATEADD(day, -45, CURRENT_DATE)
                    AND CHANNEL IN (''Display'', ''Online Digital Video'', ''Native'', ''Paid Social'', ''CTV'')
                GROUP BY
                    DATE,
                    PLACEMENT_CHANNEL_CAMPAIGN_NAME
            )
            SELECT
                DATA.DATE,
                COALESCE(DATA.CREATIVE_ID_DCM, ''-'') AS CREATIVE_ID_DCM,
                DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME,
                DATA.CREATIVE_NAME_UP,
                CASE WHEN DATA.CHANNEL = ''Paid Social'' THEN COALESCE(CONCAT(DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME, ''_'', DATA.CREATIVE_NAME_UP), ''-'') ELSE COALESCE(CONCAT(DATA.CREATIVE_ID_DCM, ''_'', DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME, ''_'', DATA.CREATIVE_NAME_UP), ''-'') END AS PK,
                COALESCE(SUM(DATA.CLICKS), 0) AS CLICKS,
                COALESCE(SUM(DATA.IMPRESSIONS), 0) AS IMPRESSIONS,
                COALESCE(SUM(DATA.IMPRESSIONS * CPM.TOTAL_CPM), 0) AS TOTAL_SPEND,
                CS.CREATIVE_START_DATE
            FROM
                ' || source_db || '.' || source_schema || '.ATHENA_EXECUTIVESUMMARY DATA
            INNER JOIN CREATIVE_STARTS CS
                ON COALESCE(CS.CREATIVE_ID_DCM, ''-'') = COALESCE(DATA.CREATIVE_ID_DCM, ''-'')
                AND CS.PLACEMENT_CHANNEL_CAMPAIGN_NAME = DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME
            LEFT JOIN CPM
                ON CPM.DATE = DATA.DATE
                AND CPM.PLACEMENT_CHANNEL_CAMPAIGN_NAME = DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME
            WHERE
                DATA.DATE >= DATEADD(day, -45, CURRENT_DATE)
                AND DATA.CHANNEL IN (''Display'', ''Online Digital Video'', ''Native'', ''Paid Social'', ''CTV'')
            GROUP BY
                DATA.DATE,
                DATA.CREATIVE_ID_DCM,
                DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME,
                DATA.CREATIVE_NAME_UP,
                DATA.CHANNEL,
                CS.CREATIVE_START_DATE
            ORDER BY
                DATA.DATE,
                DATA.CREATIVE_ID_DCM,
                DATA.PLACEMENT_CHANNEL_CAMPAIGN_NAME,
                DATA.CREATIVE_NAME_UP;';

        -- Execute the dynamic SQL
        EXECUTE IMMEDIATE sql_command;

CALL SEND_EMAIL_NOTIFICATION_JS(
  'Test success',
  'data loaded sucessfully',
  'Ullas.Basavaraju@galepartners.com'
);


        -- Set success message
        result_msg := 'Success: Data Loaded Successfully';

    EXCEPTION
        WHEN STATEMENT_ERROR THEN
            result_msg := 'Statement error: ' || LAST_ERROR_MESSAGE();
            CALL SEND_EMAIL_NOTIFICATION_JS(
  'Test failed',
  'statement error   ',
  'Ullas.Basavaraju@galepartners.com'
);


        WHEN OTHER THEN
            result_msg := 'Unexpected error: ' || LAST_ERROR_MESSAGE();
            CALL SEND_EMAIL_NOTIFICATION_JS(
  'Test failef',
  ' Unexpected error',
  'Ullas.Basavaraju@galepartners.com'
);

    END;

    RETURN result_msg;
END;
$$;
