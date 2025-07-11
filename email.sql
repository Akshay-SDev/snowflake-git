
CREATE OR REPLACE PROCEDURE SEND_EMAIL_NOTIFICATION_JS(
    subject STRING,
    body STRING,
    recipients STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    var integration = 'my_email_int';  
    var subject = arguments[0];
    var body = arguments[1];
    var recipients = arguments[2];

    snowflake.execute({
        sqlText: `CALL SYSTEM$SEND_EMAIL(?, ?, ?, ?)`,
        binds: [integration, recipients, subject, body]
    });

    return 'Email sent successfully to: ' + recipients;

} catch (err) {
    return ' Error occurred: ' + err.message;
}
$$;
