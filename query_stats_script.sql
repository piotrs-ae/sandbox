WITH specified_date AS (
    SELECT
        TO_DATE('YYYY-MM-DD') AS target_date
)
SELECT
    qh.QUERY_ID,
    qh.QUERY_TEXT,
    qh.USER_NAME,
    qh.ROLE_NAME,
    qh.WAREHOUSE_NAME,
    qh.DATABASE_NAME,
    qh.SCHEMA_NAME,
    qh.START_TIME,
    qh.END_TIME,
    qh.TOTAL_ELAPSED_TIME,
    qh.ROWS_PRODUCED,
    qh.BYTES_SCANNED
FROM
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh,
    specified_date
WHERE
    qh.USER_NAME = 'PSIEMINSKI'
    AND DATE(qh.START_TIME) = specified_date.target_date
ORDER BY
    qh.START_TIME;
