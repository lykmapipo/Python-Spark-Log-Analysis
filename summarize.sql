--
-- Spark SQL query to aggregate and summarize structured logs.
--
SELECT
  'Total number of log entries' AS ANALYSIS,
  COUNT(*) AS VALUE
FROM structured_logs

UNION

SELECT
  'Average log entry length' AS ANALYSIS,
  AVG(log_length) AS VALUE
FROM structured_logs

UNION

SELECT
  CONCAT('Total number of ', log_level, ' log entries') AS ANALYSIS,
  COUNT(*) AS VALUE
FROM structured_logs
GROUP BY log_level

UNION

SELECT
  'Average log message length' AS ANALYSIS,
  AVG(log_message_length) AS VALUE
FROM structured_logs
