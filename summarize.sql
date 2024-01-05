--
-- Spark SQL query to aggregate and summarize structured logs.
--

SELECT
  'Total number of log entries' AS ANALYSIS,
  COUNT(*) AS VALUE
FROM structured_logs

UNION

SELECT
  CONCAT('Total number of ', log_level, ' log entries') AS ANALYSIS,
  COUNT(*) AS VALUE
FROM structured_logs
GROUP BY log_level

UNION

SELECT
  CONCAT('Total number of log entries in ', log_year) AS ANALYSIS,
  COUNT(*) AS VALUE
FROM structured_logs
GROUP BY log_year

UNION

SELECT
  CONCAT('Total number of log entries on ', log_month, ' month of the year') AS ANALYSIS,
  COUNT(*) AS VALUE
FROM structured_logs
GROUP BY log_month

UNION

SELECT
  CONCAT('Total number of log entries on ', log_day, ' day of the month') AS ANALYSIS,
  COUNT(*) AS VALUE
FROM structured_logs
GROUP BY log_day

UNION

SELECT
  CONCAT('Total number of log entries on ', log_hour, ' hour of the day') AS ANALYSIS,
  COUNT(*) AS VALUE
FROM structured_logs
GROUP BY log_hour

UNION

SELECT
  'Average log entry length' AS ANALYSIS,
  AVG(log_length) AS VALUE
FROM structured_logs

UNION

SELECT
  'Average log message length' AS ANALYSIS,
  AVG(log_message_length) AS VALUE
FROM structured_logs
