SELECT
    time_bucket
,   url_level1
,   url_level2
,   action                      AS  activity
,   COUNT(event_id)             AS  activity_count
,   COUNT(DISTINCT  user.id)    AS  user_count
FROM
    (
        SELECT
            *
        ,   concat(split(url_index1, '[.]')[1], '.', split(url_index1, '[.]')[2])               AS  url_level1
        ,   FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp, 'dd/MM/yyyy HH:mm:ss'), 'yyyyMMddHH')       AS  time_bucket
        FROM
            event_master
    )
GROUP BY
    time_bucket
,   url_level1
,   url_level2
,   action