SELECT
    user.id                                                                 AS  user_id
,   timestamp                                                               AS  time_stamp
,   concat(split(url_index1, '[.]')[1], '.', split(url_index1, '[.]')[2])   AS  url_level1
,   url_level2
,   url_level3
,   action                                                                  AS  activity
FROM
    event_master