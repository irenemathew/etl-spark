SELECT
t.month_bucket,
t.trade,
t.search_frequency
FROM
(
SELECT
    *
,   ROW_NUMBER() OVER(
        PARTITION BY
            month_bucket
        ORDER BY
            search_frequency    DESC
    )       AS  rank
FROM
    (
        SELECT
            month_bucket
        ,   trade
        ,   COUNT(user_id) OVER(
                PARTITION BY
                    month_bucket
                ,   trade
            )               AS  search_frequency
        FROM
            (
                SELECT
                    user.id                                                                     AS  user_id
                ,   url_level3                                                                  AS  trade
                ,   FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp, 'dd/MM/yyyy HH:mm:ss'), 'yyyyMM')   AS  month_bucket
                FROM
                    event_master
                WHERE
                    locate('/find/', cleansed_url)  >   0
            )
    )
    ) t
WHERE
    t.rank    =  1
ORDER BY t.month_bucket