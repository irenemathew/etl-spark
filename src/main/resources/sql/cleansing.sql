SELECT
    *
,   split(cleansed_url, '[/]')[2]                                                       AS  url_index1
,   split(cleansed_url, '[/]')[3]                                                       AS  url_level2
,   split(cleansed_url, '[/]')[4]                                                       AS  url_level3
FROM
    (
        SELECT
            *
        ,   CASE
                WHEN locate('://', url) == 0
                THEN concat('https://', '', url)
                ELSE url
            END     AS  cleansed_url
        FROM
            event
    )