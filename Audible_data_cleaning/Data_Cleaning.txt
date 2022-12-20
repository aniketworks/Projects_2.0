WITH cleaned_table AS
(
  SELECT
  name,
  --author
  SUBSTR(author,STRPOS(author,':')+1,(LENGTH(author)-STRPOS(author,':'))) AS cleaned_author,
  -- narrator
  SUBSTR(narrator,STRPOS(narrator,':')+1,(LENGTH(narrator)-STRPOS(narrator,':'))) AS cleaned_narrator,
  -- time
  IF(STARTS_WITH(time, 'Less'),1,
    IF(time='',0,
      IF(ENDS_WITH(time,'hr') OR ENDS_WITH(time,'hrs'),
        CAST(SPLIT(REGEXP_REPLACE(time, '[^0-9,., ]', ''),' ')[offset(0)] AS INT64)*60,
        IF(LENGTH(time) < 8,
          CAST(SPLIT(REGEXP_REPLACE(time, '[^0-9,., ]', ''),' ')[offset(0)] AS INT64),
          (CAST(SPLIT(REGEXP_REPLACE(time, '[^0-9,., ]', ''),' ')[offset(0)] AS INT64)*60 +
          CAST(SPLIT(REGEXP_REPLACE(time, '[^0-9,., ]', ''),' ')[offset(3)] AS INT64))
          )
        )
      )
    )AS cleaned_time,
  releasedate,
  -- language
  INITCAP(language) AS cleaned_language,
  --price column
  CAST(IF(price='Free','0.0',REGEXP_REPLACE(price, ',', '')) AS FLOAT64) AS cleaned_price,
  --Rating created from stars column
  IF(stars ='Not rated yet',0.0,CAST(SPLIT(REGEXP_REPLACE(stars, '[^0-9,., ]', ''),' ')[offset(0)] AS FLOAT64)) AS rating,
  --Reviews count created from stars column
  IF(stars ='Not rated yet',0.0,CAST(SPLIT(REGEXP_REPLACE(stars, '[^0-9 ]', ''),' ')[offset(4)] AS FLOAT64)) AS reviews_count
FROM
  `portfolio-370909.Audible.data`
)
-- This group by is used to remove duplicate entries from the cleaned table
SELECT
  * EXCEPT(reviews_count),
  MAX(reviews_count) AS cleaned_reviews_count
FROM
  cleaned_table
GROUP BY
  name, cleaned_author, cleaned_narrator, cleaned_time, releasedate, cleaned_language, cleaned_price, rating