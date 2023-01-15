/*
Questions that we can answer with this dataset

1. Top-N Universities in the World
2. Top-N Universities by Region
3. Top-N Universities by Type
4. University Count By Region, Country
5. University Count By Region, Country - Remove Duplicates

*/

--- Preview University Ranking Data ---
SELECT * 
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_csv" limit 10;

--- Data Quality Issues ---
--- 1. Top 5 universities ---
SELECT * 
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_csv"
WHERE rank_display < 5
ORDER BY year, rank_display;

--- Distinct Rank Display Values Sorted by Length
SELECT DISTINCT rank_display, length(rank_display) as rank_length
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_csv"
ORDER BY length(rank_display) desc;


--- 2. Top 5 universities - String ---
SELECT * 
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_csv"
WHERE rank_display in ('1','2','3','4','5')
ORDER BY year, rank_display;

--- 3. Check Column Mapping - Cross Check Athena Output and CSV File Content. Look for double quotes! ---
SELECT * 
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_csv"
WHERE university like '%Nanyang Technological University%'
ORDER BY year, rank_display;

--- 4. Convert rank_display to a number using type casting
--- Data Type Conversion
---    https://docs.aws.amazon.com/redshift/latest/dg/r_Data_type_formatting.html
SELECT cast(rank_display as int)
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_csv";

--- 5. Handle range of values in rank_display 
--- split values based on "-" and return the first value in the range
--- Transform data to required format and type
---   https://docs.aws.amazon.com/redshift/latest/dg/SPLIT_PART.html
SELECT cast(split_part(rank_display,'-',1) as int)
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_csv";

--- 6. Handling Null Values coalesce and try syntax
SELECT coalesce(try(cast(split_part(rank_display,'-',1) as int)), 9999) as n_rank
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_csv";

--- 7. Query by numeric rank ---
SELECT *
FROM
(SELECT university, year, 
       rank_display, 
       coalesce(try(cast(split_part(rank_display,'-',1) as int)), 9999) as n_rank,
       score, country, city, region, type,
       research_output, student_faculty_ratio, international_students,
       size, faculty_count
FROM "learn_by_doing"."university_ranking_csv")
WHERE n_rank < 6
order by year, n_rank

--- 8. More Data Quality Issues. 
---  comma ',' and decimal points '.' as thousands separator 
---  international students, faculty count ---
SELECT * 
FROM "learn_by_doing"."university_ranking_csv" 
WHERE country = 'Norway';


--- 9. Regular Expression Based Cleanup
---   REGEXP_REPLACE function
---   https://docs.aws.amazon.com/redshift/latest/dg/REGEXP_REPLACE.html

SELECT regexp_replace(international_students,'[.,]','') as international_students,
       regexp_replace(faculty_count,'[.,]','') as faculty_count
FROM "learn_by_doing"."university_ranking_csv" 
WHERE country = 'Norway';

--- 10.1 Type Conversion --- 
SELECT 
    COALESCE(TRY(CAST(regexp_replace(international_students,'[.,]','') AS int)),-1) as international_students,
    COALESCE(TRY(CAST(regexp_replace(faculty_count,'[.,]','') AS int)),-1) as faculty_count
FROM "learn_by_doing"."university_ranking_csv" 
WHERE country = 'Norway';

--- 10.2 Complete Query ---
SELECT university,
       COALESCE(TRY(CAST(year AS int)),9999) AS year, 
       rank_display, 
       COALESCE(TRY(CAST(split_part(rank_display,'-',1) AS int)),9999) AS n_rank,
       COALESCE(TRY(CAST(score AS double)),-1) AS score, 
       country, city, region, type,
       research_output, 
       COALESCE(TRY(CAST(student_faculty_ratio AS double)),-1) AS student_faculty_ratio,
       COALESCE(TRY(CAST(regexp_replace(international_students,'[.,]','') AS int)),-1) as international_students,
       size, 
       COALESCE(TRY(CAST(regexp_replace(faculty_count,'[.,]','') AS int)),-1) as faculty_count
FROM "learn_by_doing"."university_ranking_csv"
order by year, n_rank;



--- 11. Convert to appropriate type using a view
---   Simplify querying with a view
---   This time we will create an athena view ---
CREATE OR REPLACE VIEW university_ranking_view AS
SELECT university,
       COALESCE(TRY(CAST(year AS int)),9999) AS year, 
       rank_display, 
       COALESCE(TRY(CAST(split_part(rank_display,'-',1) AS int)),9999) AS n_rank,
       COALESCE(TRY(CAST(score AS double)),-1) AS score, 
       country, city, region, type,
       research_output, 
       COALESCE(TRY(CAST(student_faculty_ratio AS double)),-1) AS student_faculty_ratio,
       COALESCE(TRY(CAST(regexp_replace(international_students,'[.,]','') AS int)),-1) as international_students,
       size, 
       COALESCE(TRY(CAST(regexp_replace(faculty_count,'[.,]','') AS int)),-1) as faculty_count
FROM "learn_by_doing"."university_ranking_csv"
order by year, n_rank;

--- Query with View ---
--- 12.1. View data
SELECT * 
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_view" limit 10;

--- 12.2. Top 5 universities ---
SELECT year, university, n_rank, country, region, score, type
FROM "learn_by_doing"."university_ranking_view" 
WHERE n_rank < 6
ORDER BY year, n_rank;

--- 12.3. Top 5 universities in each region by year --- 
SELECT * 
FROM (SELECT year, 
       region, 
       DENSE_RANK() OVER(PARTITION BY year, region ORDER BY n_rank) AS region_rank,
       n_rank, university, country, score, type
      FROM "learn_by_doing"."university_ranking_view")
WHERE region_rank < 6
ORDER BY year, region, region_rank;

--- 12.4 University Count by Region ---
SELECT region, count(*) as count
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_view"
GROUP BY region
ORDER BY count;

--- 12.5 University Count by Region - Remove Duplicates ---
SELECT region, count(*) as count
FROM (
    SELECT region, university
    FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_view"
    GROUP BY region, university
    )
GROUP BY region
ORDER BY count;

--- Query using Parquet Clean Data ---
--- 1. View data
SELECT * 
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_parquet" limit 10;

--- 2. Top 5 universities ---
SELECT year, university, n_rank, country, region, score, type
FROM "learn_by_doing"."university_ranking_parquet" 
WHERE n_rank < 6
ORDER BY year, n_rank;

--- 3. Top 5 universities in each region by year --- 
SELECT * 
FROM (SELECT year, 
       region, 
       DENSE_RANK() OVER(PARTITION BY year, region ORDER BY n_rank) AS region_rank,
       n_rank, university, country, score, type
      FROM "learn_by_doing"."university_ranking_parquet")
WHERE region_rank < 6
ORDER BY year, region, region_rank;

--- 4. University Count by Region ---
SELECT region, count(*) as count
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_parquet"
GROUP BY region
ORDER BY count;

--- 5. University Count by Region - Remove Duplicates ---
SELECT region, count(*) as count
FROM (
    SELECT region, university
    FROM "AwsDataCatalog"."learn_by_doing"."university_ranking_parquet"
    GROUP BY region, university
    )
GROUP BY region
ORDER BY count;


--- Query using Glue-ETL Transformed Clean CSV Data ---
SELECT * 
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking" limit 10;

--- 12.2. Top 5 universities ---
SELECT * 
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking"
WHERE n_rank < 5
ORDER BY year, n_rank;

--- 12.3. Top 3 universities in each region by year --- 
SELECT year, region, d_rank, n_rank, university, type, country
FROM (
    SELECT university, year, n_rank, country, region, score, type,
    DENSE_RANK() OVER(PARTITION BY year, region ORDER BY n_rank) AS d_rank
    FROM "learn_by_doing"."university_ranking")
WHERE d_rank < 4
ORDER BY year, region, d_rank;

--- 12.4 University Count by Region ---
SELECT region, count(*) as count
FROM "AwsDataCatalog"."learn_by_doing"."university_ranking"
GROUP BY region
ORDER BY count;

--- 12.5 University Count by Region - Remove Duplicates ---
SELECT region, count(*) as count
FROM (
    SELECT region, university
    FROM "AwsDataCatalog"."learn_by_doing"."university_ranking"
    GROUP BY region, university
    )
GROUP BY region
ORDER BY count;
