SHOW TABLES
FROM
SCHEMA "data-engineer-database"."mauroalberelli_coderhouse";

DROP TABLE IF EXISTS mauroalberelli_coderhouse.stage_starwars_table;

CREATE TABLE stage_starwars_table(
    id INT IDENTITY(1,1),
    primary KEY(id),
	Name	        VARCHAR(200)
,   Gender          VARCHAR(50)
,   Birth_year      VARCHAR(50)
,   Eye_color       VARCHAR(50)
,   Skin_color      VARCHAR(50)
,   Hair_color      VARCHAR(50)
,   Mass            VARCHAR(50)
,   Height          VARCHAR(50)
,   Homeworld       VARCHAR(50)


);
SELECT  
*
FROM mauroalberelli_coderhouse.stage_starwars_table ;