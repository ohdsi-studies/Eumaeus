{DEFAULT @cohort_database_schema = scratch.dbo}
{DEFAULT @cohort_table = mschuemi_temp}
{DEFAULT @exposure_id = 1}
{DEFAULT @first_exposure_id = 2}
{DEFAULT @second_exposure_id = 3}
{DEFAULT @both_exposure_id = 4}

DELETE FROM @cohort_database_schema.@cohort_table 
WHERE cohort_definition_id IN (@first_exposure_id, @second_exposure_id, @both_exposure_id);

INSERT INTO @cohort_database_schema.@cohort_table (
	cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date
	)
SELECT CASE 
		WHEN seq_id = 1
			THEN CAST(@first_exposure_id AS INT)
		ELSE CAST(@second_exposure_id AS INT)
		END AS cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date
FROM (
	SELECT subject_id,
		cohort_start_date,
		cohort_end_date,
		ROW_NUMBER() OVER (
			PARTITION BY subject_id
			ORDER BY cohort_start_date
			) AS seq_id
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id = @exposure_id
	) tmp
WHERE seq_id = 1
	OR seq_id = 2;
	
INSERT INTO @cohort_database_schema.@cohort_table (
	cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date
	)
SELECT CAST(@both_exposure_id AS INT) AS cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date
FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id = @first_exposure_id
	OR cohort_definition_id = @second_exposure_id;

