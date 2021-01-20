{DEFAULT @cohort_database_schema = scratch.dbo}
{DEFAULT @cohort_table = mschuemi_temp}
{DEFAULT @start_date = 20080101}
{DEFAULT @end_date = 20081231}
{DEFAULT @visit_concept_ids = 9202}
{DEFAULT @cohort_id = 1}
{DEFAULT @sample_size_per_month = 50000}
{DEFAULT @washout_period = 365}

DELETE FROM @cohort_database_schema.@cohort_table 
WHERE cohort_definition_id = @cohort_id;

INSERT INTO @cohort_database_schema.@cohort_table (
	cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date
	)
SELECT cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date
FROM (
	SELECT CAST(@cohort_id AS INT) AS cohort_definition_id,
		visit_occurrence.person_id AS subject_id,
		visit_start_date AS cohort_start_date,
		visit_end_date AS cohort_end_date,
		ROW_NUMBER() OVER (
			PARTITION BY YEAR(visit_start_date),
				MONTH(visit_start_date) 
			ORDER BY NEWID()
			) AS rn
	FROM @cdm_database_schema.visit_occurrence
	INNER JOIN @cdm_database_schema.observation_period
		ON visit_occurrence.person_id = observation_period.person_id
			AND visit_start_date >= DATEADD(DAY, @washout_period, observation_period_start_date)
			AND visit_start_date <= observation_period_end_date
	WHERE visit_start_date >= CAST('@start_date' AS DATE)
		AND visit_start_date <= CAST('@end_date' AS DATE)
		AND visit_concept_id IN (@visit_concept_ids)
	) visits_in_period
WHERE rn <= @sample_size_per_month;
