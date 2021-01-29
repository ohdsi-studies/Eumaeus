{DEFAULT @washout_period = 365}
{DEFAULT @first_occurrence_only = TRUE}
{DEFAULT @cdm_database_schema = cdm}
{DEFAULT @cohort_database_schema = scratch_mschuemi2}
{DEFAULT @cohort_table = mschuemi_vac_surv_mdcd}
{DEFAULT @cohort_id = 197610}

IF OBJECT_ID('tempdb..#obs_end', 'U') IS NOT NULL
  DROP TABLE #obs_end;

SELECT MAX(observation_period_end_date) AS obs_end_date
INTO #obs_end
FROM cdm.observation_period;


IF OBJECT_ID('tempdb..#numerator', 'U') IS NOT NULL
  DROP TABLE #numerator;

SELECT DATEDIFF(MONTH, cohort_start_date, obs_end_date) AS months_to_db_end,
	FLOOR((YEAR(cohort_start_date) - year_of_birth) / 10) AS age_group,
	gender_concept_id,
	COUNT(*) AS cohort_count
INTO #numerator 
FROM (
{@first_occurrence_only} ? {
	SELECT subject_id,
		MIN(cohort_start_date) AS cohort_start_date,
		MIN(cohort_end_date) AS cohort_end_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id = @cohort_id
	GROUP BY subject_id
} : {	
	SELECT subject_id,
		cohort_start_date,
		cohort_end_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id = @cohort_id
}
	) cohort
CROSS JOIN #obs_end
INNER JOIN @cdm_database_schema.person
	ON subject_id = person.person_id
INNER JOIN @cdm_database_schema.observation_period
	ON observation_period.person_id = person.person_id
		AND DATEADD(DAY, @washout_period, observation_period_start_date) <= cohort_start_date
		AND observation_period_end_date >= cohort_start_date
WHERE cohort_start_date <= obs_end_date
GROUP BY DATEDIFF(MONTH, cohort_start_date, obs_end_date),
	FLOOR((YEAR(cohort_start_date) - year_of_birth) / 10),
	gender_concept_id;


IF OBJECT_ID('tempdb..#months', 'U') IS NOT NULL
  DROP TABLE #months;
  
CREATE TABLE #months (months_to_db_end INT);

INSERT INTO #months (months_to_db_end)
VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12);


IF OBJECT_ID('tempdb..#denominator', 'U') IS NOT NULL
  DROP TABLE #denominator;
  
SELECT months_to_db_end,
	age_group,
	gender_concept_id,
	SUM(CAST(DATEDIFF(DAY, start_date, end_date) AS BIGINT)) / 365.25 AS person_years
INTO #denominator
FROM (
{@first_occurrence_only} ? {
	SELECT months_to_db_end,
		age_group,
		gender_concept_id,
		CASE
			WHEN cohort_start_date IS NOT NULL THEN 
				CASE
					WHEN cohort_start_date < start_date THEN end_date
					ELSE cohort_start_date
				END
			ELSE start_date
		END AS start_date,
		end_date
	FROM (
}
		SELECT person.person_id,
			months_to_db_end,
			FLOOR((YEAR(DATEADD(MONTH, -months_to_db_end, observation_period_end_date)) - year_of_birth) / 10) AS age_group,
			gender_concept_id,
			CASE 
				WHEN observation_period_start_date > DATEADD(MONTH, -months_to_db_end, observation_period_end_date) THEN observation_period_start_date
				ELSE DATEADD(MONTH, -months_to_db_end, observation_period_end_date)
			END AS start_date,
			CASE 
				WHEN observation_period_end_date < DATEADD(MONTH, 1 - months_to_db_end, observation_period_end_date) THEN observation_period_end_date
				ELSE DATEADD(MONTH, 1 - months_to_db_end, observation_period_end_date)
			END AS end_date
		FROM (
			SELECT person_id,
				DATEADD(DAY, @washout_period, observation_period_start_date) AS observation_period_start_date,
				observation_period_end_date
			FROM @cdm_database_schema.observation_period
			WHERE DATEADD(DAY, @washout_period, observation_period_start_date) < observation_period_end_date
		) trunc_op
		CROSS JOIN #obs_end
		INNER JOIN #months
			ON observation_period_start_date <= DATEADD(MONTH, 1 - months_to_db_end, obs_end_date)
				AND observation_period_end_date >= DATEADD(MONTH, - months_to_db_end, obs_end_date)
		INNER JOIN @cdm_database_schema.person
			ON trunc_op.person_id = person.person_id
{@first_occurrence_only} ? {
		) time_spans_1
	LEFT JOIN (
		SELECT subject_id,
			MIN(cohort_start_date) AS cohort_start_date
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id = @cohort_id
		GROUP BY subject_id
		) cohort
	ON subject_id = person_id
		AND cohort_start_date < end_date
}
	) time_spans_2
GROUP BY months_to_db_end,
	age_group,
	gender_concept_id;

IF OBJECT_ID('tempdb..#rates_summary', 'U') IS NOT NULL
  DROP TABLE #rates_summary;

SELECT denominator.age_group,
	concept_name AS gender,
	denominator.months_to_db_end,
	CASE 
		WHEN numerator.cohort_count IS NOT NULL THEN numerator.cohort_count
		ELSE CAST(0 AS INT)
	END AS cohort_count,
	person_years,
	obs_end_date
INTO #rates_summary
FROM #denominator denominator
CROSS JOIN #obs_end
INNER JOIN @cdm_database_schema.concept
	ON denominator.gender_concept_id = concept_id
LEFT JOIN #numerator numerator
	ON denominator.age_group = numerator.age_group
		AND denominator.gender_concept_id = numerator.gender_concept_id
		AND denominator.months_to_db_end = numerator.months_to_db_end;

TRUNCATE TABLE #numerator;
DROP TABLE #numerator;

TRUNCATE TABLE #denominator;
DROP TABLE #denominator;

TRUNCATE TABLE #months;
DROP TABLE #months;

