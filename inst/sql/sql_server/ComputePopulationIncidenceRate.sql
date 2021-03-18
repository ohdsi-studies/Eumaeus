{DEFAULT @washout_period = 365}
{DEFAULT @first_occurrence_only = TRUE}
{DEFAULT @cdm_database_schema = cdm}
{DEFAULT @cohort_database_schema = scratch_mschuemi2}
{DEFAULT @cohort_table = mschuemi_vac_surv_mdcd}
{DEFAULT @cohort_ids = 4032787, 438730, 443721, 432594}
{DEFAULT @start_date = 20080101}
{DEFAULT @end_date = 20081231}
{DEFAULT @rate_type = visit-based}
{DEFAULT @visit_concept_ids = 9202}
{DEFAULT @tar_start = 1}
{DEFAULT @tar_end = 28}
{DEFAULT @exposure_id = 21184}


IF OBJECT_ID('tempdb..#risk_window', 'U') IS NOT NULL
  DROP TABLE #risk_window;

-- Create risk windows considering observation period, washout period, and specified start and end date 
SELECT FLOOR((YEAR(start_date) - year_of_birth) / 10) AS age_group,
	gender_concept_id,
	person.person_id,
	start_date,
	end_date
INTO #risk_window
FROM (
{@rate_type == 'visit-based'} ? {
	SELECT visit_occurrence.person_id,
		DATEADD(DAY, @tar_start, visit_start_date) AS start_date,
		CASE
			WHEN end_date < DATEADD(DAY, @tar_end, visit_start_date) THEN end_date
			ELSE DATEADD(DAY, @tar_end, visit_start_date)
		END AS end_date,
		ROW_NUMBER() OVER (PARTITION BY visit_occurrence.person_id ORDER BY NEWID()) AS rn
	FROM (
}
{@rate_type == 'exposure-based'} ? {
	SELECT exposure.subject_id AS person_id,
		DATEADD(DAY, @tar_start, cohort_start_date) AS start_date,
		CASE
			WHEN end_date < DATEADD(DAY, @tar_end, cohort_start_date) THEN end_date
			ELSE DATEADD(DAY, @tar_end, cohort_start_date)
		END AS end_date
	FROM (
}
	SELECT person_id,
		CASE
			WHEN CAST('@start_date' AS DATE) > DATEADD(DAY, @washout_period, observation_period_start_date) THEN CAST('@start_date' AS DATE)
			ELSE DATEADD(DAY, @washout_period, observation_period_start_date)
		END AS start_date,
		CASE
			WHEN CAST('@end_date' AS DATE) < observation_period_end_date THEN CAST('@end_date' AS DATE)
			ELSE observation_period_end_date
		END AS end_date 
	FROM @cdm_database_schema.observation_period
{@rate_type == 'visit-based'} ? {
	) tmp
	INNER JOIN @cdm_database_schema.visit_occurrence
		ON tmp.person_id = visit_occurrence.person_id 
			AND DATEADD(DAY, @tar_start, visit_start_date) >= start_date
			AND DATEADD(DAY, @tar_start, visit_start_date) <= end_date
	WHERE visit_concept_id IN (@visit_concept_ids)
}
{@rate_type == 'exposure-based'} ? {
	) tmp
	INNER JOIN @cohort_database_schema.@cohort_table exposure
		ON tmp.person_id = exposure.subject_id
			AND DATEADD(DAY, @tar_start, cohort_start_date) >= start_date
			AND DATEADD(DAY, @tar_start, cohort_start_date) <= end_date
	WHERE cohort_definition_id = (@exposure_id)
}
) trunc_obs_periods
INNER JOIN @cdm_database_schema.person
	ON trunc_obs_periods.person_id = person.person_id
WHERE start_date <= end_date
{@rate_type == 'visit-based'} ? {
	AND rn = 1
}
;

-- Count events stratified by age and gender
IF OBJECT_ID('tempdb..#event_count', 'U') IS NOT NULL
  DROP TABLE #event_count;

SELECT cohort_definition_id AS cohort_id,
	age_group,
	gender_concept_id,
	COUNT(*) AS cohort_count,
	COUNT(DISTINCT risk_window.person_id) AS cohort_person_count,
	SUM(DATEDIFF(DAY, outcome.cohort_start_date, risk_window.end_date)) AS days_to_censor
INTO #event_count
FROM (
{@first_occurrence_only} ? {
	SELECT subject_id,
		cohort_definition_id,
		MIN(cohort_start_date) AS cohort_start_date,
		MIN(cohort_end_date) AS cohort_end_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@cohort_ids)
	GROUP BY subject_id,
		cohort_definition_id
} : {	
	SELECT subject_id,
		cohort_definition_id
		cohort_start_date,
		cohort_end_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@cohort_ids)
}
) outcome
INNER JOIN #risk_window risk_window
	ON outcome.subject_id = risk_window.person_id
		AND outcome.cohort_start_date >= risk_window.start_date
		AND outcome.cohort_start_date <= risk_window.end_date
GROUP BY cohort_definition_id,
	age_group,
	gender_concept_id;

-- Count background time (time at risk disregarding events)
IF OBJECT_ID('tempdb..#background_time', 'U') IS NOT NULL
  DROP TABLE #background_time;

SELECT age_group,
	gender_concept_id,
	SUM(CAST(DATEDIFF(DAY, start_date, end_date) + 1 AS BIGINT)) AS day_count,
	COUNT(DISTINCT person_id) AS person_count
INTO #background_time
FROM #risk_window risk_window
GROUP BY age_group,
	gender_concept_id;

-- Compute numerator and denominator
IF OBJECT_ID('tempdb..#rates', 'U') IS NOT NULL
  DROP TABLE #rates;

SELECT all_cohorts.cohort_id AS outcome_id,
	background_time.age_group,
	concept_name AS gender,
	CASE 
		WHEN cohort_count IS NULL THEN 0
		ELSE cohort_count
	END AS cohort_count,
	CASE 
		WHEN cohort_person_count IS NULL THEN 0
		ELSE cohort_person_count
	END AS cohort_person_count,
	CASE
		WHEN days_to_censor IS NULL THEN day_count / 365.25
		ELSE (day_count - days_to_censor) / 365.25
	END AS person_years,
	person_count
INTO #rates
FROM #background_time background_time
CROSS JOIN (
	SELECT DISTINCT cohort_id 
	FROM #event_count
) all_cohorts
INNER JOIN @cdm_database_schema.concept
	ON concept_id = background_time.gender_concept_id
LEFT JOIN #event_count event_count
	ON background_time.age_group = event_count.age_group
		AND background_time.gender_concept_id = event_count.gender_concept_id
		AND all_cohorts.cohort_id = event_count.cohort_id;

TRUNCATE TABLE #risk_window;
DROP TABLE #risk_window;

TRUNCATE TABLE #event_count;
DROP TABLE #event_count;

TRUNCATE TABLE #background_time;
DROP TABLE #background_time;
