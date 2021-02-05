{DEFAULT @washout_period = 365}
{DEFAULT @first_occurrence_only = TRUE}
{DEFAULT @time_at_risk_start = 1}
{DEFAULT @time_at_risk_end = 28}
{DEFAULT @cdm_database_schema = CDM_jmdc_v1063.dbo}
{DEFAULT @cohort_database_schema = scratch.dbo}
{DEFAULT @cohort_table = mschuemi_temp}
{DEFAULT @exposure_id = 5664}
{DEFAULT @outcome_ids = 5665}
{DEFAULT @start_date = 20080101}
{DEFAULT @end_date = 20081231}

IF OBJECT_ID('tempdb..#exposure', 'U') IS NOT NULL
  DROP TABLE #exposure;
  
SELECT subject_id,
	FLOOR((YEAR(tar_start_date) - year_of_birth) / 10) AS age_group,
	gender_concept_id,
	tar_start_date,
	CASE 
		WHEN tar_end_date > observation_period_end_date
			THEN observation_period_end_date
		ELSE tar_end_date
		END AS tar_end_date
INTO #exposure
FROM (
	SELECT subject_id,
		DATEADD(DAY, @time_at_risk_start, cohort_start_date) AS tar_start_date,
		CASE 
			WHEN DATEADD(DAY, @time_at_risk_end, cohort_start_date) > CAST('@end_date' AS DATE)
				THEN CAST('@end_date' AS DATE)
			ELSE DATEADD(DAY, @time_at_risk_end, cohort_start_date)
			END AS tar_end_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id = @exposure_id
		AND cohort_start_date >= CAST('@start_date' AS DATE)
		AND cohort_start_date <= CAST('@end_date' AS DATE)
	) exposure
INNER JOIN @cdm_database_schema.person
	ON subject_id = person.person_id
INNER JOIN @cdm_database_schema.observation_period
	ON subject_id = observation_period.person_id
		AND tar_start_date >= DATEADD(DAY, @washout_period, observation_period_start_date)
		AND tar_start_date <= observation_period_end_date;


IF OBJECT_ID('tempdb..#numerator', 'U') IS NOT NULL
  DROP TABLE #numerator;

SELECT age_group,
	gender_concept_id,
	outcome_id,
	COUNT(*) AS outcome_events,
	COUNT(DISTINCT outcome.subject_id) AS outcome_subjects
INTO #numerator
FROM #exposure exposure
{@first_occurrence_only} ? {
INNER JOIN (
	SELECT subject_id,
		cohort_definition_id AS outcome_id,
		MIN(cohort_start_date) AS cohort_start_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@outcome_ids)
	GROUP BY subject_id,
		cohort_definition_id
	) outcome
} : {		
INNER JOIN (
	SELECT subject_id,
		cohort_definition_id AS outcome_id,
		cohort_start_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@outcome_ids)
	) outcome
}
	ON exposure.subject_id = outcome.subject_id
		AND tar_start_date <= outcome.cohort_start_date
		AND tar_end_date >= outcome.cohort_start_date
GROUP BY age_group,
	gender_concept_id,
	outcome_id;
	
IF OBJECT_ID('tempdb..#denominator', 'U') IS NOT NULL
  DROP TABLE #denominator;
  
SELECT age_group,
	gender_concept_id,
	concept_name AS gender,
	SUM(DATEDIFF(DAY, tar_start_date, tar_end_date) + 1) AS days_at_risk,
	COUNT(DISTINCT subject_id) AS exposed_subjects
INTO #denominator
FROM #exposure
INNER JOIN @cdm_database_schema.concept
	ON gender_concept_id = concept_id
GROUP BY age_group,
	gender_concept_id,
	concept_name;

TRUNCATE TABLE #exposure;
DROP TABLE #exposure;
