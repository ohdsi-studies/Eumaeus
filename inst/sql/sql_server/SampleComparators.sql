{DEFAULT @cdm_database_schema = CDM_jmdc_v1063.dbo}
{DEFAULT @cohort_database_schema = scratch.dbo}
{DEFAULT @cohort_table = mschuemi_temp}
{DEFAULT @start_date = 20080101}
{DEFAULT @end_date = 20081231}
{DEFAULT @visit_concept_ids = 9202}
{DEFAULT @target_sample_cohort_id = 1}
{DEFAULT @comparator_sample_cohort_id = 2}
{DEFAULT @crude_comparator_sample_cohort_id = 3}
{DEFAULT @exclusion_cohort_id = 123}
{DEFAULT @exposure_id = 5664}
{DEFAULT @washout_period = 365}
{DEFAULT @multiplier = 2}
{DEFAULT @max_target_per_month = 350000}

DELETE FROM @cohort_database_schema.@cohort_table 
WHERE cohort_definition_id = @target_sample_cohort_id;

INSERT INTO @cohort_database_schema.@cohort_table (
	cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date
	)
SELECT CAST(@target_sample_cohort_id AS INT) AS cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date
FROM (
	SELECT subject_id,
		cohort_start_date,
		cohort_end_date,
		ROW_NUMBER() OVER (
			PARTITION BY YEAR(cohort_start_date),
			MONTH(cohort_start_date) ORDER BY NEWID()
			) AS rn
	FROM @cohort_database_schema.@cohort_table
	INNER JOIN @cdm_database_schema.observation_period
	ON subject_id = person_id
		AND cohort_start_date >= DATEADD(DAY, @washout_period, observation_period_start_date)
		AND cohort_start_date <= observation_period_end_date
	WHERE cohort_definition_id = @exposure_id
		AND cohort_start_date >= CAST('@start_date' AS DATE)
		AND cohort_start_date <= CAST('@end_date' AS DATE)
	) tmp
WHERE rn <= @max_target_per_month;

IF OBJECT_ID('tempdb..#stratified_counts', 'U') IS NOT NULL
  DROP TABLE #stratified_counts;

SELECT COUNT(*) AS target_cohort_count,
	YEAR(cohort_start_date) AS cohort_year,
	MONTH(cohort_start_date) AS cohort_month,
	FLOOR((YEAR(cohort_start_date) - year_of_birth) / 10) AS cohort_age_group,
	gender_concept_id AS cohort_gender_concept_id
INTO #stratified_counts
FROM @cohort_database_schema.@cohort_table cohort
INNER JOIN @cdm_database_schema.person
	ON subject_id = person_id
WHERE cohort_definition_id = @target_sample_cohort_id
	AND cohort_start_date >= CAST('@start_date' AS DATE)
	AND cohort_start_date <= CAST('@end_date' AS DATE)
GROUP BY YEAR(cohort_start_date),
	MONTH(cohort_start_date),
	FLOOR((YEAR(cohort_start_date) - year_of_birth) / 10),
	gender_concept_id;

DELETE FROM @cohort_database_schema.@cohort_table 
WHERE cohort_definition_id = @comparator_sample_cohort_id;

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
	SELECT CAST(@comparator_sample_cohort_id AS INT) AS cohort_definition_id,
		visit_occurrence.person_id AS subject_id,
		visit_start_date AS cohort_start_date,
		visit_end_date AS cohort_end_date,
		ROW_NUMBER() OVER (
			PARTITION BY YEAR(visit_start_date),
				MONTH(visit_start_date),
				FLOOR((YEAR(visit_start_date) - year_of_birth) / 10),
				gender_concept_id
			ORDER BY NEWID()
			) AS rn,
		YEAR(visit_start_date) AS visit_year,
		MONTH(visit_start_date) AS visit_month,
		FLOOR((YEAR(visit_start_date) - year_of_birth) / 10) AS visit_age_group,
		gender_concept_id AS visit_gender_concept_id
	FROM @cdm_database_schema.visit_occurrence
	INNER JOIN @cdm_database_schema.observation_period
		ON visit_occurrence.person_id = observation_period.person_id
			AND visit_start_date >= DATEADD(DAY, @washout_period, observation_period_start_date)
			AND visit_start_date <= observation_period_end_date
	INNER JOIN @cdm_database_schema.person
		ON visit_occurrence.person_id = person.person_id
	LEFT JOIN (
		SELECT subject_id
		FROM @cohort_database_schema.@cohort_table 
		WHERE cohort_definition_id = @exclusion_cohort_id
		) exclusion_cohort
		ON visit_occurrence.person_id = exclusion_cohort.subject_id
	WHERE visit_start_date >= CAST('@start_date' AS DATE)
		AND visit_start_date <= CAST('@end_date' AS DATE)
		AND visit_concept_id IN (@visit_concept_ids)
		AND exclusion_cohort.subject_id IS NULL
	) visits_in_period
INNER JOIN #stratified_counts stratified_counts
	ON cohort_year = visit_year
		AND cohort_month = visit_month
		AND cohort_age_group = visit_age_group
		AND cohort_gender_concept_id = visit_gender_concept_id
WHERE rn <= @multiplier * target_cohort_count;

DELETE FROM @cohort_database_schema.@cohort_table 
WHERE cohort_definition_id = @crude_comparator_sample_cohort_id;

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
	SELECT CAST(@crude_comparator_sample_cohort_id AS INT) AS cohort_definition_id,
		visit_occurrence.person_id AS subject_id,
		visit_start_date AS cohort_start_date,
		visit_end_date AS cohort_end_date,
		ROW_NUMBER() OVER (
			PARTITION BY YEAR(visit_start_date),
				MONTH(visit_start_date)
			ORDER BY NEWID()
			) AS rn,
		YEAR(visit_start_date) AS visit_year,
		MONTH(visit_start_date) AS visit_month
	FROM @cdm_database_schema.visit_occurrence
	INNER JOIN @cdm_database_schema.observation_period
		ON visit_occurrence.person_id = observation_period.person_id
			AND visit_start_date >= DATEADD(DAY, @washout_period, observation_period_start_date)
			AND visit_start_date <= observation_period_end_date
	INNER JOIN @cdm_database_schema.person
		ON visit_occurrence.person_id = person.person_id
	LEFT JOIN (
		SELECT subject_id
		FROM @cohort_database_schema.@cohort_table 
		WHERE cohort_definition_id = @exclusion_cohort_id
		) exclusion_cohort
		ON visit_occurrence.person_id = exclusion_cohort.subject_id
	WHERE visit_start_date >= CAST('@start_date' AS DATE)
		AND visit_start_date <= CAST('@end_date' AS DATE)
		AND visit_concept_id IN (@visit_concept_ids)
		AND exclusion_cohort.subject_id IS NULL
	) visits_in_period
INNER JOIN (
	SELECT cohort_year, 
		cohort_month,
		SUM(target_cohort_count) AS target_cohort_count
	FROM #stratified_counts stratified_counts
	GROUP BY cohort_year, 
		cohort_month
	) aggregated_counts
	ON cohort_year = visit_year
		AND cohort_month = visit_month
WHERE rn <= target_cohort_count;

TRUNCATE TABLE #stratified_counts;
DROP TABLE #stratified_counts;