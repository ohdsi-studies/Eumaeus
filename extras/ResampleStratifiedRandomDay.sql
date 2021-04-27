{DEFAULT @cdm_database_schema = CDM_jmdc_v1063.dbo}
{DEFAULT @cohort_database_schema = scratch.dbo}
{DEFAULT @cohort_table = mschuemi_temp}
{DEFAULT @start_date = 20080101}
{DEFAULT @end_date = 20081231}
{DEFAULT @target_sample_cohort_id = 1}
{DEFAULT @random_date_comparator_sample_cohort_id = 4}
{DEFAULT @exclusion_cohort_id = 123}
{DEFAULT @washout_period = 365}
{DEFAULT @multiplier = 2}

-- Create stratified counts ---------------------------------------------------------------------------------------
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

--Random-day cohort stratified by age and sex ----------------------------------------------------------------------------------
DELETE FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id = @random_date_comparator_sample_cohort_id;

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
	SELECT CAST(@random_date_comparator_sample_cohort_id AS INT) AS cohort_definition_id,
		random_dates.person_id AS subject_id,
		index_date AS cohort_start_date,
		index_date AS cohort_end_date,
		ROW_NUMBER() OVER (
			PARTITION BY YEAR(index_date),
			MONTH(index_date),
			FLOOR((YEAR(index_date) - year_of_birth) / 10),
			gender_concept_id ORDER BY NEWID()
			) AS rn,
		YEAR(index_date) AS index_year,
		MONTH(index_date) AS index_month,
		FLOOR((YEAR(index_date) - year_of_birth) / 10) AS index_age_group,
		gender_concept_id AS index_gender_concept_id
	FROM (
		SELECT person_id,
			DATEADD(DAY, FLOOR(DATEDIFF(DAY, start_date, end_date) * RAND()), start_date) AS index_date
		FROM (
			SELECT person_id,
				CASE 
					WHEN DATEADD(DAY, @washout_period, observation_period_start_date) > CAST('@start_date' AS DATE)
						THEN DATEADD(DAY, @washout_period, observation_period_start_date)
					ELSE CAST('@start_date' AS DATE)
					END AS start_date,
				CASE 
					WHEN observation_period_end_date < CAST('@end_date' AS DATE)
						THEN observation_period_end_date
					ELSE CAST('@end_date' AS DATE)
					END AS end_date
			FROM @cdm_database_schema.observation_period
			) trunc_op
		WHERE start_date < end_date
		) random_dates
	INNER JOIN @cdm_database_schema.person
		ON random_dates.person_id = person.person_id
	LEFT JOIN (
		SELECT subject_id,
			cohort_start_date
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id = @exclusion_cohort_id
		) exclusion_cohort
		ON random_dates.person_id = exclusion_cohort.subject_id
			AND index_date >= exclusion_cohort.cohort_start_date		
	WHERE exclusion_cohort.subject_id IS NULL
	) random_dates_in_period
INNER JOIN #stratified_counts stratified_counts
	ON cohort_year = index_year
		AND cohort_month = index_month
		AND cohort_age_group = index_age_group
		AND cohort_gender_concept_id = index_gender_concept_id
WHERE rn <= @multiplier * target_cohort_count;
