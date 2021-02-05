{DEFAULT @washout_period = 365}
{DEFAULT @cdm_database_schema = CDM_jmdc_v1063.dbo}
{DEFAULT @cohort_database_schema = scratch.dbo}
{DEFAULT @cohort_table = mschuemi_temp}
{DEFAULT @exposure_id = 5664}
{DEFAULT @start_date = 20080101}
{DEFAULT @end_date = 20081231}

SELECT COUNT(*) AS cohort_count,
        YEAR(cohort_start_date) AS cohort_year,
        MONTH(cohort_start_date) AS cohort_month
    FROM @cohort_database_schema.@cohort_table cohort
    INNER JOIN @cdm_database_schema.observation_period
        ON subject_id = person_id
            AND cohort_start_date >= DATEADD(DAY, @washout_period, observation_period_start_date)
            AND cohort_start_date <= observation_period_end_date
    WHERE cohort_definition_id = @exposure_id
        AND cohort_start_date >= CAST('@start_date' AS DATE)
        AND cohort_start_date <= CAST('@end_date' AS DATE)
    GROUP BY YEAR(cohort_start_date),
        MONTH(cohort_start_date);