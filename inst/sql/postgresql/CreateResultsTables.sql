-- Drop old tables if exist

DROP TABLE IF EXISTS analysis;
DROP TABLE IF EXISTS database;
DROP TABLE IF EXISTS database_characterization;
DROP TABLE IF EXISTS estimate;
DROP TABLE IF EXISTS exposure;
DROP TABLE IF EXISTS negative_control_outcome;
DROP TABLE IF EXISTS positive_control_outcome;
DROP TABLE IF EXISTS time_period;


-- Create tables

--Table analysis

CREATE TABLE analysis (
			method VARCHAR(255) NOT NULL,
			analysis_id INTEGER NOT NULL,
			description VARCHAR(255) NOT NULL,
			time_at_risk VARCHAR(255) NOT NULL,
			PRIMARY KEY(method, analysis_id)
);

--Table database

CREATE TABLE database (
			database_id VARCHAR(255) NOT NULL,
			database_name VARCHAR(255) NOT NULL,
			description TEXT NOT NULL,
			vocabulary_version VARCHAR(255) NOT NULL,
			min_obs_period_date DATE NOT NULL,
			max_obs_period_date DATE NOT NULL,
			study_package_version VARCHAR(255) NOT NULL,
			is_meta_analysis INTEGER NOT NULL,
			PRIMARY KEY(database_id)
);

--Table database_characterization

CREATE TABLE database_characterization (
			subject_count INTEGER NOT NULL,
			stratum TEXT NOT NULL,
			stratification VARCHAR(255) NOT NULL,
			database_id VARCHAR(255) NOT NULL,
			PRIMARY KEY(stratum, stratification, database_id)
);

--Table estimate

CREATE TABLE estimate (
			database_id VARCHAR(255) NOT NULL,
			method VARCHAR(255) NOT NULL,
			analysis_id INTEGER NOT NULL,
			exposure_id INTEGER NOT NULL,
			outcome_id INTEGER NOT NULL,
			period_id INTEGER NOT NULL,
			rr NUMERIC,
			ci_95_lb NUMERIC,
			ci_95_ub NUMERIC,
			p NUMERIC,
			exposure_subjects BIGINT,
			counterfactual_subjects BIGINT,
			exposure_days BIGINT,
			counterfactual_days BIGINT,
			exposure_outcomes BIGINT,
			counterfactual_outcomes BIGINT,
			log_rr NUMERIC,
			se_log_rr NUMERIC,
			llr NUMERIC,
			critical_value NUMERIC,
			calibrated_rr NUMERIC,
			calibrated_ci_95_lb NUMERIC,
			calibrated_ci_95_ub NUMERIC,
			calibrated_log_rr NUMERIC,
			calibrated_se_log_rr NUMERIC,
			calibrated_p NUMERIC,
			PRIMARY KEY(database_id, method, analysis_id, exposure_id, outcome_id, period_id)
);

--Table exposure

CREATE TABLE exposure (
			exposure_id INTEGER NOT NULL,
			exposure_name VARCHAR(255) NOT NULL,
			total_shots INTEGER NOT NULL,
			base_exposure_id INTEGER NOT NULL,
			base_exposure_name VARCHAR(255) NOT NULL,
			shot VARCHAR(255) NOT NULL,
			PRIMARY KEY(exposure_id, base_exposure_id)
);

--Table negative_control_outcome

CREATE TABLE negative_control_outcome (
			outcome_id INTEGER NOT NULL,
			outcome_name VARCHAR(255) NOT NULL,
			PRIMARY KEY(outcome_id)
);

--Table positive_control_outcome

CREATE TABLE positive_control_outcome (
			outcome_id INTEGER NOT NULL,
			outcome_name VARCHAR(255) NOT NULL,
			exposure_id INTEGER NOT NULL,
			negative_control_id INTEGER NOT NULL,
			effect_size NUMERIC NOT NULL,
			PRIMARY KEY(outcome_id)
);

--Table time_period

CREATE TABLE time_period (
			start_date DATE NOT NULL,
			end_date DATE NOT NULL,
			period_id INTEGER NOT NULL,
			label VARCHAR(255) NOT NULL,
			exposure_id INTEGER NOT NULL,
			PRIMARY KEY(period_id, exposure_id)
);