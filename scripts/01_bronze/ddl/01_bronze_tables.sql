/*
===========================================================================
Script    : 01_bronze_tables.sql
Location  : scripts/01_bronze/ddl/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-03-13
Version   : 1.0
===========================================================================
Script Purpose:
    Creates the bronze tables into which the records from the source files
	are loaded.

  Tables Created:
      bronze.crm_customers
	  bronze.cbs_accounts
	  bronze.cbs_transactions
	  bronze.cbs_branches
	  bronze.hrms_employees
	  bronze.los_loan_applications

  Warning:
      Running this script will drop and recreate all bronze tables.
      All existing data will be permanently lost.
=============================================================================
  Change Log:
      1.0   2026-03-13   Initial creation
=============================================================================
*/
-- Drop table [bronze.crm_customers] if it exists
DROP TABLE IF EXISTS bronze.crm_customers;
GO

-- Create table [bronze.crm_customers]
CREATE TABLE bronze.crm_customers
(
	customer_id NVARCHAR(50),
	first_name NVARCHAR(50),
	last_name NVARCHAR(50),
	company_name NVARCHAR(50),
	segment NVARCHAR(50),
	risk_band NVARCHAR(50),
	date_of_birth DATE,
	gender NVARCHAR(50),
	national_id NVARCHAR(50),
	email NVARCHAR(250),
	phone_number NVARCHAR(50),
	address_line_1 NVARCHAR(200),
	city NVARCHAR(50),
	[state] NVARCHAR(50),
	zip_code NVARCHAR(50),
	country NVARCHAR(50),
	onboard_date DATE,
	onboarding_branch_id NVARCHAR(50),
	customer_since DECIMAL(8, 1),
	is_active BIT,
	marketing_opt_in BIT,
	preferred_language NVARCHAR(50),
	annual_income DECIMAL(18, 2),
	credit_score DECIMAL(8, 1),
	created_at DATETIME2,
	updated_at DATETIME2,

	-- metadata columns
	_source_system NVARCHAR(50) NOT NULL,
	_source_file NVARCHAR(200) NOT NULL,
	_batch_id INT NOT NULL,
	_load_timestamp DATETIME2 NOT NULL,
	_is_deleted BIT DEFAULT 0 NOT NULL,
	_batch_date DATE NOT NULL
);
GO

-- Drop table [bronze.cbs_accounts] if it exists
DROP TABLE IF EXISTS bronze.cbs_accounts;
GO

-- Create table [bronze.cbs_accounts]
CREATE TABLE bronze.cbs_accounts
(
	account_id NVARCHAR(50),
	customer_id NVARCHAR(50),
	account_type NVARCHAR(50),
	account_status NVARCHAR(50),
	open_date DATE,
	close_date DATE,
	currency_code NVARCHAR(50),
	current_balance DECIMAL(18, 2),
	available_balance DECIMAL(18, 2),
	overdraft_limit DECIMAL(18, 2),
	interest_rate DECIMAL(12, 4),
	branch_id NVARCHAR(50),
	assigned_employee_id NVARCHAR(50),
	is_primary BIT,
	created_at DATETIME2,
	updated_at DATETIME2,

	-- metadata columns
	_source_system NVARCHAR(50) NOT NULL,
	_source_file NVARCHAR(200) NOT NULL,
	_batch_id INT NOT NULL,
	_load_timestamp DATETIME2 NOT NULL,
	_is_deleted BIT DEFAULT 0 NOT NULL,
	_batch_date DATE NOT NULL
);
GO

-- Drop table [bronze.cbs_branches] if it exists
DROP TABLE IF EXISTS bronze.cbs_branches;
GO

-- Create table [bronze.cbs_branches]
CREATE TABLE bronze.cbs_branches
(
	branch_id NVARCHAR(50),
	branch_name NVARCHAR(200),
	branch_type NVARCHAR(50),
	address_line_1 NVARCHAR(200),
	city NVARCHAR(50),
	[state] NVARCHAR(50),
	zip_code NVARCHAR(50),
	country NVARCHAR(50),
	phone_number NVARCHAR(50),
	email NVARCHAR(250),
	opened_date DATE,
	is_active BIT,
	region NVARCHAR(50),
	manager_employee_id NVARCHAR(50),

	-- metadata columns
	_source_system NVARCHAR(50) NOT NULL,
	_source_file NVARCHAR(200) NOT NULL,
	_batch_id INT NOT NULL,
	_load_timestamp DATETIME2 NOT NULL,
	_is_deleted BIT DEFAULT 0 NOT NULL,
	_batch_date DATE NOT NULL
);
GO

-- Drop table [bronze.cbs_transactions] if it exists
DROP TABLE IF EXISTS bronze.cbs_transactions;
GO

-- Create table [bronze.cbs_transactions]
CREATE TABLE bronze.cbs_transactions
(
	transaction_id NVARCHAR(50),
	account_id NVARCHAR(50),
	transaction_type NVARCHAR(50),
	amount DECIMAL(18, 2),
	debit_credit NVARCHAR(50),
	currency NVARCHAR(50),
	transaction_date DATE,
	transaction_time TIME,
	transaction_date_time DATETIME2,
	channel NVARCHAR(50),
	[status] NVARCHAR(50),
	balance_after DECIMAL(18, 2),
	counterpart_account_id NVARCHAR(50),
	merchant_name NVARCHAR(50),
	merchant_category NVARCHAR(50),
	reference_number NVARCHAR(50),
	[description] NVARCHAR(500),
	branch_id NVARCHAR(50),
	is_flagged BIT,
	created_at DATETIME2,

	-- metadata columns
	_source_system NVARCHAR(50) NOT NULL,
	_source_file NVARCHAR(200) NOT NULL,
	_batch_id INT NOT NULL,
	_load_timestamp DATETIME2 NOT NULL,
	_is_deleted BIT DEFAULT 0 NOT NULL,
	_batch_date DATE NOT NULL
);
GO

-- Drop table [bronze.hrms_employees] if it exists
DROP TABLE IF EXISTS bronze.hrms_employees;
GO

-- Create table [bronze.hrms_employees]
CREATE TABLE bronze.hrms_employees
(
	employee_id NVARCHAR(50),
	first_name NVARCHAR(50),
	last_name NVARCHAR(50),
	email NVARCHAR(250),
	phone_number NVARCHAR(50),
	department NVARCHAR(50),
	job_title NVARCHAR(50),
	branch_id NVARCHAR(50),
	hire_date DATE,
	termination_date DATE,
	salary DECIMAL(18, 2),
	is_active BIT,
	manager_id NVARCHAR(50),

	-- metadata columns
	_source_system NVARCHAR(50) NOT NULL,
	_source_file NVARCHAR(200) NOT NULL,
	_batch_id INT NOT NULL,
	_load_timestamp DATETIME2 NOT NULL,
	_is_deleted BIT DEFAULT 0 NOT NULL,
	_batch_date DATE NOT NULL
);
GO

-- Drop table [bronze.los_loan_applications] if it exists
DROP TABLE IF EXISTS bronze.los_loan_applications;
GO

-- Create table [bronze.los_loan_applications]
CREATE TABLE bronze.los_loan_applications
(
	loan_id NVARCHAR(50),
	customer_id NVARCHAR(50),
	branch_id NVARCHAR(50),
	loan_officer_employee_id NVARCHAR(50),
	loan_type NVARCHAR(50),
	loan_status NVARCHAR(50),
	application_date DATE,
	decision_date DATE,
	disbursement_date DATE,
	requested_amount DECIMAL(18, 2),
	approved_amount DECIMAL(18, 2),
	disbursed_amount DECIMAL(18, 2),
	outstanding_balance DECIMAL(18, 2),
	interest_rate DECIMAL(12, 4),
	term_months INT,
	monthly_payment DECIMAL(18, 2),
	days_delinquent INT,
	collateral_type NVARCHAR(50),
	collateral_value DECIMAL(12, 2),
	purpose_description NVARCHAR(500),
	rejection_reason NVARCHAR(500),
	created_at DATETIME2,
	updated_at DATETIME2,

	-- metadata columns
	_source_system NVARCHAR(50) NOT NULL,
	_source_file NVARCHAR(200) NOT NULL,
	_batch_id INT NOT NULL,
	_load_timestamp DATETIME2 NOT NULL,
	_is_deleted BIT DEFAULT 0 NOT NULL,
	_batch_date DATE NOT NULL
);
GO
