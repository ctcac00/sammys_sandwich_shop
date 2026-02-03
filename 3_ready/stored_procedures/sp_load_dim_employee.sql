/*
================================================================================
  STORED PROCEDURE: SP_LOAD_DIM_EMPLOYEE
================================================================================
  Loads the employee dimension from enriched layer.
================================================================================
*/

USE DATABASE SAMMYS_SANDWICH_SHOP;
USE SCHEMA SAMMYS_READY;

CREATE OR REPLACE PROCEDURE sp_load_dim_employee()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    -- Merge employee dimension from enriched layer
    MERGE INTO dim_employee tgt
    USING (
        SELECT 
            employee_id,
            first_name,
            last_name,
            full_name,
            job_title,
            department,
            hourly_rate,
            -- Rate band for analysis
            CASE 
                WHEN hourly_rate < 15.00 THEN 'Entry Level'
                WHEN hourly_rate < 18.00 THEN 'Standard'
                WHEN hourly_rate < 25.00 THEN 'Senior'
                ELSE 'Management'
            END AS rate_band,
            location_id,
            is_manager,
            hire_date,
            DATEDIFF('month', hire_date, CURRENT_DATE()) AS tenure_months,
            employment_status
        FROM SAMMYS_ENRICHED.enriched_employees
    ) src
    ON tgt.employee_id = src.employee_id AND tgt.is_current = TRUE
    WHEN MATCHED THEN UPDATE SET
        first_name = src.first_name,
        last_name = src.last_name,
        full_name = src.full_name,
        job_title = src.job_title,
        department = src.department,
        hourly_rate = src.hourly_rate,
        rate_band = src.rate_band,
        location_id = src.location_id,
        is_manager = src.is_manager,
        tenure_months = src.tenure_months,
        employment_status = src.employment_status,
        _updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        employee_id, first_name, last_name, full_name, job_title, department,
        hourly_rate, rate_band, location_id, is_manager, hire_date, tenure_months,
        employment_status, effective_date, is_current
    ) VALUES (
        src.employee_id, src.first_name, src.last_name, src.full_name, 
        src.job_title, src.department, src.hourly_rate, src.rate_band,
        src.location_id, src.is_manager, src.hire_date, src.tenure_months,
        src.employment_status, CURRENT_DATE(), TRUE
    );
    
    SELECT COUNT(*) INTO v_rows_inserted FROM dim_employee;
    
    RETURN 'SUCCESS: dim_employee now has ' || v_rows_inserted || ' records';
END;
$$;
