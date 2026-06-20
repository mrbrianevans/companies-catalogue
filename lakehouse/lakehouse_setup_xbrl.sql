
--- XBRL
CREATE TABLE IF NOT EXISTS xbrl.xbrl
(run_code VARCHAR, company_id VARCHAR, date DATE, file_type VARCHAR, taxonomy VARCHAR, balance_sheet_date DATE, companies_house_registered_number VARCHAR, entity_current_legal_name VARCHAR, company_dormant BOOLEAN, average_number_employees_during_period BIGINT, period_start DATE, period_end DATE, tangible_fixed_assets BIGINT, debtors BIGINT, cash_bank_in_hand DOUBLE, current_assets BIGINT, creditors_due_within_one_year BIGINT, creditors_due_after_one_year BIGINT, net_current_assets_liabilities BIGINT, total_assets_less_current_liabilities BIGINT, net_assets_liabilities_including_pension_asset_liability DOUBLE, called_up_share_capital DOUBLE, profit_loss_account_reserve BIGINT, shareholder_funds DOUBLE, turnover_gross_operating_revenue BIGINT, other_operating_income BIGINT, cost_sales BIGINT, gross_profit_loss BIGINT, administrative_expenses BIGINT, raw_materials_consumables VARCHAR, staff_costs BIGINT, depreciation_other_amounts_written_off_tangible_intangible_fixed_assets VARCHAR, other_operating_charges_format2 VARCHAR, operating_profit_loss BIGINT, profit_loss_on_ordinary_activities_before_tax BIGINT, tax_on_profit_or_loss_on_ordinary_activities BIGINT, profit_loss_for_period BIGINT, "error" VARCHAR, zip_url VARCHAR, zip_start DATE, zip_end DATE, csv_name VARCHAR);

-- sorted table
ALTER TABLE xbrl.xbrl SET SORTED BY (zip_start ASC);

