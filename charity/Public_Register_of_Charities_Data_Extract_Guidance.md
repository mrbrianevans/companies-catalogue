# Public Register of Charities in England and Wales data extract guidance

## Contents

Introduction

Beta Register extract

Opening a tab-separated values file

Tables available in the data extract

Charity table

Charity table data definition

Charity_annual_return_history table

Charity_annual_return_history table data definition

Charity_ar_parta table

Charity_ar_parta table data definition

Charity_ar_partb table

Charity_ar_partb table data definition

Charity_area_of_operation table

Charity_area_of_operation table data definition

Charity_classification table

Charity_classification table data definition

Charity_event_history table

Charity_event_history table data definition

Charity_governing_document table

Charity_governing_document table data definition

Charity_other_names table

Charity_other_names table data definition

Charity_other_regulators table

Charity_other_regulators table data definition

Charity_policy table

Charity_policy table data definition

Charity_published_report table

Charity_published_report table data definition

Charity_trustee table

Charity_trustee table data definition

## Introduction

This extract contains all registered, removed and linked charities. All tables of the extract are currently in beta. All tables are provided in JSON and tab-separated values format, this is due to embedded characters within the dataset. For those unfamiliar with these formats guidance is provided below.  

## Beta Register extract

The Register extract contains tables that replicate all the data available within the Public Register of Charities. Details of all the tables can be found in Tables available in the dataset section of this document. 

The Beta Register extract provides many tables that can be linked to form a whole database of the Register, linking fields are clearly stated in the introduction to each data set. 

## Opening a tab-separated values file

Database applications will be familiar with tab-separated values format.  

To open a tab-separated values file in excel follow the following steps. 

1. Double click the tab-separated values file to open 
2. A window titled “Text import wizard” should appear 
3. Select “delimited”, then click next 
4. Check the “tab” box under the “Delimiters”, then click next 
5. Click “Finish”, the file will now open  

## Tables available in the data extract

### Charity table

The charity table provides an overview of a charity including registration date, latest submission information and contact information. This is the table provided in the basic details Register extract. Data is provided for registered and removed charities.  

### Charity table data definition

| Field name | Type | Description |
|------------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| linked_charity_number | int | A number that uniquely identifies the subsidiary or group member associated with a registered charity. Used for user identification purposes where the subsidiary is known by the parent registration number and the subsidiary number. The main parent charity has a linked_charity_number of 0. |
| charity_name | varchar | The Main Name of the Charity |
| charity_type | varchar | The type of the charity displayed on the public register of charities. Only the main parent charity will have a value for this field (i.e. linked_charity_number=0). |
| charity_registration_status | varchar | The charity registration status indicates whether a charity is registered or removed |
| date_of_registration | date | The date the charity was registered with the Charity Commission. |
| date_of_removal | date | This is the date the charity was removed from the Register of Charities. This will not necessarily be the same date that the charity ceased to exist or ceased to operate. For non-removed charities the field is NULL. |
| charity_reporting_status | varchar | The current reporting status of the charity |
| latest_acc_fin_period_start_date | date | The start date of the latest financial period for which the charity has made a submission. |
| latest_acc_fin_period_end_date | date | The end date of the latest financial period for which the charity has made a submission. |
| latest_income | decimal | The latest income submitted by the charity. This is the total gross income submitted on part A of the annual return submission. |
| latest_expenditure | decimal | The latest expenditure submitted by a charity. This is the expenditure submitted on part A of the annual return submission. |
| charity_contact_address1 | varchar | Charity Address Line 1 |
| charity_contact_address2 | varchar | Charity Address Line 2 |
| charity_contact_address3 | varchar | Charity Address Line 3 |
| charity_contact_address4 | varchar | Charity Address Line 4 |
| charity_contact_address5 | varchar | Charity Address Line 5 |
| charity_contact_postcode | varchar | Charity Postcode |
| charity_contact_phone | varchar | Charity Public Telephone Number |
| charity_contact_email | varchar | Charity Public email address |
| charity_contact_web | varchar | Charity Website Address |
| charity_company_registration_number | varchar | Registered Company Number of the Charity as assigned by Companies House. Integer returned as string |
| charity_insolvent | bit | Indicates if the charity is insolvent. |
| charity_in_administration | bit | Indicates if the charity is in administration. |
| charity_previously_excepted | bit | Indicates the charity was previously an excepted charity. |
| charity_is_cdf_or_cif | varchar | Indicates whether the charity is a Common Investment Fund or Common Deposit Fund. |
| charity_is_cio | bit | Indicates whether the charity is a Charitable Incorporated Organisation. |
| cio_is_dissolved | bit | Indicates the CIO is to be dissolved. |
| date_cio_dissolution_notice | date | Date the CIO dissolution notice expires |
| charity_activities | varchar | The charity activities, the trustees’ description of what they do and who they help. |
| charity_gift_aid | bit | Indicates whether the charity is registered for gift aid with HMRC. True, False, NULL (not known) |
| charity_has_land | bit | Indicates whether the charity owns or leases any land or buildings. True, False, NULL (not known) |

### Charity_annual_return_history table

The charity_annual_return_history table provides submission details of the current and previous annual returns provided by the charity. Organisation_number can be used to link the data to other tables. 

### Charity_annual_return_history table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| fin_period_start_date | date | The start date of the financial period which is detailed for the charity. |
| fin_period_end_date | date | The end date of the financial period which is detailed for the charity. |
| ar_cycle_reference | varchar | The annual return cycle to which the submission details relate. |
| reporting_due_date | date | The due date of the financial period which is detailed for the charity. |
| date_annual_return_received | date | The date the annual return was received for the financial period which is detailed for the charity. |
| date_accounts_received | date | The date the charity accounts were received for the financial period which is detailed for the charity. |
| total_gross_income | decimal | The total gross income reported on Part A of the annual return for the financial period detailed. |
| total_gross_expenditure | decimal | The total gross expenditure reported on Part A of the annual return for the financial period detailed. |
| accounts_qualified | bit | Indicates whether the accounts have a qualified opinion. (True or NULL) |
| suppression_ind | bit | An indicator of whether the finances for this year are currently suppressed. 1 = Supressed, 0 = not supressed. |
| suppression_type | varchar | The type of suppression that is applied to the finances for this year. |

### Charity_ar_parta table

The charity_ar_parta table provides financial details provided on part a of current and previous annual returns provided by the charity. Organisation_number can be used to link the data to other tables. 

### Charity_ar_parta table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| latest_fin_period_submitted_ind | bit | Indicates whether the financial data on this line relates to the latest financial data submitted by the charity. (True or False) |
| fin_period_order_number | tinyint | A field to aid ordering of the financial data for each charity. (1=Most recent data in the table, 5=Least recent data in the table) |
| ar_cycle_reference | varchar | The annual return cycle to which the submission details relate. |
| fin_period_start_date | date | The start date of the financial period which is detailed for the charity. |
| fin_period_end_date | date | The end date of the financial period which is detailed for the charity. |
| ar_due_date | date | The due date of the annual return which is detailed for the charity. |
| ar_received_date | date | The date the annual return was received for the financial period which is detailed for the charity. |
| total_gross_income | decimal | The total gross income reported on Part A of the annual return for the financial period detailed. |
| total_gross_expenditure | decimal | The total gross expenditure reported on Part A of the annual return for the financial period detailed. |
| charity_raises_funds_from_public | bit | Indicates if the charity raised funds from the public for the financial period which is detailed for the charity. (True, False or NULL) |
| charity_professional_fundraiser | bit | Indicates if the charity worked with professional fundraisers for the financial period which is detailed for the charity. (True, False or NULL) |
| charity_agreement_professional_fundraiser | bit | Indicates if the charity had an agreement with its professional fundraisers for the financial period which is detailed. (True, False or NULL) |
| charity_commercial_participator | bit | Indicates if the charity worked with commercial participators for the financial period detailed. (True, False or NULL) |
| charity_agreement_commerical_participator | bit | Indicates if the charity had an agreement with its commercial participators for the financial period detailed. (True, False or NULL) |
| grant_making_is_main_activity | bit | Indicates if grant making was the main way the charity carried out its purposes for the financial period detailed. (True, False or NULL) |
| charity_receives_govt_funding_contracts | bit | Indicates if the charity received any income from government contracts for the financial period detailed. (True, False or NULL) |
| count_govt_contracts | int | The number of government contracts the charity had for the financial period detailed. |
| charity_receives_govt_funding_grants | bit | Indicates if the charity received any income from government grants for the financial period detailed. (True, False or NULL) |
| count_govt_grants | int | The number of government grants the charity received for the financial period detailed. |
| income_from_government_contracts | decimal | The income the charity received from government contracts for the financial period detailed. |
| income_from_government_grants | decimal | The income the charity received from government grants for the financial period detailed. |
| charity_has_trading_subsidiary | bit | Indicates if the charity had a trading subsidiary for the financial period detailed. (True, False or NULL) |
| trustee_also_director_of_subsidiary | bit | Indicates if a trustee was also a director of a trading subsidiary for the financial period detailed. (True, False or NULL) |
| does_trustee_receive_any_benefit | bit | Indicates if any of the trustees received any remuneration, payments or benefits from the charity other than refunds of legitimate trustee expenses for the financial period detailed. (True, False or NULL) |
| trustee_payments_acting_as_trustee | bit | Indicates if any trustees received payments for acting as a trustee for the financial period detailed. (True, False or NULL) |
| trustee_receives_payments_services | bit | Indicates if any trustees received payments for providing services to the charity for the financial period detailed. (True, False or NULL) |
| trustee_receives_other_benefit | bit | Indicates if any trustees received any other benefit from the charity for the financial period detailed. (True, False or NULL) |
| trustee_resigned_employment | bit | Indicates if any of the trustees resigned and took up employment with the charity during the financial period detailed. (True, False or NULL) |
| employees_salary_over_60k | bit | Indicates if any of the charity's staff received total employee benefits of £60,000 or more. (True, False or NULL) |
| count_salary_band_60001_70000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_70001_80000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_80001_90000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_90001_100000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_100001_110000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_110001_120000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_120001_130000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_130001_140000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_140001_150000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_150001_200000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_200001_250000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_250001_300000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_300001_350000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_350001_400000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_400001_450000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_450001_500000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_salary_band_over_500000 | int | Number of staff whose total employment benefits were in this band for the financial period detailed. |
| count_volunteers | int | Number of Volunteers. The trustees' estimate of the number of people who undertook voluntary work in the UK for the charity during the year. The number shown is a head count and not expressed as full-time equivalents. Charities are invited to provide an estimate of volunteer numbers in their Annual Return but are not obliged to do so. Where a number is provided by the charity, including zero, that number is displayed. |

### Charity_ar_partb table

The charity_ar_partb table provides financial details provided on part B of current and previous annual returns provided by the charity. Organisation_number can be used to link the data to other tables. Note that charities are only required to submit a part B when their income for that year exceeds £500k. 

### Charity_ar_partb table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| latest_fin_period_submitted_ind | bit | Indicates whether the financial data on this line relates to the latest financial data submitted by the charity. (True or False) |
| fin_period_order_number | tinyint | A field to aid ordering of the financial data for each charity. (1=Most recent data in the table, 5=Least recent data in the table) |
| ar_cycle_reference | varchar | The annual return cycle to which the submission details relate. |
| fin_period_start_date | date | The start date of the financial period which is detailed for the charity. |
| fin_period_end_date | date | The end date of the financial period which is detailed for the charity. |
| ar_due_date | date | The due date of the annual return which is detailed for the charity. |
| ar_received_date | date | The date the annual return was received for the financial period which is detailed for the charity. |
| income_donations_and_legacies | decimal | Income from donations and legacies as entered on the Annual Return form for the financial period detailed. |
| income_charitable_activities | decimal | Income received as fees or grants specifically for goods and services supplied by the charity to meet the needs of its beneficiaries for the financial period detailed. |
| income_other_trading_activities | decimal | Income from other trading activity as entered on the Annual Return form for the financial period detailed. |
| income_investments | decimal | Income from investments including dividends, interest and rents but excluding changes (realised and unrealised gains) in the capital value of the investment portfolio for the financial period detailed. |
| income_other | decimal | Other income. This category includes gains on the disposal of own use assets (i.e. fixed assets not held as investments), but otherwise is only used exceptionally for very unusual transactions that cannot be accounted for in the categories above for the financial period detailed. |
| income_total_income_and_endowments | decimal | Total income including endowments for the financial period detailed. |
| income_legacies | decimal | Income from legacies as entered on the Annual Return form for the financial period detailed. |
| income_endowments | decimal | Income from endowments as entered on the Annual Return form for the financial period detailed. |
| expenditure_raising_funds | decimal | Costs associated with providing goods and services to the public, where the main motive is to raise funds for the charity rather than providing goods or services to meet the needs of its beneficiaries for the financial period detailed. (eg charity shops, fundraising dinners etc.). |
| expenditure_charitable_expenditure | decimal | Costs incurred by the charity in supplying goods or services to meet the needs of its beneficiaries. Grants made to meet the needs of the charity’s beneficiaries for the financial period detailed. |
| expenditure_other | decimal | Other expenditure for the financial period detailed. This category is only used very exceptionally for items that don’t fit within one of the categories above. |
| expenditure_total | decimal | Total expenditure for the financial period detailed on the Part B of the annual return. |
| expenditure_investment_management | decimal | Expenditure managing investments for the financial period detailed. |
| expenditure_grants_institution | decimal | Any grants that the charity has awarded to other institutions to further their charitable work. |
| expenditure_governance | decimal | Costs associated with running the charity itself for the financial period. (e.g. costs of trustee meetings, internal and external audit costs and legal advice relating to governance matters). |
| expenditure_support_costs | decimal | Support costs should be allocated across activities and are those costs which, while necessary to deliver an activity, do not themselves produce the activity. They include the central office functions of the charity and are often apportioned to activities. The amount shown here is the total amount of support costs (for charitable, fundraising and governance activities) included in resources expended. |
| expenditure_depreciation | decimal | Depreciation charge for the year can be found in the fixed asset analysis notes to the accounts. This is the amount of depreciation on tangible fixed assets (including impairment charges, if any), which will be shown as the charge for the year in the tangible fixed asset note to the accounts. |
| gain_loss_investment | decimal | The gain or loss associated with the charity’s investments |
| gain_loss_pension_fund | decimal | The gain or loss associated with the charity’s pension fund |
| gain_loss_revaluation_fixed_investment | decimal | The gain or loss associated with any revaluation of fixed assets |
| gain_loss_other | decimal | The gain or loss associated with any other assets |
| reserves | decimal | The level of reserves is those unrestricted funds which are freely available for the charity to spend and can be found in the Financial Review in the Trustees Annual Report and will exclude endowments. |
| assets_total_fixed | decimal | Total fixed assets. Fixed assets are those held for continuing use and include tangible fixed assets such as land, buildings, equipment and vehicles, and any investments held on a long-term basis to generate income or gains. |
| assets_own_use | decimal | Total own use assets. This is a calculated field. assets_own_use = assets_total_fixed – assets_long_term_investment |
| assets_long_term_investment | decimal | Fixed Asset Investment are held for the long term to generate income or gains and may include quoted and unquoted shares, bonds, gilts, common investment funds, investment property and term deposits held as part of an investment portfolio. |
| defined_benefit_pension_scheme | decimal | This is surplus or deficit in any defined benefit pension scheme operated and represents a potential long-term asset or liability. |
| assets_other_assets | decimal | The value of any other assets |
| assets_total_liabilities | decimal | The value of the total liabilities for the charity. This is a calculated field. assets_total_liabilities = creditors_one_year_total_current + creditors_falling_due_after_one_year |
| assets_current_investment | decimal | Total Current Assets are a separate class of Total Current Asset and they are held with intention of disposing of them within 12 months. |
| assets_total_assets_and_liabilities | decimal | Total Net assets or liabilities can be found on the Balance Sheet. This is the total of all assets shown less all liabilities. This should be the same as the Total funds of the charity. |
| creditors_one_year_total_current | decimal | Creditors due within one year are the amounts owed to creditors and include loans and overdrafts, trade creditors, accruals and deferred income and they are payable within one year. |
| creditors_falling_due_after_one_year | decimal | These are the amounts owed to creditors payable after more than one year, with provisions for liabilities and charges. |
| assets_cash | decimal | Cash at bank and in hand are a separate class of Total Current Assets. This amount includes deposits with banks and other financial institutions, which are repayable on demand, but excludes bank overdrafts. |
| funds_endowment | decimal | Endowment funds include the amount of all permanent and expendable endowment funds. |
| funds_unrestricted | decimal | Unrestricted funds include the amount of all funds held for the general purposes of the charity. This will include unrestricted income funds, designated funds, revaluation reserves and any pension reserve. |
| funds_restricted | decimal | Restricted funds include the amount of all funds held that must be spent on the purposes of the charity. |
| funds_total | decimal | Total funds can be found on the Balance Sheet and should be the same as Total net assets/(liabilities). |
| count_employees | int | The number of people that the charity employs |
| charity_only_accounts | bit | Indicates if the accounts represent only the charity accounts |
| consolidated_accounts | bit | Consolidated accounts bring together the resources of the charity and the subsidiaries under its control in one statement. These subsidiaries may be non-charitable and to exist for purposes that benefit the parent charity e.g. fund-raising. If set to 1 the accounts are consolidated. |

### Charity_area_of_operation table

The charity_area_of_operation table provides details on what areas the charity works in. Organisation_number can be used to link the data to other tables. Note that charities can add many areas of operation for their charity. 

### Charity_area_of_operation table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| linked_charity_number | int | A number that uniquely identifies the subsidiary or group member associated with a registered charity. Used for user identification purposes where the subsidiary is known by the parent registration number and the subsidiary number. The main parent charity has a linked_charity_number of 0. |
| geographic_area_type | varchar | The area type for this row |
| geographic_area_description | varchar | The area descriptor for this row |
| parent_geographic_area_type | varchar | The parent area type. For example, if the area type is a country this indicator will be continent |
| parent_geographic_area_description | varchar | The descriptor for the parent area type |
| welsh_ind | bit | Indicates Welsh areas |

### Charity_classification table

The charity_classification table provides details on what the charity does, who they help and how this is achieved. Organisation_number can be used to link the data to other tables. Note that charities can select multiple classifications for their charity in each category. 

### Charity_classification table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| linked_charity_number | int | A number that uniquely identifies the subsidiary or group member associated with a registered charity. Used for user identification purposes where the subsidiary is known by the parent registration number and the subsidiary number. The main parent charity has a linked_charity_number of 0. |
| classification_code | int | The code of the classification described in the row |
| classification_type | varchar | The type of the classification. What - What the charity does How - How the charity helps Who - Who the charity helps |
| classification_description | varchar | The descriptor of the classification code. |

### Charity_event_history table

The charity_event_history table provides basic details of events. Organisation_number can be used to link the data to other tables. For events such as transfers assoc_organisation_number can be used to link to the details of the associated charity. 

### Charity_event_history table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| charity_name | varchar | The Main Name of the Charity |
| linked_charity_number | int | A number that uniquely identifies the subsidiary or group member associated with a registered charity. Used for user identification purposes where the subsidiary is known by the parent registration number and the subsidiary number. The main parent charity has a linked_charity_number of 0. |
| charity_event_order | bigint | The order of the event in the charity history. 1 is the earliest event. |
| event_type | varchar | The type of charity event that has occurred. |
| date_of_event | date | The date that the event occurred. |
| reason | varchar | The reason that the event occurred. A registration event will not have a reason available. |
| assoc_organisation_number | int | The charity id for the charity associated with the charity event. For example, in the case of asset transfer in this is the charity that has transferred the funds into the charity. |
| assoc_registered_charity_number | int | The registered charity number for the charity associated with the charity event. For example, in the case of asset transfer in this is the charity that has transferred the funds into the charity. |
| assoc_charity_name | varchar | The charity name of the charity associated with the charity event. For example, in the case of asset transfer in this is the charity that has transferred the funds into the charity. |

### Charity_governing_document table

The charity_governing_document table provides details of the structure of the charity governing document and the charity objects. Organisation_number can be used to link the data to other tables. 

### Charity_governing_document table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| linked_charity_number | int | A number that uniquely identifies the subsidiary or group member associated with a registered charity. Used for user identification purposes where the subsidiary is known by the parent registration number and the subsidiary number. The main parent charity has a linked_charity_number of 0. |
| governing_document_description | varchar | The description of the governing document. Note that this is not the governing document itself but the details of the original document and any subsequent amendments. |
| charitable_objects | varchar | The charitable objects of the charity. |
| area_of_benefit | varchar | The area of benefit of the charity as defined in the governing document. This field can be blank as a charity does not have to define an area of benefit in the governing document. |

### Charity_other_names table

The charity_other_names table provides details of any other names the charity may use. Previously used names are also provided. Organisation_number can be used to link the data to other tables. 

### Charity_other_names table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| linked_charity_number | int | A number that uniquely identifies the subsidiary or group member associated with a registered charity. Used for user identification purposes where the subsidiary is known by the parent registration number and the subsidiary number. The main parent charity has a linked_charity_number of 0. |
| charity_name_id | int | An id for the other charity name |
| charity_name_type | varchar | The type of other charity name. This can be working name or previous name. |
| charity_name | varchar | The Main Name of the Charity |

### Charity_other_regulators table

The charity_other_regulators table provides details of any other regulators that apply to the charity. Organisation_number can be used to link the data to other tables. 

### Charity_other_regulators table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| regulator_order | bigint | A field to aid the ordering of the regulators for the charity. |
| regulator_name | varchar | The name of the regulator. |
| regulator_web_url | varchar | The web URL for the regulator. |

### Charity_policy table

The charity_policy table provides details of any policies that the charity has registered as having in place. Organisation_number can be used to link the data to other tables. 

### Charity_policy table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| linked_charity_number | int | A number that uniquely identifies the subsidiary or group member associated with a registered charity. Used for user identification purposes where the subsidiary is known by the parent registration number and the subsidiary number. The main parent charity has a linked_charity_number of 0. |
| policy_name | varchar | The name of the policy that the charity has in place. |

### Charity_published_report table

The charity_published_report table provides details of any reports that are currently active for the charity. These could include inquiry opening information, inquiry reports and warnings amongst others. Organisation_number can be used to link the data to other tables. 

### Charity_published_report table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| charity_id | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| linked_charity_number | int | A number that uniquely identifies the subsidiary or group member associated with a registered charity. Used for user identification purposes where the subsidiary is known by the parent registration number and the subsidiary number. The main parent charity has a linked_charity_number of 0. |
| report_name | varchar | The type of report that has been published in relation to the charity. |
| report_location | varchar | The web URL for the location on the charity commission .gov site where the published report can be located. |
| date_published | date | The date that the message on the public register of charities to the report was published. |

### Charity_trustee table

The charity_trustee table provides details of trustees that are currently active for the charity. Organisation_number can be used to link the data to other tables. Trustee_id is unique for each trustee and can be used to locate other trusteeships a trustee may have. 

### Charity_trustee table data definition

| Field | Type | Description |
|-------|------|-------------|
| date_of_extract | date | The date that the extract was taken from the main dataset. |
| organisation_number | int | The organisation number for the charity. This is the index value for the charity. |
| registered_charity_number | int | The registration number of the registered organisation allocated by the Commission. Note that a main charity and all its linked charities will share the same registered_charity_number. |
| linked_charity_number | int | A number that uniquely identifies the subsidiary or group member associated with a registered charity. Used for user identification purposes where the subsidiary is known by the parent registration number and the subsidiary number. The main parent charity has a linked_charity_number of 0. |
| trustee_id | int | The id number of the trustee. |
| trustee_name | varchar | The name of the trustee. |
| trustee_is_chair | bit | TRUE if the trustee is the Chair. FALSE otherwise. |
| individual_or_organisation | char | A flag to denote whether the trustee is an individual or an organisation. O for organisation or P for an individual. |
| trustee_date_of_appointment | date | The date of appointment of the trustee for that charity. |