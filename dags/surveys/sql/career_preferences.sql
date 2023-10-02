{% extends "rotate_table.sql" %}
{% block query %}
WITH consumer_cps AS (
    -- Consumer Career Preferences Survey
    SELECT
        form_id
        , submission_id
        , submission_timestamp
        , 'Career Preferences Survey' AS survey_name
        , MAX(CASE WHEN label = 'Learn UUID' THEN value END) AS learn_uuid
        , MAX(CASE WHEN label = 'Email' THEN value END) AS email
        , MAX(CASE WHEN label = 'Discipline' THEN value END) AS discipline
        , MAX(CASE WHEN label = 'Pace' THEN value END) AS pace
        , MAX(CASE WHEN label = 'Start Date' THEN value END) AS start_date
        , MAX(CASE WHEN label = 'LinkedIn URL' THEN value END) AS linkedin_url
        , MAX(CASE WHEN label = 'Blog URL' THEN value END) AS blog_url
        , MAX(CASE WHEN label = 'Webpage/Portfolio URL' THEN value END) AS webpage_portfolio_url
        , MAX(CASE WHEN label ~ '1\\. Introduce yourself' THEN value END) AS introduce_yourself
        , MAX(CASE WHEN label ~ '2\\. Whare your your plans after' THEN value END) AS plans_after
        , MAX(CASE WHEN label ~ '3\\. What are your hobbies' THEN value END) AS hobbies
        , MAX(CASE WHEN label ~ '4\\. Where do you think you could most benefit' THEN value END) AS most_benefit
        , MAX(CASE WHEN label ~ '5\\. What, if any, barriers' THEN value END) AS barriers
        , MAX(CASE WHEN label ~ '6\\. Are you returning to work after a 12-month' THEN value END) AS returning_to_work
        , MAX(CASE WHEN label ~ '7\\. Is there anything else' THEN value END) AS anything_else
        , MAX(CASE WHEN label = 'How would you rate your level of readiness to enter your job search?  (Professional Tech Identity, Online Branding/LinkedIn, Networking, Resume)' THEN value END) AS readiness_cps
        , MAX(CASE WHEN label = 'How would you rate your level of motivation to pursue a new career in your discipline of study?' THEN value END) AS motivation_cps
        -- Alumni-only fields
        , NULL AS alumni_career_coach
        , NULL AS alumni_same_coach
        , NULL AS alumni_catalyst
        , NULL AS alumni_support_options
    FROM {{ params.table_refs["formstack.career_preferences_merged"] }}
    GROUP BY 
        form_id
        , submission_id
        , submission_timestamp
),
cps AS (
    SELECT * FROM consumer_cps
    UNION ALL
    -- Alumni Career Preferences Survey
    SELECT
        form_id
        , submission_id
        , submission_timestamp
        , 'Alumni Career Preferences Survey' AS survey_name
        , MAX(CASE WHEN label = 'Learn UUID' THEN value END) AS learn_uuid
        , MAX(CASE WHEN label = 'Email' THEN value END) AS email
        , MAX(CASE WHEN label = 'Discipline' THEN value END) AS discipline
        , MAX(CASE WHEN label = 'Pace' THEN value END) AS pace
        , MAX(CASE WHEN label = 'Start Date' THEN value END) AS start_date
        , MAX(CASE WHEN label = 'LinkedIn URL' THEN value END) AS linkedin_url
        , MAX(CASE WHEN label = 'Blog URL' THEN value END) AS blog_url
        , MAX(CASE WHEN label = 'Webpage/Portfolio URL' THEN value END) AS webpage_portfolio_url
        , MAX(CASE WHEN label ~ '1\\. Introduce yourself' THEN value END) AS introduce_yourself
        , MAX(CASE WHEN label ~ '2\\. Whare your your plans after' THEN value END) AS plans_after
        , MAX(CASE WHEN label ~ '3\\. What are your hobbies' THEN value END) AS hobbies
        , MAX(CASE WHEN label ~ '4\\. Where do you think you could most benefit' THEN value END) AS most_benefit
        , MAX(CASE WHEN label ~ '5\\. What, if any, barriers' THEN value END) AS barriers
        , MAX(CASE WHEN label ~ '6\\. Are you returning to work after a 12-month' THEN value END) AS returning_to_work
        , MAX(CASE WHEN label ~ '7\\. Is there anything else' THEN value END) AS anything_else
        , NULL AS readiness_cps
        , NULL AS motivation_cps
        -- Alumni-only fields
        , MAX(CASE WHEN label = 'Who was your former Career Coach? (If you don''t remember, that''s okay!)' THEN value END) AS alumni_career_coach
        , MAX(CASE WHEN label = 'Would you like to work with the same coach again, if possible?' THEN value END) AS alumni_same_coach
        , MAX(CASE WHEN label = 'What is the catalyst for you for getting back in touch? Why now?' THEN value END) AS alumni_catalyst
        , MAX(CASE WHEN label = 'What would you like support with from a Career Coach? Choose up to 3. (Optional)' THEN value END) AS alumni_support_options
    FROM {{ params.table_refs["formstack.career_preferences_alumni_merged"] }}
    GROUP BY 
        form_id
        , submission_id
        , submission_timestamp
),
amazon_p1 AS (
    -- Amazon Career Choice (Phase 1)
    SELECT
        form_id
        , submission_id
        , submission_timestamp
        , 'Amazon Career Choice Phase 1' AS survey_name
        , MAX(CASE WHEN label = 'Learn UUID' THEN value END) AS learn_uuid
        , MAX(CASE WHEN label = 'Email' THEN value END) AS email
        -- Amazon Only Phase 1
        , MAX(CASE WHEN label = 'Which of these best describes your current Employment Status at Amazon?' THEN value END) AS amazon_current_employment_p1
        , MAX(CASE WHEN label = 'What is your current Job Title at Amazon?' THEN value END) AS amazon_current_job_title_p1
        , MAX(CASE WHEN label = 'What are your plans after Flatiron School? (Articulate your near term career goals)' THEN value END) AS amazon_plans_after_p1
        , MAX(CASE WHEN label = 'How would you rate your level of readiness to enter your job search?  (Professional Tech Identity, Online Branding/LinkedIn, Networking, Resume)' THEN value END) AS amazon_readiness_p1
        , MAX(CASE WHEN label = 'How would you rate your level of motivation to pursue a new career in your discipline of study?' THEN value END) AS amazon_motivation_p1
        , MAX(CASE WHEN label = 'How would you describe your gender?' THEN value END) AS amazon_gender
        , MAX(CASE WHEN label = 'What is your racial identity?' THEN value END) AS amazon_race
        , MAX(CASE WHEN label = 'How would you describe your ethnicity?' THEN value END) AS amazon_ethnicity
        , MAX(CASE WHEN label = 'What is the highest level of education you have achieved?' THEN value END) AS amazon_education
        , MAX(CASE WHEN label = 'Additional identity categories (Select all that apply)' THEN value END) AS amazon_identities
    FROM {{ params.table_refs["formstack.amazon_career_choice_phase_1_merged"] }}
    GROUP BY 
        form_id
        , submission_id
        , submission_timestamp
),
amazon_cps AS (
    -- Amazon Career Choice (Career Preferences Survey)
    SELECT
        form_id
        , submission_id
        , submission_timestamp
        , 'Amazon Career Choice Career Preferences Survey' AS survey_name
        , MAX(CASE WHEN label = 'Learn UUID' THEN value END) AS learn_uuid
        , MAX(CASE WHEN label = 'Email' THEN value END) AS email
        -- Standard CPS questions
        , MAX(CASE WHEN label ~ '1\\. Introduce yourself' THEN value END) AS introduce_yourself
        , MAX(CASE WHEN label ~ '2\\. Whare your your plans after' THEN value END) AS plans_after
        , MAX(CASE WHEN label ~ '3\\. What are your hobbies' THEN value END) AS hobbies
        , MAX(CASE WHEN label ~ '4\\. Where do you think you could most benefit' THEN value END) AS most_benefit
        , MAX(CASE WHEN label ~ '5\\. What, if any, barriers' THEN value END) AS barriers
        , MAX(CASE WHEN label ~ '6\\. Are you returning to work after a 12-month' THEN value END) AS returning_to_work
        , MAX(CASE WHEN label ~ '7\\. Is there anything else' THEN value END) AS anything_else
        -- Amazon-Specific CPS
        , MAX(CASE WHEN label = 'Which of these best describes your current Employment Status at Amazon?' THEN value END) AS amazon_current_employment_cps
        , MAX(CASE WHEN label ~ 'Job Title at Amazon' THEN value END) AS amazon_current_job_title_cps
        , MAX(CASE WHEN label IN (
            'What are your plans after Flatiron School? (Articulate your near term career goals)',
            'Which of these options best represents your career goals? (Select all that apply.)') THEN value END) AS amazon_plans_after_cps
        , MAX(CASE WHEN label ~ 'use the most support' THEN value END) AS amazon_use_support
        , MAX(CASE WHEN label = 'On your ideal timeline, when do you have a new job?' THEN value END) AS amazon_when_have_new_job
        , MAX(CASE WHEN label = 'At Flatiron School, we assume all students will partner with Career Services unless you specifically tell us you prefer to opt-out.' THEN value END) AS amazon_opt_out
        , MAX(CASE WHEN label = 'If you chose ‘Opt Out’ please select the reason that best represents your decision to opt out.' THEN value END) AS amazon_opt_out_reason
        , MAX(CASE WHEN label = 'Company Name' THEN value END) AS amazon_company_name
        , MAX(CASE WHEN label = 'Job Title' THEN value END) AS amazon_job_title
        , NVL(MAX(CASE WHEN label = 'Which of these best describes your preferred work location? (Select all that apply)' THEN value END), 'N/A') AS amazon_work_location
        , NVL(MAX(CASE WHEN label = 'Preferred Job Structure (Select all that apply)' THEN value END), 'N/A') AS amazon_job_structure
        , MAX(CASE WHEN label = 'How would you rate your level of readiness to enter your job search?  (Professional Tech Identity, Online Branding/LinkedIn, Networking, Resume)' THEN value END) AS amazon_readiness_cps
        , MAX(CASE WHEN label = 'How would you rate your level of motivation to pursue a new career in your discipline of study?' THEN value END) AS amazon_motivation_cps
        , MAX(CASE WHEN label = 'How would you rate your level of effort in your job search up to now?' THEN value END) AS amazon_level_of_effort
    FROM {{ params.table_refs["formstack.amazon_career_choice_career_preferences_merged"] }}
    GROUP BY 
        form_id
        , submission_id
        , submission_timestamp
)
SELECT 
    *
    -- Amazon Only Phase 1
    , NULL AS amazon_current_employment_p1
    , NULL AS amazon_current_job_title_p1
    , NULL AS amazon_plans_after_p1
    , NULL AS amazon_readiness_p1
    , NULL AS amazon_motivation_p1
    , NULL AS amazon_gender
    , NULL AS amazon_race
    , NULL AS amazon_ethnicity
    , NULL AS amazon_education
    , NULL AS amazon_identities
    -- Amazon-Specific CPS
    , NULL AS amazon_current_employment_cps
    , NULL AS amazon_current_job_title_cps
    , NULL AS amazon_plans_after_cps
    , NULL AS amazon_use_support
    , NULL AS amazon_when_have_new_job
    , NULL AS amazon_opt_out
    , NULL AS amazon_opt_out_reason
    , NULL AS amazon_company_name
    , NULL AS amazon_job_title
    , NULL AS amazon_work_location
    , NULL AS amazon_job_structure
    , NULL AS amazon_readiness_cps
    , NULL AS amazon_motivation_cps
    , NULL AS amazon_level_of_effort
FROM cps

UNION ALL 

SELECT 
    NULL AS form_id
    , NULL AS submission_id
    , GREATEST(a.submission_timestamp, b.submission_timestamp) AS submission_timestamp
    , 'Amazon Career Choice Phase 1 & Career Preferences Surveys' AS survey_name
    , NVL(a.learn_uuid, b.learn_uuid) AS learn_uuid
    , NVL(a.email, b.email) AS email
    , NULL AS discipline
    , NULL AS pace
    , NULL AS start_date
    , NULL AS linkedin_url
    , NULL AS blog_url
    , NULL AS webpage_portfolio_url
    -- Standard CPS questions
    , b.introduce_yourself
    , b.plans_after
    , b.hobbies
    , b.most_benefit
    , b.barriers
    , b.returning_to_work
    , b.anything_else
    , NULL AS readiness_cps
    , NULL AS motivation_cps
    -- Alumni-only fields
    , NULL AS alumni_career_coach
    , NULL AS alumni_same_coach
    , NULL AS alumni_catalyst
    , NULL AS alumni_support_options
    -- Amazon Only Phase 1
    , a.amazon_current_employment_p1
    , a.amazon_current_job_title_p1
    , a.amazon_plans_after_p1
    , a.amazon_readiness_p1
    , a.amazon_motivation_p1
    , a.amazon_gender
    , a.amazon_race
    , a.amazon_ethnicity
    , a.amazon_education
    , a.amazon_identities
    -- Amazon-Specific CPS
    , b.amazon_current_employment_cps
    , b.amazon_current_job_title_cps
    , b.amazon_plans_after_cps
    , b.amazon_use_support
    , b.amazon_when_have_new_job
    , b.amazon_opt_out
    , b.amazon_opt_out_reason
    , b.amazon_company_name
    , b.amazon_job_title
    , b.amazon_work_location
    , b.amazon_job_structure
    , b.amazon_readiness_cps
    , b.amazon_motivation_cps
    , b.amazon_level_of_effort
FROM amazon_p1 a 
FULL OUTER JOIN amazon_cps b 
    ON a.learn_uuid = b.learn_uuid
{% endblock %}
