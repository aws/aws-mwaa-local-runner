{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    form_id
    , submission_id
    , survey_name
    , submission_timestamp
    -- Reviewer Info
    , learn_uuid
    , email
    , coach_name
    -- Student Info
    , student_name
    , student_email
    , student_discipline
    -- Submission
    , submission_type
    , submission_date
    , submission_url
    , requested_11
    , additional_info
    -- LinkedIn Rubric - Checklist items
    , linkedin_privacy_settings
    , linkedin_privacy_settings_c
    , linkedin_photo
    , linkedin_photo_c
    , linkedin_profile_url
    , linkedin_profile_url_c
    , linkedin_contact_info
    , linkedin_contact_info_c
    , linkedin_headline
    , linkedin_headline_c
    , linkedin_summary
    , linkedin_summary_c
    , linkedin_keywords
    , linkedin_keywords_c
    , linkedin_experience
    , linkedin_experience_c
    , linkedin_education
    , linkedin_education_c
    , linkedin_licenses
    , linkedin_licenses_c
    , linkedin_featured_section
    , linkedin_featured_section_c
    , linkedin_skills
    , linkedin_skills_c
    , NVL(linkedin_score, linkedin_score2) AS linkedin_score
    -- LinkedIn Rubric - Bonus items
    , linkedin_groups
    , linkedin_groups_c
    , linkedin_endorsements
    , linkedin_endorsements_c
    , linkedin_post_content
    , linkedin_post_content_c
    , linkedin_follow
    , linkedin_follow_c
    -- Resume Rubric - Checklist items
    , resume_template
    , resume_template_c
    , resume_contact
    , resume_contact_c
    , resume_technical
    , resume_technical_c
    , resume_projects
    , resume_projects_c
    , resume_experience
    , resume_experience_c
    , resume_education
    , resume_education_c
    , resume_summary
    , resume_summary_c
    , resume_1_page
    , resume_1_page_c
    , resume_emphasis
    , resume_emphasis_c
    , resume_bullets
    , resume_bullets_c
    , resume_grammar
    , resume_grammar_c
    , resume_score
    , loom_video_url
    , takeaways_next_steps
FROM 
    (
        SELECT form_id, submission_id, 'LinkedIn & Resume MVP Rubric' AS survey_name, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.linkedin_resume_rubric_merged"] }}
    ) PIVOT (
        MAX(value) for label IN (
            -- Reviewer Info
            'Learn UUID' AS learn_uuid
            , 'Email' AS email
            , 'Coach Name' AS coach_name
            -- Student Info
            , 'Student Name' AS student_name
            , 'Student Email' AS student_email
            , 'Student Discipline' AS student_discipline
            -- Submission
            , 'Submission Type' AS submission_type
            , 'Submission Date' AS submission_date
            , 'Submission URL' AS submission_url
            , 'Requested 11' AS requested_11
            , 'Additional Info' AS additional_info
            -- LinkedIn Rubric - Checklist items
            , 'Change your Privacy Settings to Public' AS linkedin_privacy_settings
            , 'Change your Privacy Settings to Public Comments' AS linkedin_privacy_settings_c
            , 'Add a Professional Profile Photo' AS linkedin_photo
            , 'Add a Professional Profile Photo Comments' AS linkedin_photo_c
            , 'Customize Your Public Profile URL' AS linkedin_profile_url
            , 'Customize Your Public Profile URL Comments' AS linkedin_profile_url_c
            , 'Fill out your Complete Contact Information' AS linkedin_contact_info
            , 'Fill out your Complete Contact Information Comments' AS linkedin_contact_info_c
            , 'Create your Headline' AS linkedin_headline
            , 'Create your Headline Comments' AS linkedin_headline_c
            , 'Write a Summary' AS linkedin_summary
            , 'Write a Summary Comments' AS linkedin_summary_c
            , 'Add in Keywords to Headline, Summary, Experience, and Education sections relevant to your field of study' AS linkedin_keywords
            , 'Add in Keywords to Headline, Summary, Experience, and Education sections relevant to your field of study Comments' AS linkedin_keywords_c
            , 'Add Experience' AS linkedin_experience
            , 'Add Experience Comments' AS linkedin_experience_c
            , 'Add Education including Flatiron School' AS linkedin_education
            , 'Add Education including Flatiron School Comments' AS linkedin_education_c
            , 'Add Licenses/Certifications' AS linkedin_licenses
            , 'Add Licenses/Certifications Comments' AS linkedin_licenses_c
            , 'Add a Featured Section to showcase projects, demos, your blog, or other accomplishments' AS linkedin_featured_section
            , 'Add a Featured Section to showcase projects, demos, your blog, or other accomplishments Comments' AS linkedin_featured_section_c
            , 'Add/Edit Skills Section the skills relevant to your field of study' AS linkedin_skills
            , 'Add/Edit Skills Section the skills relevant to your field of study Comments' AS linkedin_skills_c
            , 'LinkedIn Total Score (33 max)' AS linkedin_score
            , 'LinkedIn Total Score (30 max)' AS linkedin_score2
            -- LinkedIn Rubric - Bonus items
            , 'Join Linkedin Groups to meet and engage with people in your field, share content, and learn about jobs' AS linkedin_groups
            , 'Join Linkedin Groups to meet and engage with people in your field, share content, and learn about jobs Comments' AS linkedin_groups_c
            , 'Request Recommendations and Skills Endorsements from Flatiron instructors and peers, former colleagues, managers' AS linkedin_endorsements
            , 'Request Recommendations and Skills Endorsements from Flatiron instructors and peers, former colleagues, managers Comments' AS linkedin_endorsements_c
            , 'Post Content (Status Update, Comment, Like, Share) to Increase your Profile Visibility on LinkedIn' AS linkedin_post_content
            , 'Post Content (Status Update, Comment, Like, Share) to Increase your Profile Visibility on LinkedIn Comments' AS linkedin_post_content_c
            , 'Follow Companies, Influencers of Interest in your field of study' AS linkedin_follow
            , 'Follow Companies, Influencers of Interest in your field of study Comments' AS linkedin_follow_c
            -- Resume Rubric - Checklist items
            , 'Use of Preformatted Resume Template (Recommended)' AS resume_template
            , 'Use of Preformatted Resume Template (Recommended) Comments' AS resume_template_c
            , 'Section: Your Name and Contact Info' AS resume_contact
            , 'Section: Your Name and Contact Info Comments' AS resume_contact_c
            , 'Section: Technical Skills' AS resume_technical
            , 'Section: Technical Skills Comments' AS resume_technical_c
            , 'Section: Technical Projects' AS resume_projects
            , 'Section: Technical Projects Comments' AS resume_projects_c
            , 'Section: Work Experience' AS resume_experience
            , 'Section: Work Experience Comments' AS resume_experience_c
            , 'Section: Education (include Flatiron School)' AS resume_education
            , 'Section: Education (include Flatiron School) Comments' AS resume_education_c
            , 'Section: Profile Headline / Summary that concisely states your tech focus, background and experience, skills highlights, and future impact' AS resume_summary
            , 'Section: Profile Headline / Summary that concisely states your tech focus, background and experience, skills highlights, and future impact Comments' AS resume_summary_c
            , 'Keep resume to 1-page (≤2 pages for Cyber)' AS resume_1_page
            , 'Keep resume to 1-page (≤2 pages for Cyber) Comments' AS resume_1_page_c
            , 'Use Italics, CAPITALS, Bold, and Borderlines for emphasis (sparingly)' AS resume_emphasis
            , 'Use Italics, CAPITALS, Bold, and Borderlines for emphasis (sparingly) Comments' AS resume_emphasis_c
            , 'Strong bullets = Action verb + task + result' AS resume_bullets
            , 'Strong bullets = Action verb + task + result Comments' AS resume_bullets_c
            , 'Proper Formatting, Grammar, Spelling' AS resume_grammar
            , 'Proper Formatting, Grammar, Spelling Comments' AS resume_grammar_c
            , 'Rubric Total Score (33 max)' AS resume_score
            , 'Loom Video URL' AS loom_video_url
            , 'Takeaways / Next Steps' AS takeaways_next_steps
        )
    )
{% endblock %}
