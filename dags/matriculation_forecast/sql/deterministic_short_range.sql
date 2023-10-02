{% extends "rotate_table.sql" %}
{% block query %}
WITH prework AS (
    select *, last_value(pct ignore nulls) over (partition by learn_uuid__c order by date rows unbounded preceding) as percent_complete
    from (
        select
            c.date
            , cc.learn_uuid__c
            , max(ch.newvalue__double / nullif(cc.total_steps__c, 0)::float) as pct
        from fis.calendar c
        join fis.rosters r
            on c.date between r.added_to_cohort_date::date and nvl(r.removed_from_cohort_date::date, r.cohort_start_date)
        left join stitch_salesforce.course__c cc
            on r.learn_uuid = cc.learn_uuid__c
        left join stitch_salesforce.course__history ch
            on cc.id = ch.parentid
            and c.date = ch.createddate::date
            and ch.field = 'Total_Steps_Completed__c'
        group by 
            c.date
            , cc.learn_uuid__c
    )
),
avg_matriculations AS (
    SELECT 
        matriculated_cohort_discipline AS discipline
        , matriculated_cohort_modality AS modality
        , COUNT(1)/COUNT(DISTINCT matriculated_cohort_name)::FLOAT AS avg_matriculations
    FROM fis.students
    WHERE matriculated_cohort_start_date BETWEEN current_date - INTERVAL '6 months' AND current_date
        AND institution_type = 'Consumer'
    GROUP BY 1,2
),
peak_commits AS (
    select
        c.date
        , r.cohort_name
        , count(distinct case 
            when r.committed_date <= nvl(r.removed_from_cohort_date::date, c.date)
            then r.learn_uuid end) as peak_commits
    from fis.calendar c
    join fis.rosters r
        on c.date between r.added_to_cohort_date::date and r.cohort_start_date
    join fis.students s 
        on r.learn_uuid = s.learn_uuid
    where (s.matriculation_date is null or s.matriculation_date >= r.cohort_start_date)
        and r.cohort_start_date <> '2099-12-25'
    group by 1, 2
),
preday1_rosters AS (
    select
        c.date
        , r.cohort_start_date
        , datediff(day, c.date, r.cohort_start_date) as days_until_start
        , r.cohort_name
        , r.discipline
        , r.modality
        , pc.peak_commits as cumulative_peak_commits
        , MAX(pc.peak_commits) OVER (PARTITION BY r.cohort_name) AS peak_commits
        , count(distinct r.learn_uuid)::float as on_roster
        , count(case when r.on_roster_day_1 then r.learn_uuid end) as on_roster_to_day_1
        , count(distinct case when r.committed_date::date <= c.date then r.learn_uuid end)::float as committed_or_enrolled
        , count(distinct case when r.committed_date::date <= c.date and r.on_roster_day_1 then r.learn_uuid end)::float as committed_or_enrolled_to_day_1
        , count(distinct 
            case when r.financially_cleared_date::date <= c.date
                and r.enrollment_agreement_cleared_date::date <= c.date
                and r.academically_cleared_date::date <= c.date
                then r.learn_uuid end)::float as enrolled
        , count(distinct 
            case when r.financially_cleared_date::date <= c.date
                and r.enrollment_agreement_cleared_date::date <= c.date
                and r.academically_cleared_date::date <= c.date
                and r.on_roster_day_1
                then r.learn_uuid end)::float as enrolled_to_day_1
        , m.matriculations
        , on_roster - committed_or_enrolled as registered
        , on_roster_to_day_1 - committed_or_enrolled_to_day_1 as registered_to_day_1
        , committed_or_enrolled - enrolled as committed
        , committed_or_enrolled_to_day_1 - enrolled_to_day_1 as committed_to_day_1
        , on_roster / m.matriculations as on_roster_to_matriculated
        , registered_to_day_1 / nullif(registered, 0) as registered_to_matriculated
        , committed_to_day_1 / nullif(committed, 0) as committed_to_matriculated
        , enrolled_to_day_1 / nullif(enrolled, 0) as enrolled_to_matriculated
        , on_roster_to_day_1 / m.matriculations as cumulative_matriculation_ratio
        , count(distinct case when NVL(p.percent_complete, 0) < 0.25 then r.learn_uuid end)::float as pw_lt_25
        , count(distinct case when p.percent_complete >= 0.25 and p.percent_complete < 0.6 then r.learn_uuid end)::float as pw_25_to_60
        , count(distinct case when p.percent_complete >= 0.6 and p.percent_complete < 0.85 then r.learn_uuid end)::float as pw_60_to_85
        , count(distinct case when p.percent_complete >= 0.85 then r.learn_uuid end)::float as pw_gt_85
    from fis.calendar c
    join fis.rosters r
        on c.date between r.added_to_cohort_date::date and nvl(r.removed_from_cohort_date, r.cohort_start_date)
    join fis.students s 
        on r.learn_uuid = s.learn_uuid
        and nvl(r.cohort_start_date <= s.matriculation_date, true)
    join peak_commits pc 
        on r.cohort_name = pc.cohort_name
        and c.date = pc.date
    left join 
        (
            select matriculated_cohort_name, nullif(count(1), 0)::float as matriculations
            from fis.students
            group by 1
        ) m
        on r.cohort_name = m.matriculated_cohort_name
    left join prework p
        on r.learn_uuid = p.learn_uuid__c
        and c.date = p.date
    where
        c.date <= least(r.cohort_start_date, current_date)
        and r.pacing <> 'Self Paced'
        and r.campus <> 'Amazon'
    group by
        c.date
        , r.cohort_start_date
        , datediff(day, c.date, r.cohort_start_date)
        , r.cohort_name
        , r.discipline
        , r.modality
        , pc.peak_commits
        , m.matriculations
),
peak_commit_matric_ratios AS (
    select
        days_until_start
        , discipline
        , modality
        , avg(matriculations / nullif(peak_commits, 0)::float) as avg_peak_commit_to_matriculate
        , avg(cumulative_peak_commits / nullif(peak_commits, 0)::float) as avg_cumulative_peak_commit_ratio
    from preday1_rosters
    where days_until_start between 0 and 90
        and cohort_start_date between current_date - interval '12 months' and current_date
    group by 1, 2, 3
),
roster_ratios AS (
    select
        days_until_start
        , discipline
        , modality
        --, roster_to_matriculation_ratio
        , avg(avg_on_roster_to_matriculated::float) over (partition by discipline, modality order by days_until_start rows between 2 preceding and 2 following) as roster_to_matriculation_ratio
        , avg(avg_registered_to_matriculated::float) over (partition by discipline, modality order by days_until_start rows between 2 preceding and 2 following) as registered_to_matriculation_ratio
        , avg(avg_committed_to_matriculated::float) over (partition by discipline, modality order by days_until_start rows between 2 preceding and 2 following) as committed_to_matriculation_ratio
        , avg(avg_enrolled_to_matriculated::float) over (partition by discipline, modality order by days_until_start rows between 2 preceding and 2 following) as enrolled_to_matriculation_ratio
        , avg(cumulative_matriculation_ratio::float) over (partition by discipline, modality order by days_until_start rows between 2 preceding and 2 following) as cumulative_matriculation_ratio
    from
        (
            select
                days_until_start
                , discipline
                , modality
                , avg(on_roster_to_matriculated::float) as avg_on_roster_to_matriculated
                , avg(registered_to_matriculated::float) as avg_registered_to_matriculated
                , avg(committed_to_matriculated::float) as avg_committed_to_matriculated
                , avg(enrolled_to_matriculated::float) as avg_enrolled_to_matriculated
                , avg(cumulative_matriculation_ratio::float) as cumulative_matriculation_ratio
            from preday1_rosters
            where
                case
                    when discipline in ('Software Engineering', 'Data Science')
                    then cohort_start_date between current_date - interval '6 months' and current_date
                    else cohort_start_date between current_date - interval '12 months' and current_date
                end
            group by
                days_until_start
                , discipline
                , modality
        )
    where days_until_start <= 100
)
select
    p.date
    , CASE
        WHEN p.cohort_start_date < CURRENT_DATE
        THEN p.date = p.cohort_start_date
        WHEN p.cohort_start_date >= CURRENT_DATE
        THEN p.date = CURRENT_DATE - 1
        ELSE FALSE END AS is_most_relevant
    , p.cohort_start_date
    , p.days_until_start
    , p.cohort_name
    , p.discipline
    , p.modality
    , p.cumulative_peak_commits
    , p.peak_commits
    , CASE 
        WHEN p.days_until_start <= 21 THEN p.peak_commits 
        ELSE p.cumulative_peak_commits / nullif(pc.avg_cumulative_peak_commit_ratio, 0)
        END as expected_peak_commits
    , p.on_roster
    , p.registered
    , p.committed
    , p.enrolled
    , p.matriculations
    , p.pw_lt_25
    , p.pw_25_to_60
    , p.pw_60_to_85
    , p.pw_gt_85
    , rr.roster_to_matriculation_ratio
    , rr.registered_to_matriculation_ratio
    , rr.committed_to_matriculation_ratio
    , rr.enrolled_to_matriculation_ratio
    , rr.cumulative_matriculation_ratio
    , pc.avg_cumulative_peak_commit_ratio
    , pc.avg_peak_commit_to_matriculate
    , p.on_roster / nullif(rr.roster_to_matriculation_ratio, 0) AS on_roster_projection
    , (p.registered * nvl(rr.registered_to_matriculation_ratio, 0) +
        p.committed * nvl(rr.committed_to_matriculation_ratio, 0) +
        p.enrolled * nvl(rr.enrolled_to_matriculation_ratio, 0)) / nullif(rr.cumulative_matriculation_ratio, 0) AS weighted_roster_projection
    , expected_peak_commits * pc.avg_peak_commit_to_matriculate as peak_commit_projection
    , case
        when p.days_until_start <= 7 then pw_gt_85 + pw_60_to_85/2.0
        when p.days_until_start <= 14 then pw_gt_85 + pw_60_to_85 + pw_25_to_60/2.0
        when p.days_until_start <= 21 then pw_gt_85 + pw_60_to_85 + pw_25_to_60
        else p.on_roster / nullif(rr.roster_to_matriculation_ratio, 0) END AS pw_progress_projection
    , am.avg_matriculations
from preday1_rosters p
left join peak_commit_matric_ratios pc
    on p.days_until_start = pc.days_until_start
    and p.discipline = pc.discipline
    and p.modality = pc.modality
left join roster_ratios rr 
    on p.days_until_start = rr.days_until_start
    and p.discipline = rr.discipline
    and p.modality = rr.modality
left join avg_matriculations am
    on p.discipline = am.discipline
    and p.modality = am.modality
{% endblock %}