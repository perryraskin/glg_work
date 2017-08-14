/*

There are two queries which do essential the same thing. They're not exactly in sync but are close. 

There was an issue with the audit table where 'schedule' rows were being erroneously written to the table every few seconds. Query 1 attempts 
to resolve this with the following line "and DATEDIFF(s, cpa2.create_date, cpa.create_date) < 60 ".

Also, my understanding is, some technical change took place at the end of last october. I'm not certain what the change was, but I've 
been limiting my queries to Nov, 2016 and later.

*/

-- Query 1 of 2--
use glglive_daily;
set nocount on;
set transaction isolation level read uncommitted
declare @from as nvarchar(10) = '11/01/2016'
;WITH cte as (
select 
        consultation_participant_id,
        (select min(consultation_participant_audit_id) 
          from consult.consultation_participant_audit cpa3 
         where cpa3.consultation_participant_id = cpa.consultation_participant_id
           and cpa3.audit_info = 'SCHEDULED') 
            as consultation_participant_audit_id
  from consult.consultation_participant_audit cpa
 where cpa.create_date > @from
   and cpa.audit_info = 'SCHEDULED'
 group by consultation_participant_id 
),
cte_bu1  (client_type_id, bu) as (
select   1, 'fs'
union
select   2, 'fs'
union
select   3, 'fs'
union
select   4, 'fs'
union
select   5, 'fs'
union
select  11, 'fs'
union
select  44, 'fs'
union
select  47, 'fs'
union
select  48, 'fs'
union
select  49, 'fs'
union
select  50, 'fs'
union
select  53, 'fs'
union
select  62, 'fs'
union
select  63, 'fs'
union
select  64, 'fs'
union
select  65, 'fs'
union
select  66, 'fs'
union
select  67, 'fs'
union
select  68, 'fs'
union
select  69, 'fs'
union
select  70, 'fs'
union
select  71, 'fs'
union
select  72, 'fs'
union
select  73, 'fs'
union
select  74, 'fs'
union
select  75, 'fs'
union
select  76, 'fs'
union
select  77, 'fs'
union
select  78, 'fs'
union
select  79, 'fs'
union
select  80, 'fs'
union
select  36, 'corporate'
union
select  58, 'corporate'
union
select  59, 'corporate'
union
select  60, 'corporate'
union
select  61, 'corporate'
union
select  33, 'psf'
union
select  37, 'glgNonClient'
union
select  45, 'law'
union
select  52, 'lifeSciences'
union
select  54, 'socialImpact'
union
select  56, 'glgShare'
),
cte_bu  (client_type_id, bu) as (
  select * from cte_bu1
 union
 select client_type_id, client_type from client_type ct
 where not exists (select 1 from cte_bu1 where cte_bu1.client_type_id = ct.client_type_id)
)
select scheduled_on, app, scheduled_by, bu, count(*) as total_originally_scheduled, sum(rescheduled) as total_rescheduled, 
sum(cancelled) as total_cancelled
from (
    select  cast(cpa.create_date as date) scheduled_on, 
        case when oneclick.audit_info = 'ONE_CLICK_SCHEDULING'
        then 'one-click'
        when  oneclick.audit_info = 'ONE_CLICK_SCHEDULING_CARD'
        then 'go-engage-one-click'
        else cpa.app_name
         end as app,
        case when con.person_id is not null
        then 'client'
        when ut.person_id is not null
        then 'rm'
        else 'cm'
         end as scheduled_by,
        case when (select count(*) from consult.consultation_participant_audit cpa2 
                    where cpa.consultation_participant_id = cpa2.consultation_participant_id
                      and DATEDIFF(s, cpa2.create_date, cpa.create_date) < 60 
                      and cpa2.audit_info = 'SCHEDULED' ) > 1 

         then 1
         else 0
          end as rescheduled,
        case when (select count(*) from consult.consultation_participant_audit cpa4
                    where cpa.consultation_participant_id = cpa4.consultation_participant_id
                        and cpa4.audit_info = 'CANCELLED') >= 1
        then 1
        else 0
            end as cancelled,
  bu.bu
  from cte
  join consult.consultation_participant_audit cpa on cpa.consultation_participant_audit_id = cte.consultation_participant_audit_id
  join consult.consultation_participant cp on cpa.consultation_participant_id = cp.consultation_participant_id
  join vw_client_consultation mrcr on mrcr.consultation_id = cp.consultation_id
  join client_type_relation ctr on ctr.client_id = mrcr.client_id
  join cte_bu bu on bu.client_type_id = ctr.client_type_id
  left join  council_member cm on cm.person_id = cpa.created_by_person_id
  left join  contact con on con.person_id = cpa.created_by_person_id
  left join  user_table ut on ut.person_id = cpa.created_by_person_id
  left outer join consult.consultation_participant_audit oneclick 
          on oneclick.consultation_participant_id = cpa.consultation_participant_id
         and DATEDIFF(s, oneclick.create_date, cpa.create_date) < 10
         and oneclick.audit_info in ('ONE_CLICK_SCHEDULING', 'ONE_CLICK_SCHEDULING_CARD')
  where cpa.create_date > @from
) o
group by scheduled_on, app, scheduled_by, bu
order by scheduled_on asc, app



-- Query 2 of 2 --

declare @beta_start datetime = '7/1/2016';
with cte_pss as (
  select top 1000 p.email, p.person_id, u.user_id
  from user_table u
  join person p on u.person_id = p.person_id and u.active_ind = 1
  left join GLG_DEPARTMENT D on U.DEPARTMENT = D.GLG_DEPARTMENT_ID 
  where (p.first_name + ' ' + p.last_name IN
          ('Austin Rittenberry', 'Brianna Barcena', 'Danielle Slater', 'Demi Young', 'Emily Colwell' , 'Erin DeGeer', 'Kristen Smaila', 'Lauren Jacobs', 'Mary Albright', 'Patrick Rosen'
               , 'Samantha Byrd', 'Tahjai Conkerite', 'Heather Loflin', 'Lena Armuth', 'Lesley Ornelas', 'Nikitha Aithal'
               , 'Ossian Olausson', 'Shane Summers', 'Kacie Simpson', 'Aoife Murphy')
          AND p.email LIKE '%@glgroup.com'
)
  or d.department_name = 'project support'
) , 
cte_rm as (
  select p.email, p.person_id, u.user_id
  from user_table u
  join person p on u.person_id = p.person_id and u.ACTIVE_IND = 1
  where u.user_id not in (0, 3621) 
) , 
pss_involved as (
       select distinct meeting_id
       from meeting_council_member_status_relation mcmsr
       join cte_pss pss on mcmsr.created_by = pss.user_id
       where mcmsr.create_date > @beta_start
) , 
new_workflow as (
       select distinct meeting_id
       from meeting_council_member_status_relation mcmsr
       where
             mcmsr.meeting_participant_status_id in (47,48,49,50)
             and mcmsr.create_date > @beta_start
) , 
one_click as (
       select consultation_participant_id
       from consult.consultation_participant_audit
       where create_date > @beta_start
       and audit_info = 'ONE_CLICK_SCHEDULING'
)
select
c.created_date
, case 
       when pi.meeting_id is not null then 'pss' 
       when rma.user_id is not null then 'rm' 
       when oc.consultation_participant_id is not null then 'oneclick' 
       else 'system' 
   end as source,
case when nw.meeting_id is not null then 'av req' else 'direct' end as workflow 
, case when mcmsr.meeting_participant_status_id = 6 then 1 else 0 end as schedule 
, mcmsr.meeting_id
, c.PRIMARY_RM_PERSON_ID
, case when meeting_participant_status_id = 6 then 1 else 0 end schedules 
, case when meeting_participant_status_id = 58 then 1 else 0 end cancels 
, pss.user_id scheduler_vega_id
, case when meeting_participant_status_id = 6 then mcmsr.create_date else null end as scheduled_date 
into #temp2
from meeting_council_member_status_relation mcmsr
join consult.consultation_participant cp on cp.meeting_id = mcmsr.meeting_id
join consult.consultation c on c.consultation_id = cp.consultation_id
join cte_rm rm on rm.person_id = c.primary_rm_person_id
left join cte_pss pss on pss.user_id = mcmsr.created_by
left join pss_involved pi on mcmsr.meeting_id = pi.meeting_id
left join new_workflow nw on mcmsr.meeting_id = nw.meeting_id
left join one_click oc on cp.consultation_participant_id = oc.consultation_participant_id
left join cte_rm rma on rma.user_id = mcmsr.created_by
where meeting_participant_status_id in (6, 58, 52) -- scheduled, cancelled, call_occurred 
and c.created_date >= '1/1/2016'
order by 1;