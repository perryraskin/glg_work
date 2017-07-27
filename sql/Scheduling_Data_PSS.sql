---//---- Getting all PSS team members ---------------------
 Use glglive_daily;
 
 declare @beta_start datetime = '1/1/2017';
  
 ;with cte_pss as (
     SELECT
    p.first_name + ' ' + p.last_name AS Pss_name,
      p.email,
      p.person_id,
      u.user_id
    FROM user_table u
      JOIN person p
        ON u.person_id = p.person_id
           AND u.active_ind = 1
      LEFT JOIN GLG_DEPARTMENT D
        ON U.DEPARTMENT = D.GLG_DEPARTMENT_ID
    WHERE (p.first_name + ' ' + p.last_name IN ('Austin Rittenberry',
'Alex Jordan',
'Brianna Barcena',
'Demi Young',
'Emily Colwell ',
'Erin DeGeer',
'Heather Loflin ',
'Jazma Johnson',
'Kacie Simpson',
'Kristen Smaila ',
'Kyndal Hammock ',
'Lauren Jacobs',
'Lena Armuth',
'Lesley C. Ornelas',
'Lucy Redshaw',
'Mary Albright ',
'Nikitha Aithal',
'Ossian Olausson',
'Patrick Rosen',
'Samantha Byrd',
'Shane Summers',
'Tahjai Conkerite')
AND p.email LIKE '%@glgroup.com') OR 
d.department_name = 'project support'
)
---------//--------- All activity history where a pss team member was involved --------------------
,pss_involved
as
(
  select distinct meeting_id
  from meeting_council_member_status_relation mcmsr 
  join cte_pss pss on mcmsr.created_by = pss.user_id
  where mcmsr.create_date > @beta_start
)
---------//--------- All scheduling done by pss after recieving the requets --------------------
,cte_request_scheduling_activity_by_pss
  as
(
    select 
	Row_Number() Over (Partition by mcmsr.Meeting_ID Order by mcmsr.Meeting_ID,mcmsr.create_date) AS RN,
	mcmsr.Meeting_ID,
	mcmsr.Created_by,
	pss.person_id,
	cp.consultation_participant_ID,
	mcmsr.create_date as scheduled_date,
	initial.CREATE_DATE as initial_request
    from meeting_council_member_status_relation mcmsr 
	join consult.consultation_participant cp on cp.meeting_id = mcmsr.meeting_id
    join consult.consultation c on c.consultation_id = cp.consultation_id
	join pss_involved pi  ON pi.Meeting_id = mcmsr.Meeting_Id
    join cte_pss pss on pss.user_id = mcmsr.created_by 
	Outer apply(
	Select top 1 * from meeting_council_member_status_relation
	where meeting_id = mcmsr.Meeting_ID and created_by = mcmsr.created_by
	and meeting_participant_status_id in (47,48,61) order by create_date asc
	) initial  --- For Individuals who were requested and scheduled the calls
    where mcmsr.meeting_participant_status_id in (6) -- ,58,52) -- scheduled, cancelled, call_occurred
      and c.created_date > @beta_start
      and pi.meeting_id is not null 
	  and initial.CREATE_DATE < mcmsr.create_date -- request before schedule
)
, request_assigned as (
Select p.created_by
,DATEADD(Week, DATEDIFF(week, 0, p.initial_request), -1) week
,Avg(Datediff(hh,p.initial_request,p.scheduled_date)) AS Initial_turnaround
,Avg(Case when p1.scheduled_date is not null then Datediff(hh,p.scheduled_date,p1.scheduled_date) end) AS Second_turnaround
,Avg(Case when p2.scheduled_date is not null then Datediff(hh,p1.scheduled_date,p2.scheduled_date) end) AS Third_turnaround
from cte_request_scheduling_activity_by_pss p

left join cte_request_scheduling_activity_by_pss p1
ON p1.Meeting_id = p.Meeting_id and p1.Created_by = p.Created_by and p1.RN = 2

left join cte_request_scheduling_activity_by_pss p2
ON p2.Meeting_id = p.Meeting_id and p2.Created_by = p.Created_by and p2.RN = 3

where p.initial_request is not null and p.RN = 1
group by p.created_by
,DATEADD(Week, DATEDIFF(week, 0, p.initial_request), -1)
)
,cte_scheduling_activity_by_pss
  as
  (
    select c.created_date
    , mcmsr.meeting_id
	, mcmsr.create_date
    , c.primary_rm_person_id
    , pss.email
    , sc.glg_scheduling_person_id -- direct scheduling assignments
    from meeting_council_member_status_relation mcmsr 
    join consult.consultation_participant cp on cp.meeting_id = mcmsr.meeting_id
    join consult.consultation c on c.consultation_id = cp.consultation_id 
    join cte_pss pss on pss.user_id = mcmsr.created_by 
    join pss_involved pi on mcmsr.meeting_id = pi.meeting_id 
    left join SCHEDULING.CONSULTATION_PARTICIPANT SC 
      ON sc.consultation_participant_id = cp.consultation_participant_id
      AND  pss.person_id = SC.glg_scheduling_person_id
    where meeting_participant_status_id in (6) -- ,58,52) -- scheduled, cancelled, call_occurred
      and c.created_date > @beta_start
      and pi.meeting_id is not null 
)
,cte_scheduling_requests
  as
  (
    select c.created_date
    , mcmsr.meeting_id
    , pss.email
	, mcmsr.create_Date
    from meeting_council_member_status_relation mcmsr 
    join consult.consultation_participant cp on cp.meeting_id = mcmsr.meeting_id
    join consult.consultation c on c.consultation_id = cp.consultation_id 
    join cte_pss pss on pss.user_id = mcmsr.created_by 
    join pss_involved pi on mcmsr.meeting_id = pi.meeting_id 
    where mcmsr.meeting_participant_status_id IN (47,48,61)
      and c.created_date > @beta_start
      and pi.meeting_id is not null 
)
,cte_requests_by_date
  as
  (
    select DATEADD(Week, DATEDIFF(week, 0, create_date), -1) week, email,
    count(*) as requestCount
    from cte_scheduling_requests sr
    group by DATEADD(Week, DATEDIFF(week, 0, create_date), -1), email 
	  )
,cte_schedules_by_date
  as
  (
    select DATEADD(Week, DATEDIFF(week, 0, create_date), -1) week, email,
    count(*) as scheduleCount, 
    count(distinct meeting_id) count,
    count(glg_scheduling_person_id) as scheduling_assignments
    from cte_scheduling_activity_by_pss sa
    group by DATEADD(Week, DATEDIFF(week, 0, create_date), -1), email
  )

select s.week as Week, pss.Pss_Name AS PSS_Member_Name
, s.email as PSS_Member_Email,
isnull(r.requestCount,0) as Total_Requests_Recieved,
s.scheduleCount as Total_Scheduled_Calls, 
s.count as Unique_Calls_Scheduled,
isnull(req.Initial_turnaround,0) as Initial_Schedule_Avg_turnaround_In_Hours,
isnull(req.Second_Turnaround,0) as Second_Schedule_Avg_Turmaround_In_Hours,
isnull(req.Third_Turnaround,0) as Third_Schedule_Avg_Turmaround_In_Hours
  from cte_schedules_by_date s 
  left join cte_requests_by_date r on r.week = s.week and s.email = r.email
  left join cte_pss pss ON pss.email = s.email
  left join request_assigned req ON req.Created_by = pss.User_id and req.week = s.week
  where s.week <= getdate()
  order by s.week, pss.Pss_name asc