select h_id,location_number,update_flag,max(run_id) run_id 
from hid_log 
where report_type = 'Last Dose' and update_flag < 2 --current update flag
and h_id = 9791 --input_hid
group by h_id,location_number,update_flag;

select *
from hid_log 
where report_type = 'Last Dose' and update_flag < 2 --current update flag
--and h_id = 9589 --input_hid
order by run_date;


create table last_dose_nic as
select distinct  '2DT' record_type,d.location_number,d.client_id,cast (0 as number) run_id, cast (0 as number) update_flag from
(select * from last_dose_report_detail d 
  inner join 
    (
      select h_id,location_number,update_flag,max(run_id) run_id 
      from hid_log 
      where report_type = 'Last Dose' 
      and update_flag < 2 --current update flag
      and h_id = (Select h_id from process_log where flag = 'RUNID' and run_id = 114556)--input_run_id
      group by h_id,location_number,update_flag
    ) h
  on h.run_id = d.run_id and canines > 0
) d
left join (
  select * from last_dose_clients c 
  where run_id = 114556 --input run_id
  and canines <> 0
) a on a.client_id = d.client_id
where a.client_id is null
;

alter table last_dose_nic add primary key(run_id,client_id);

select * from last_dose_clients c 
where run_id = 114559 --input run_id
;
114558
114559
114557
114556


select cc16.CC.RUN_CLINIC(cc16.cc.init(9368,11266,'Last Dose',sysdate,2)) from dual;
select cc16.CC.RUN_CLINIC(cc16.cc.init(11325,11361,'Last Dose',sysdate,2)) from dual;
select cc16.CC.RUN_CLINIC(cc16.cc.init(814,11325,'Last Dose',sysdate,2)) from dual;
select cc16.CC.RUN_CLINIC(cc16.cc.init(9791,12359,'Last Dose',sysdate,2)) from dual;
select cc16.CC.RUN_CLINIC(cc16.cc.init(7543,11448,'Last Dose',sysdate,2)) from dual;
select * from process_log order by run_date desc;

select * from last_dose_nic;

select '1HD' RECORD_TYPE,
'12/02/2015' LOCATION_NUMBER,
cast(update_flag as varchar2(20)) CLIENT_ID,
'2016' RUN_ID,
'2'  UPDATE_FLAG
from last_dose_nic
union
select RECORD_TYPE,
cast(LOCATION_NUMBER as varchar2(20)),
cast(CLIENT_ID as varchar2(20)),
cast(RUN_ID as varchar2(20)),
cast(UPDATE_FLAG as varchar2(20)) from last_dose_nic;

select * from cooked.client c LEFT JOIN Vetstreet.Pgsql_Pms_Client_Code_Lkup Ccl ON C.Pms_Client_Code_Id = Ccl.Id 
where client_id in (133330344) and h_id = 11325;

select * from cooked.patient c
--LEFT JOIN Vetstreet.Pgsql_Pms_Client_Code_Lkup Ccl ON C.Pms_Client_Code_Id = Ccl.Id 
where client_id in (133330344) and h_id = 11325;