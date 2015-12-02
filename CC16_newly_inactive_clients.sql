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



select distinct d.client_id from
(select * from last_dose_report_detail d 
  inner join 
    (
      select h_id,location_number,update_flag,max(run_id) run_id 
      from hid_log 
      where report_type = 'Last Dose' 
      and update_flag < 2 --current update flag
      and h_id = (Select h_id from process_log where flag = 'RUNID' and run_id = 114532)--input_run_id
      group by h_id,location_number,update_flag
    ) h
  on h.run_id = d.run_id and canines > 0
) d
left join (
  select * from last_dose_clients c 
  where run_id = 114532 --input run_id
  and canines <> 0
) a on a.client_id = d.client_id
where a.client_id is null;



select * from last_dose_clients c 
where run_id = 114532 --input run_id
;


select cc16.CC.RUN_CLINIC(cc16.cc.init(9368,11266,'Last Dose',sysdate,2)) from dual;
select cc16.CC.RUN_CLINIC(cc16.cc.init(11325,11361,'Last Dose',sysdate,2)) from dual;
select cc16.CC.RUN_CLINIC(cc16.cc.init(814,11325,'Last Dose',sysdate,2)) from dual;
select cc16.CC.RUN_CLINIC(cc16.cc.init(9791,12359,'Last Dose',sysdate,2)) from dual;
select cc16.CC.RUN_CLINIC(cc16.cc.init(7543,11448,'Last Dose',sysdate,2)) from dual;
select * from process_log order by run_date desc;
