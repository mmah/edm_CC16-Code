
PACKAGE BODY cc AS
 
 
function CHECK_XREF (input_hid in number, input_location_number in number) return number is  
PRAGMA AUTONOMOUS_TRANSACTION;  
 
hid_not_in_xref EXCEPTION;  
check_in_dbraw number(1); -- temp storage for count value if clinic is in PROD   
check_in_xref number(1); -- temp storage for count value if is in the XREF 
 
begin
 
change_date_format;
 
-- assign the 2 inputs to the global variables. Not entirely necessary here. 
-- If this function is run by itself there's no point in attaching them to the global variables.  
-- If it's run as a call from run_clinic they will already be assigned. 
-- however if we ever want to log messages, by calling the log functions from this function   
-- they will need to be assigned here if this function is run independently. 
gv_input_hid := input_hid;  
gv_input_location_number := input_location_number;  
 
 
 
-- The order here is crucial. The highest level must be checked first. 
 
 
----- CHECK IN XREF ------------------------------------------------------------------------- 
-- If the clinic is not in the XREF it can't be checked to see if it's key and it can't be determined   
-- which database it's in.  
-- Assign a 1 to check_in_xref if the clinic is in the XREF  
-- Assign a 0 to check_in_xref if the clinic is not in the XREF  
select count(*) into check_in_xref from ACCOUNTS  
where hid = gv_input_hid
and location_number = gv_input_location_number ;
 
-- If the clinic is not in the XREF an exception is raised.  
if check_in_xref = 0 then   
  raise hid_not_in_xref;
end if;  
--------------------------------------------------------------------------------------------- 
 
 
commit;  
-- return > 0 if the clinic passes all tests and can be processed in this database.  
return(1); 
 
 
-- exception handling 
exception
 
when hid_not_in_xref then   
  return(-20004);  
when others then   
  return(-20005);  
 
end CHECK_XREF;  

 
 
FUNCTION init (input_hid in number, input_location_number in number, input_report_type in varchar, input_end_date in varchar2 default null, input_update_flag in number default null) return number is
PRAGMA AUTONOMOUS_TRANSACTION;  
 
count_months number;  
fth4_row_count number;
check_in_prod number; 
report_month_count number;  
check_date_norm date; 
check_date_norm_count number;   
check_xref_failed exception;
invalid_end_date EXCEPTION; 
invalid_report_type EXCEPTION;  
hid_not_in_prod exception;  
hid_not_in_raw exception;   
missing_end_Date exception; 
incomplete_Data exception;  
check_in_raw exception; 
no_previous_run exception;  
not_in_fth4 exception;
not_normalized exception;   
norm_result varchar2(1000); 
last_run_id number;
norm_count number; 
 
 
begin
 
 
change_date_format;
-- get run ID. A New RUNID will be assigned with every attempt to run this Pre processor. 
-- It is the first step so that when the first check is ran if it errors then the error in the log will have a runid associated with it. 
select runid.nextval into gv_input_run_id from dual;  -- assign to input_run_id 
 
 
 
if input_report_type = 'CONTROL' then 
  gv_input_end_Date := input_end_date;
else 
gv_input_end_date := input_end_date;  
end if;  
 
-- assign input values to global variables
gv_input_hid := input_hid;  
gv_input_location_number := input_location_number;  
gv_input_report_type := input_report_type;
gv_input_update_flag := input_update_flag;
 
if upper(gv_input_report_type) not in (upper('Consumer') ,upper('Last Dose'),'PSR','PSR-B','UPL', 'CONTROL','NSAID','PPR') 
then raise invalid_report_type; 
end if;  
 
if gv_input_end_date is null then 
  if upper(gv_input_report_type) in( upper('Consumer') ,upper('Last Dose')) then 
gv_input_end_date := trunc(localtimestamp-2);   
  end if;
  if upper(gv_input_report_type) in( 'PSR','PSR-B') then 
raise missing_end_Date; 
  end if;
  if upper(gv_input_report_type) in( 'UPL' ,'CONTROL'  ,'NSAID','PPR')  then 
raise missing_end_Date; 
  end if;
 
end if;  
 
-- LAST DOSE CODE  
 
if upper(input_report_type) <> upper('Last Dose') and input_end_date is not null then
  gv_input_end_date  :=trunc(to_Date(input_end_date),'mm');  
end if;  
 
 
if upper(gv_input_report_type) = upper('Last Dose') --and input_update_flag > 1  
then 
--gv_input_start_date := add_months(gv_input_end_Date,-12);  
gv_input_start_date := last_dose_start(gv_input_hid,gv_input_update_flag,gv_input_end_date);
end if;  
 
 
 
-- NSAID 
if upper(gv_input_report_type) = 'NSAID' then   
select add_months(gv_input_End_date,-11) into gv_input_start_date from dual; 
end if;  
 
 
--CONTROL_ADDENDUM 
 
 
 
-- call the check_xref to confirm that the info entered is correct. This should have already been done. but just to double check we're running again here. 
-- if it fails (returns < 0) raise an exception 
if CHECK_XREF(input_hid,input_location_number) < 0 then 
  raise check_xref_failed ; 
end if;  
 
 
 
select distinct account_number into gv_input_account_number from accounts where location_number = gv_input_location_number;   
--select database into gv_db_run_from from accounts where location_number = gv_input_location_number;  
check_in_prod := 0;
 
 
 
 
 
 
-- call the log function and write to the process log table when the runid is successfully assigned and checked.  
build_process_log('RUNID','The RUNID for this '||gv_input_report_type||' and HID '||gv_input_hid||' location_number '||gv_input_location_number||' end date '||gv_input_end_date||' is '||gv_input_run_id||'. It will be run in '||gv_db_run_from||'.
','INIT'); 
 
 
commit;  
 
-- return the RUNID
return(gv_input_run_id);
 
-- exception handling 
exception
when no_previous_run then   
  build_process_log('ERROR','Trying to run version '||gv_input_update_flag||' when the previous version has not run yet '||input_report_type||'.','INIT'); 
  return(-20056);  
when missing_end_Date then  
  build_process_log('ERROR','No end date given for '||input_report_type||'.','INIT');
  return(-20056);  
when not_normalized then
  build_process_log('ERROR','This HID '||gv_input_hid||' has not been normalized through the end_date.','INIT'); 
  return(-20055);  
when hid_not_in_prod then   
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' has no data in PROD.','INIT');  
  return(-20052);  
when invalid_report_type then   
  build_process_log('ERROR','This report_Type '||gv_input_report_type||' is not valid.','INIT');   
  return(-20061);  
when incomplete_Data then   
  build_process_log('INFO','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' has incomplete data in DBRAW.','INIT');
  build_process_log('RUNID','The RUNID for this '||gv_input_report_type||' and HID '||gv_input_hid||' location_number '||gv_input_location_number||' end date '||gv_input_end_date||' is '||gv_input_run_id||'. It will be run in '||gv_db_run_from||
'.','INIT');   
 
  return(gv_input_run_id);  
when not_in_fth4 then 
  build_process_log('ERROR','This HID '||gv_input_hid||' is not in lineitem_norm_FTH4 in DBRAW.','INIT');
  return(-20053);  
when check_xref_failed then 
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' was not properly validated.','INIT'); 
  return(CHECK_XREF(input_hid,input_location_number));-- return the specific error from the CHECK_XREF  
when invalid_end_date then  
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' can not be run for this end date. The data is not yet available.','INIT'); 
  return(-20011);  
when others then   
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed to init:'||SQLERRM,'INIT');
  return(-20005);  
end init; 
 

 
function  run_clinic (input_run_id in number)  return number is   
PRAGMA AUTONOMOUS_TRANSACTION;  
 
check_build_log number; 
check_process_log number;   
check_runid_progress number;
prod_run_failed exception;  
 
invalid_runid  exception;   
runid_already_run  exception;   
runid_in_progress EXCEPTION;
--PRAGMA EXCEPTION_INIT(prod_run_failed, -20050);   
 
begin
 
 
change_date_format;
 
 
-- assign input_run_id to the global variable   
gv_input_run_id := input_run_id;
 
--   
 
select report_type into gv_input_report_type from process_log where run_id = gv_input_run_id and flag = 'RUNID'; 
select update_flag into gv_input_update_flag from process_log where run_id = gv_input_run_id and flag = 'RUNID'; 
 
 
 
--------- CHECK BUILD LOG ----------------------------------------------------------------------  
 
-- If the RUNID is in the build log it has completed successfully, it can't be run again. 
-- if the same HID, location_number, END_DATE needs to be run again then a new RUN_ID must be created.  
-- Assign a 1 to check_build_log if the runid has already been run 
-- Assign a 0 to check_build_log if the runid has not already been run 
select count(*) into check_build_log from hid_log where run_id = gv_input_run_id;  
 
-- If the RUNID has already been run raise an exception
if check_build_log <> 0 then
raise runid_already_run;
end if;  
------------------------------------------------------------------------------------------------  
 
 
---- CHECK RUNID PROGRESS ------------------------------------------------------------------------
 
-- Checks to see if this RUNID is running by validating where the RUNID is in the progress log.   
-- pulls all the records that indicate RUN_CLINIC has been started for this RUNID, then pulls all the records that indicate it has completed. 
-- Assign a 1 to check_hid_progress if RUN_CLINIC was started for the RUNID but not completed
-- Assign a 0 to check_hid_progress if RUN_CLINIC has never been started or has completed either on error or success. 
-- If there is no record of completetion in the build_process_log 
-- and the user is certian that it is no longer running then a new RUNID must be created for the HID, location_number, END_DATE.   
select count(*) into check_runid_progress from (
select process,end_Date from process_log   
where flag = 'STARTED'
and process =('RUN_CLINIC') 
and run_id = gv_input_run_id
minus
select process,end_Date from process_log   
where flag in ('SUCCESS','ERROR','INFO')  
and process =('RUN_CLINIC') 
and run_id = gv_input_run_id
);   
 
-- if the RUNID is in progress then raise an exception 
if check_runid_progress <> 0 then 
  raise runid_in_progress;  
end if;  
------------------------------------------------------------------------------------------------  
--select distinct flag from process_log;   
 
------ CHECK PROCESS LOG -----------------------------------------------------------------------  
-- This checks to see if the RUNID specified has been loaded with the init function
-- it returns a 1 if it has been loaded.  
-- it returns a 0 if it has not been loaded.
select count(1) into check_process_log
from process_log 
where flag = 'RUNID'  
and run_id = input_run_id;  
------------------------------------------------------------------------------------------------  
 
-- if THE RUNID has not been loaded then raise an exception  
if check_process_log = 0 then   
raise invalid_runid;  
end if;  
 
 
 
-- there is one record per RUNID with the flag of RUNID in the process_log table.  
-- The following queries go to that table and pull that record for that RUNID and get the HID, location_number and END_DATE for that RUNID   
-- and assign those values to the global variables. 
select h_id into gv_input_hid from process_log where flag = 'RUNID' and run_id = gv_input_run_id;  
select location_number into gv_input_location_number from process_log where flag = 'RUNID' and run_id = gv_input_run_id;  
select trunc(end_date) into gv_input_end_date from process_log where flag = 'RUNID' and run_id = gv_input_run_id;
select trunc(start_date) into gv_input_start_date from process_log where flag = 'RUNID' and run_id = gv_input_run_id; 
 
--if gv_db_run_from = 'PROD' then 
--copy_to_prod;  
--commit;
--end if;
 
 
-- A record is written to the process_log to indicate the starting of the run_clinic function. 
build_process_log('STARTED','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' is being processed for this end date '||gv_input_end_date||'.','RUN_CLINIC'); 
 
-- The patient table build is begun.  
-- Totals are calculated for the time period.   
if upper(gv_input_report_type) = 'CONSUMER' then
build_consumer;  
c_merial.email_pkg.PR_SEND_EMAIL('consumer@vetinsite.com','jwheeler@vetinsite.com','','Consumer File HID '||gv_input_hid||' processed','','',null);   
end if;  
-- Product totals are calculated. 
--build_report_data;   
 
if upper(gv_input_report_type) = 'PSR' then 
build_wellness;  
build_PSR;
build_psr_views(gv_input_run_id);
c_merial.email_pkg.PR_SEND_EMAIL('PSR@vetinsite.com','jwheeler@vetinsite.com','','PSR File HID '||gv_input_hid||' processed','','',null);
end if;  
 
 
if upper(gv_input_report_type) = 'CONTROL' then 
build_control(gv_input_run_id);  
c_merial.email_pkg.PR_SEND_EMAIL('CONTROL@vetinsite.com','jwheeler@vetinsite.com','','CONTROL File HID '||gv_input_hid||' processed','','',null); 
end if;  
 
 
if upper(gv_input_report_type) = 'LAST DOSE' then   
build_last_dose_clients;
build_last_dose;
if gv_input_update_flag > 1 then 
  build_nic_list;
end if;
c_merial.email_pkg.PR_SEND_EMAIL('LAST_DOSE@vetinsite.com','jwheeler@vetinsite.com','','LAST_DOSE File HID '||gv_input_hid||' run_id '||gv_input_run_id||' processed','','',null);
end if;  
 
if upper(gv_input_report_type) = 'NSAID' then   
build_nsaid_purchase;  
c_merial.email_pkg.PR_SEND_EMAIL('NSAID_PURCHASE@vetinsite.com','jwheeler@vetinsite.com','','NSAID_PURCHASE File HID '||gv_input_hid||' run_id '||gv_input_run_id||' processed','','',null); 
end if;  
 
 
if upper(gv_input_report_type) = 'PPR' then 
  gv_input_end_date := trunc(to_Date(gv_input_end_date),'mm');   
  build_wellness;
  ppr_build_pat_master(-2,0,'Total 2015'); 
  ppr_build_pat_master(-14,-12,'Total 2014');  
  -- Product totals are calculated.   
  ppr_build_report_data;   
end if;  
 
 
-- record written to the PROCESS LOG to indicate the RUNID completed successfully.   
build_process_log('SUCCESS','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' was processed successfully for this end date '||gv_input_end_date||'.','RUN_CLINIC');-- record is written to the HID LOG to indicate the RUNI
--D has completed successfuly.
 
build_hid_log;
-- the RUNID is returned.   
return(gv_input_run_id);
 
-- exception handling 
 
exception
 
when prod_run_failed then   
  rollback;
  build_process_log('ERROR','An error occurred while processing this clinic in PROD : '||SQLERRM,'RUN_CLINIC');  
when runid_in_progress then 
  build_process_log('INFO','This run_id '||gv_input_run_id||' is currently running.','RUN_CLINIC');
  return(-20001);  
when runid_already_run then 
  build_process_log('INFO','This run_id '||gv_input_run_id||' has already been run.','RUN_CLINIC');
  return(-20006);  
when invalid_runid then 
  if input_run_id > 0 then  
build_process_log('INFO','This run_id '||gv_input_run_id||' is not valid.','RUN_CLINIC');
return(-20007);
  else   
build_process_log('INFO','The following error was returned from init '||input_run_id||'.','RUN_CLINIC');
return(input_run_id);   
  end if;
when STORAGE_ERROR then 
  rollback;
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed. Database storage space exceeded.','RUN_CLINIC'); 
  return(-20008);  
when others then   
  rollback;
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed:'||SQLERRM,'RUN_CLINIC');
  return(-20009);  
end run_clinic;  


 
procedure build_wellness as
 
begin
 
 
insert into psr_wellness   
select gv_input_run_id run_id,hid h_id,  service_vaccine_id--, handle,descr
from cooked.service_vaccine2 sv  
where 1=1 and (
 ANTIGEN_CHLAMYDIA is not null
or ANTIGEN_CORONAVIRUS is not null
or ANTIGEN_DISTEMPER is not null
or ANTIGEN_FIP is not null
or ANTIGEN_FIV is not null
or ANTIGEN_ADENOVIRUS is not null
or ANTIGEN_LEPTO_UNSPECIFIED is not null
or ANTIGEN_LEPTO_CANICOLA is not null
or ANTIGEN_LEPTO_ICTERO is not null
or ANTIGEN_LEPTO_POMONA is not null
or ANTIGEN_LEPTO_GRIPP is not null
or ANTIGEN_LYME is not null
or ANTIGEN_PANLEUKOPENIA is not null
or ANTIGEN_PARVOVIRUS is not null
or ANTIGEN_RABIES is not null
or ANTIGEN_RHINOTRACHEITIS is not null)
and hid = gv_input_hid
union
select distinct gv_input_run_id run_id,hid,service_general_id from cooked.service_general l   
where product_info in ( 
'Surgery - neuter',
'Surgery - neuter AND extract deciduous teeth', 
'Surgery - neuter AND ocular/cosmetic/elective',
'Surgery - neuter AND ocular/cosmetic/elective w/dental',
'Surgery - spay',  
'Surgery - spay AND extract deciduous teeth',   
'Surgery - spay AND ocular/cosmetic/elective',  
'Surgery - spay AND ocular/cosmetic/elective w/dental',
'Surgery - spay w/dental',  
'Surgery - sterilization (gender unspecified)', 
'Surgery - sterilization (gender unspecified) AND ocular/cosmetic/elective', 
'Surgery - sterilization (gender unspecified) AND ocular/cosmetic/elective w/dental',
'Surgery - sterilization (gender unspecified) w/dental'
)
and hid = gv_input_hid
 
union
select distinct gv_input_run_id run_id,hid,service_test_id from cooked.service_test l
where HW_TEST is not null   
and hid = gv_input_hid
;
 
 
exception
when others then   
  rollback;
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed to init:'||SQLERRM,'BUILD_WELLNESS'); 
  raise; 
 
end build_wellness;
 


procedure build_psr as 
 
begin
 
--------------------------------------------------------------------------------------------------------
 
build_process_log('INFO','Running patient master for HID '||gv_input_hid||' location_number '||gv_input_location_number||' this end date '||gv_input_end_date||'.','BUILD_PSR');   
 
insert into psr_patient_master   
select gv_input_run_id
,patient_id
,cast(h_id as number) 
,vi_species_id 
,start_date
,end_date
-- 0's can not exist they must be nulled out the decode will be quicker than running update statements. 
-- If we have 0's those without a FT products for example, will average in to the avg cost per dose.
,case when ft_doses <= 0 or ft_rev <= 0 then null else ft_doses end ft_doses 
,case when hw_doses <= 0 or hw_rev <= 0 then null else hw_doses end hw_doses 
,case when ft_rev <= 0 or ft_doses <= 0 then null else ft_rev end ft_rev   
,case when hw_rev <= 0 or hw_doses <= 0 then null else hw_rev end hw_rev   
-- can't use Decode here since we need a less than 0. So if someone buys 24 doses they will not take away from the oppotunity. 
,case when (ft_doses <= 0 or ft_rev <= 0) and vi_species_id = 3 then dose_rec_ft_dog 
  when (ft_doses <= 0 or ft_rev <= 0) and vi_species_id = 7 then dose_rec_ft_cat 
  when ft_opp_doses < 0 then 0
  -- with negative transactions we will see recommendations > than the rec. this will fix that
  when ft_opp_doses > dose_rec_ft_dog and vi_species_id = 3 then dose_rec_ft_dog 
  when ft_opp_doses > dose_rec_ft_cat and vi_species_id = 7 then dose_rec_ft_cat 
  else ft_opp_doses end ft_opp_doses  
 
,case when (hw_doses <= 0 or hw_rev <= 0) and vi_species_id = 3 then dose_rec_hw_dog 
  when (hw_doses <= 0 or hw_rev <= 0) and vi_species_id = 7 then dose_rec_hw_cat 
  when hw_opp_doses < 0 then 0
  when hw_opp_doses > dose_rec_hw_dog and vi_species_id = 3 then dose_rec_hw_dog 
  when hw_opp_doses > dose_rec_hw_cat and vi_species_id = 7 then dose_rec_hw_cat 
  else hw_opp_doses end hw_opp_doses  
,case when total_rev < 0 then 0 else total_rev end total_rev 
,visits  
,case when wellness_care > 0 then 1 end wellness_care  
,case when youngest_age < 1 then 1 end baby 
,case when ft_rev <= 0 or ft_doses <= 0 then 0 else ft_visits end ft_visits
,case when hw_rev <= 0 or hw_doses <= 0 then 0 else hw_visits end hw_visits
,hw_tests
from (   
  select 
  l.patient_id 
  ,l.h_id
  ,p.vi_species_id 
  ,a.location_number  
  ,cast(add_months(gv_input_end_Date,-11) as date) start_date
  ,cast(add_months(gv_input_end_Date,0) as date) end_date
  -- we have to generate 0's here beause you can't sum a null
  ,sum(decode(f.type,'ft',f.quantity_norm,'fh',f.quantity_norm,'f',f.quantity_norm,0)) ft_doses -- total FT doses 
  ,sum(decode(f.type,'h',f.quantity_norm,'fh',f.quantity_norm,0)) hw_doses -- total FT doses  
  ,count(distinct decode(f.type,'ft',f.invoice_date,'fh',f.invoice_date,'f',f.invoice_date)) ft_visits -- total FT visits
  ,count(distinct decode(f.type,'h',f.invoice_date,'fh',f.invoice_date)) hw_visits -- total FT doses
  ,sum(case when f.type in ('fh') then (f.cost_norm*.66) 
  when f.type in ('f','ft') then  f.cost_norm
   else 0 end) ft_rev -- total FTrev  
  ,sum(case when f.type in ('fh') then (f.cost_norm*.33) 
  when f.type in ('h') then f.cost_norm   
   else 0 end) hw_rev -- total hw_rev 
  ,case when p.vi_species_id = 3 then dose_rec_ft_dog  
when p.vi_species_id = 7 then dose_rec_ft_cat else 0 end - sum(decode(f.type,'ft',f.quantity_norm,'fh',f.quantity_norm,'f',f.quantity_norm,0)) ft_opp_doses -- calculate opportunity FT doses
  ,case when p.vi_species_id = 3 then dose_rec_hw_dog  
when p.vi_species_id = 7 then dose_rec_hw_cat else 0 end - sum(decode(f.type,'h',f.quantity_norm,'fh',f.quantity_norm,0)) hw_opp_doses -- calculate opportunity HW doses
  ,sum(nvl(f.cost_norm,l.cost)) total_rev 
  ,count(distinct l.invoice_Date) visits  
  ,count(v.service_vaccine_id) wellness_care
  ,min((l.invoice_date-dob)/365) youngest_age   
  ,count(hw_test) hw_tests  
  -- This also assumes one fecal and one HW test per day 
  from cooked.lineitem l -- all transactions must be looked at to get total visits, and tests.
  inner join cooked.patient_ref p on p.patient_id = l.patient_id and p.h_id = l.h_id 
  left join accounts a on a.location_number = gv_input_location_number and a.hid = p.h_id
  --inner join vetstreet.pgsql_pms_species_lookup lu -- to get vi_species_id 
  --on lu.id = p.pms_species_id 
  -- need a subquery here so we're only pulling the products we want from lineitem_norm 
  left join (select f.*, pf.type from normalization.lineitem_norm_fth4 f   
   inner join reports.product_families pf 
   on pf.product = f.product
   where f.cost_norm <> 0   
   and quantity_norm is not null
   and f.norm_rule_id is not null 
   and quantity_norm <> 0   
   and (voided in('0','F','f'))) f
  on l.lineitem_id = f.lineitem_id and l.h_id = f.h_id-- join on LI ID because we're only pulling normalized rows for FTH calculations.  
  left join psr_wellness v on v.service_vaccine_id = l.service_id and p.h_id = v.h_id and v.run_id = gv_input_run_id
  left join cooked.service_test t on t.service_test_id = l.service_id and hw_test is not null 
  where trunc(l.invoice_date,'mm') between add_months(gv_input_end_Date,-11) and add_months(gv_input_end_Date,0)  
  and l.h_id = gv_input_hid 
  and (l.voided in('0','F','f'))
  and p.vi_species_id in (3,7)  
  --and ((v.service_vaccine_id is not null and l.cost > 0) or v.service_vaccine_id is null)  -- if it's a test the cost must be > 0
  group by a.location_number,l.patient_id,l.h_id,p.vi_species_id,case when p.vi_species_id = 3 then dose_rec_ft_dog   
when p.vi_species_id = 7 then dose_rec_ft_cat else 0 end,case when p.vi_species_id = 3 then dose_rec_hw_dog   
when p.vi_species_id = 7 then dose_rec_hw_cat else 0 end 
) c  
left join accounts a on a.location_number = c.location_number and a.hid = c.h_id
;
 
 
-------------------------------------------------------------------------------------------   
build_process_log('INFO','Running product master for HID '||gv_input_hid||' location_number '||gv_input_location_number||' this end date '||gv_input_end_date||'.','BUILD_PSR');   
 
insert into psr_product_master   
select   
 gv_input_run_id   
,f.h_id  
,cast(add_months(gv_input_end_Date,-11) as date) start_date  
,cast(add_months(gv_input_end_Date,0) as date) end_date
,decode(pr.type,'f','ft',type) -- we're doing some reassignment of type to either FT or HW in this study.   
,family_name   
,p.vi_species_id   
,count(distinct p.patient_id) patients
,sum(f.quantity_norm) doses 
,sum(case when pr.type in ('fh') then (f.cost_norm*.33)
when pr.type in ('h') then f.cost_norm
   else 0 end) hw_rev -- total hw_rev 
,sum(case when pr.type in ('fh') then (f.cost_norm*.66)
when pr.type in ('f','ft') then  f.cost_norm 
   else 0 end) ft_rev -- total FTrev  
from normalization.lineitem_norm_fth4 f   
inner join cooked.patient_ref p on p.patient_id = f.patient_id   
inner join reports.product_families pr on pr.product = f.product 
inner join psr_patient_master pm on pm.run_id = gv_input_run_id and pm.patient_id = p.patient_id and pm.WELLNESS_CARE is not null   
where trunc(f.invoice_date,'mm') between add_months(gv_input_end_Date,-11) and add_months(gv_input_end_Date,0)
and p.vi_species_id in (3,7)
and f.cost_norm <> 0  
and quantity_norm is not null   
and f.norm_rule_id is not null  
and quantity_norm <> 0
and f.h_id = gv_input_hid   
and (f.voided in('0','F','f') ) 
group by f.h_id,p.vi_species_id,family_name,decode(pr.type,'f','ft',type)  
;
 
--------------------------------------------------------------------   
build_process_log('INFO','Running month master for HID '||gv_input_hid||' location_number '||gv_input_location_number||' this end date '||gv_input_end_date||'.','BUILD_PSR'); 
 
 
commit;  
psr_insert_month_master(gv_input_run_id);  
 
commit;  
build_process_log('INFO','Finished running data for HID '||gv_input_hid||' location_number '||gv_input_location_number||' this end date '||gv_input_end_date||'.','BUILD_PSR');
 
 
exception
when others then   
  rollback;
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed to init:'||SQLERRM,'BUILD_PSR');   
  raise; 
 
end build_psr;



procedure psr_insert_month_master (input_run_id number) as  
 
begin
 
--delete from psr_month_master where run_id = input_run_id;   
 
 
 
insert into psr_month_master 
select input_run_id  
,cast(h_id as number) 
,vi_species_id 
,start_date
,end_date
-- 0's can not exist they must be nulled out the decode will be quicker than running update statements. 
-- If we have 0's those without a FT products for example, will average in to the avg cost per dose.
,data_month
,count(distinct case when ft_doses > 0 then patient_id end) ft_patients
,count(distinct case when hw_doses > 0 then patient_id end) hw_patients
,count(distinct patient_id) total_patients
-- can't use Decode here since we need a less than 0. So if someone buys 24 doses they will not take away from the oppotunity. 
,sum(case when ft_doses_year <= 0 then null else ft_doses end) ft_doses
,sum(case when hw_doses_year <= 0 then null else hw_doses end) hw_doses
from (   
  select distinct  
  l.patient_id 
  ,l.h_id
  ,p.vi_species_id 
  ,cast(add_months(pm.end_Date,-11) as date) start_date
  ,cast(add_months(pm.end_Date,0) as date) end_date 
  ,trunc(l.invoice_date,'mm') data_month  
  -- we have to generate 0's here beause you can't sum a null
  ,sum(decode(f.type,'ft',f.quantity_norm,'fh',f.quantity_norm,'f',f.quantity_norm,0)) over(partition by l.patient_id,trunc(l.invoice_date,'mm')) ft_doses -- total FT doses  
  ,sum(decode(f.type,'ft',f.quantity_norm,'fh',f.quantity_norm,'f',f.quantity_norm,0)) over(partition by l.patient_id) ft_doses_year -- total FT doses 
  ,sum(decode(f.type,'h',f.quantity_norm,'fh',f.quantity_norm,0)) over(partition by l.patient_id,trunc(l.invoice_date,'mm')) hw_doses -- total FT doses
  ,sum(decode(f.type,'h',f.quantity_norm,'fh',f.quantity_norm,0)) over(partition by l.patient_id) hw_doses_year -- total FT doses  
  ,count(distinct l.invoice_Date) over(partition by l.patient_id,trunc(l.invoice_date,'mm')) visits 
  ,min((l.invoice_date-dob)/365) over(partition by l.patient_id) youngest_age
  -- This also assumes one fecal and one HW test per day 
  from cooked.lineitem l -- all transactions must be looked at to get total visits, and tests.
  inner join cooked.patient_ref p  on p.patient_id = l.patient_id and l.h_id = p.h_id
  inner join psr_patient_master pm on pm.run_id = gv_input_run_id and pm.patient_id = p.patient_id and pm.WELLNESS_CARE is not null 
-- need a subquery here so we're only pulling the products we want from lineitem_norm
  left join (select f.*, pf.type from normalization.lineitem_norm_fth4 f   
   inner join reports.product_families pf 
   on pf.product = f.product
   where f.cost_norm <> 0   
   and quantity_norm is not null
   and f.norm_rule_id is not null 
   and quantity_norm <> 0   
   and (voided in('0','F','f'))) f
  on l.lineitem_id = f.lineitem_id and l.h_id = f.h_id-- join on LI ID because we're only pulling normalized rows for FTH calculations.  
  where trunc(l.invoice_date,'mm') between add_months(pm.end_Date,-11) and add_months(pm.end_Date,0)
  and l.h_id = pm.h_id
  and (l.voided in('0','F','f'))
  and p.vi_species_id in (3,7)  
)
group by cast(h_id as number) ,vi_species_id,start_date
,end_date,data_month  
;
 
end; 
 

 
procedure build_psr_views (input_run_id in number) as   
 
begin
 
gv_input_run_id := input_run_id;
 
--delete from psr_t1 where run_id = gv_input_run_id;
insert into psr_t1 
select   
run_id   
,h_id
,location_number   
,end_date
,report_name   
,species 
,num_pats
,pats_prod 
,pats_well 
,pats_prod_well
,PATS_NO_PROD_WELL 
,dose_rec
,avg_doses 
,TEN_unpro 
,TEN_unpro_DOSES   
,TWENTYFIVE_unpro  
,TWENTYFIVE_unpro_DOSES 
,partial_pats  
,case when (PARTIAL_PATS_PCT > fully_PROTECTED_PATS_pct and PARTIAL_PATS_PCT > UNPROTECTED_PATS_PCT and fully_PROTECTED_PATS_pct + PARTIAL_PATS_PCT > 1)  
then PARTIAL_PATS_PCT+(1-(abs(PARTIAL_PATS_PCT) + abs(fully_PROTECTED_PATS_pct)))   
else PARTIAL_PATS_PCT end PARTIAL_PATS_PCT
,TWENTYFIVE_PART_PROD 
,TWENTYFIVE_PART_PROD_DOSES 
,TEN_PART_PROD 
,TEN_PART_PROD_DOSES  
,fully_PROTECTED_PATS 
,case when (fully_PROTECTED_PATS_pct > PARTIAL_PATS_PCT and fully_PROTECTED_PATS_pct > UNPROTECTED_PATS_PCT and fully_PROTECTED_PATS_pct + PARTIAL_PATS_PCT > 1)
then fully_PROTECTED_PATS_pct+(1-(abs(PARTIAL_PATS_PCT) + abs(fully_PROTECTED_PATS_pct))) 
else fully_PROTECTED_PATS_pct end fully_PROTECTED_PATS_pct   
,EIGHTY_SINGLE_PATS
,EIGHTY_SINGLE_PATS_DOSES   
,UNPROTECTED_PATS  
,case when (fully_PROTECTED_PATS_pct + PARTIAL_PATS_PCT > 1) 
then UNPROTECTED_PATS_PCT-(1-(abs(PARTIAL_PATS_PCT) + abs(fully_PROTECTED_PATS_pct)))   
else UNPROTECTED_PATS_PCT end UNPROTECTED_PATS_PCT  
from (   
select   
run_id   
,h_id
,location_number   
,end_date
,report_name   
,species 
,num_pats
,pats_prod 
,pats_well 
,pats_prod_well
,pats_well-pats_prod_well PATS_NO_PROD_WELL 
,dose_rec
,avg_doses 
,nvl(round((pats_well-pats_prod_well)*.1),0) TEN_unpro 
,nvl(round((pats_well-pats_prod_well)*.1*avg_doses),0) TEN_unpro_DOSES 
,nvl(round((pats_well-pats_prod_well)*.25),0) TWENTYFIVE_unpro   
,nvl(round((pats_well-pats_prod_well)*.25*avg_doses),0) TWENTYFIVE_unpro_DOSES   
,nvl(partial_pats,0) partial_pats 
,case when pats_well = 0 then 0 else round((partial_pats/pats_well),2) end PARTIAL_PATS_PCT --
,nvl(TWENTYFIVE_PART_PROD,0) TWENTYFIVE_PART_PROD   
,nvl(TWENTYFIVE_PART_PROD_DOSES,0) TWENTYFIVE_PART_PROD_DOSES
,nvl(TEN_PART_PROD,0) TEN_PART_PROD   
,nvl(TEN_PART_PROD_DOSES,0) TEN_PART_PROD_DOSES 
,nvl(fully_PROTECTED_PATS,0) fully_PROTECTED_PATS   
,case when pats_well = 0 then 0 else round((fully_PROTECTED_PATS/pats_well),2)  end fully_PROTECTED_PATS_pct --   
--,PROTECTED_PATS_PCT 
,EIGHTY_SINGLE_PATS
,EIGHTY_SINGLE_PATS_DOSES   
,pats_well-(fully_PROTECTED_PATS+partial_pats) UNPROTECTED_PATS  
,(1-(case when pats_well = 0 then 0 else round((partial_pats/pats_well),2) end + case when pats_well = 0 then 0 else round((fully_PROTECTED_PATS/pats_well),2)  end))UNPROTECTED_PATS_PCT -- 
from (   
  select s.run_id,s.h_id
  ,s.location_number  
  ,s.end_date  
  ,report_name 
  ,species 
  -- Total Patients
  ,count(patient_id) num_pats   
 
  -- total flea/tick or hw paitnets receiving product  
  ,count(case when ft_doses > 0 and report_name = 'Flea/Tick' then patient_id
when hw_doses > 0 and report_name = 'Heartworm' then patient_id
end) pats_prod
 
  -- total patients with a wellness visit 
  ,count(wellness_care) pats_well 
 
  -- total patients with a wellness visit and on a fleatick or hw product  
  ,count(case when ((ft_doses > 0 and report_name = 'Flea/Tick') or
(hw_doses > 0 and report_name = 'Heartworm'))
 and wellness_care is not null then patient_id end) pats_prod_well 
  -- dose reccomendation
  ,case when s.report_name = 'Flea/Tick' and s.species = 'Dog' then a.dose_rec_ft_dog
when s.report_name = 'Heartworm' and s.species = 'Dog' then a.dose_rec_hw_dog
when s.report_name = 'Flea/Tick' and s.species = 'Cat' then a.dose_rec_ft_cat
when s.report_name = 'Heartworm' and s.species = 'Cat' then a.dose_rec_hw_cat
end dose_rec  
 
  -- UPDATED   
  -- average number of doses for all patients   
  ,round( case when count(distinct(case when report_name = 'Flea/Tick' and wellness_care is not null and ft_doses is not null then pm.patient_id   
  when report_name = 'Heartworm' and wellness_care is not null and hw_doses is not null then pm.patient_id end)) = 0 then 0 else  
   nvl(sum(case when report_name = 'Flea/Tick' and wellness_care is not null then ft_doses
  when report_name = 'Heartworm' and wellness_care is not null then hw_doses end) / 
   count(distinct(case when report_name = 'Flea/Tick' and wellness_care is not null and ft_doses is not null then pm.patient_id
  when report_name = 'Heartworm' and wellness_care is not null and hw_doses is not null then pm.patient_id end)),0) end ,1)   avg_doses 
/*   
  -- average number of doses for all patients   
  ,nvl(round(avg(case when report_name = 'Flea/Tick' and wellness_care is not null then ft_doses  
  when report_name = 'Heartworm' and wellness_care is not null then hw_doses end),1),0) avg_doses 
 
*/   
  -- count of patients who purchased product but less than the recommended number of doses
 
  ,count(case when report_name = 'Flea/Tick' and vi_species_id = 3 and ft_doses between 1 and (a.dose_rec_ft_dog - 1) and wellness_care is not null then patient_id 
when report_name = 'Heartworm' and vi_species_id = 3 and hw_doses between 1 and (a.dose_rec_hw_dog - 1) and wellness_care is not null then patient_id 
when report_name = 'Flea/Tick' and vi_species_id = 7 and ft_doses between 1 and (a.dose_rec_ft_cat - 1) and wellness_care is not null then patient_id 
when report_name = 'Heartworm' and vi_species_id = 7 and hw_doses between 1 and (a.dose_rec_hw_cat - 1) and wellness_care is not null then patient_id 
end) partial_pats 
 
  ----25% of patients who purchased 1-5 doses ---------------------
  ,ceil(count(case when report_name = 'Flea/Tick' and ft_doses between 1 and 5 and wellness_care is not null then patient_id   
when report_name = 'Heartworm' and hw_doses between 1 and 5 and wellness_care is not null then patient_id  
end)*0.25) twentyfive_part_prod 
 
  ,ceil(sum(case when report_name = 'Flea/Tick' and ft_doses between 1 and 5 and wellness_care is not null then 6 - ft_doses   
when report_name = 'Heartworm' and hw_doses between 1 and 5 and wellness_care is not null then 6 - hw_doses  
end)*0.25) twentyfive_part_prod_doses
  ----- 10% of patients who purchased 7-11 doses --------------------  
  ,ceil(count(case when report_name = 'Flea/Tick' and ft_doses between 7 and 11 and wellness_care is not null then patient_id  
when report_name = 'Heartworm' and hw_doses between 7 and 11 and wellness_care is not null then patient_id 
end)*0.10) ten_part_prod
 
  ,ceil(sum(case when report_name = 'Flea/Tick' and ft_doses between 7 and 11 and wellness_care is not null then 12 - ft_doses 
   when report_name = 'Heartworm' and hw_doses between 7 and 11 and wellness_care is not null then 12 - hw_doses 
   end)*0.10) ten_part_prod_doses 
  -----------------------------------------------------------
  ,count(case when report_name = 'Flea/Tick' and ft_opp_doses = 0 and wellness_care is not null then patient_id   
when report_name = 'Heartworm' and hw_opp_doses = 0 and wellness_care is not null then patient_id   
end) fully_PROTECTED_PATS   
 
  -- if 80 of single dose purchasers purchased 3 doses.
  ,ceil(count(case when report_name = 'Flea/Tick' and ft_doses = 1 and wellness_care is not null then patient_id  
 when report_name = 'Heartworm' and hw_doses = 1 and wellness_care is not null then patient_id  
 end)*.8) eighty_single_pats  
 
  ,ceil(count(case when report_name = 'Flea/Tick' and ft_doses = 1 and wellness_care is not null then patient_id  
 when report_name = 'Heartworm' and hw_doses = 1 and wellness_care is not null then patient_id  
 end)*.8)*2 eighty_single_pats_doses 
 
  from   
  (select * from psr_markets,psr_species,(select * from process_log where flag = 'RUNID' ) hl ) s
  left join psr_patient_master pm on s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and pm.run_id = s.run_id 
  left join accounts a on s.location_number = a.location_number and a.hid = s.h_id 
  group by s.run_id,s.h_id  
  ,s.location_number  
  ,s.end_date  
  ,report_name 
  ,species 
  ,case when s.report_name = 'Flea/Tick' and s.species = 'Dog' then a.dose_rec_ft_dog
when s.report_name = 'Heartworm' and s.species = 'Dog' then a.dose_rec_hw_dog
when s.report_name = 'Flea/Tick' and s.species = 'Cat' then a.dose_rec_ft_cat
when s.report_name = 'Heartworm' and s.species = 'Cat' then a.dose_rec_hw_cat
end)   
where run_id = gv_input_run_id  
);   
 
 
commit;  
 
 
 
--delete from psr_t2 where run_id = gv_input_run_id;
insert into psr_t2 
--create or replace view psr_t2 as   
select distinct d.run_id,d.h_id,d.location_number,d.end_date,d.report_name,d.species 
,case when d.report_name = 'Flea/Tick' and d.species = 'Dog' then b.dose_rec_ft_dog  
when d.report_name = 'Heartworm' and d.species = 'Dog' then b.dose_rec_hw_dog
when d.report_name = 'Flea/Tick' and d.species = 'Cat' then b.dose_rec_ft_cat
when d.report_name = 'Heartworm' and d.species = 'Cat' then b.dose_rec_hw_cat
end dose_rec  
,d.doses 
,nvl(patients,0) patients   
,case when (case when d.report_name = 'Flea/Tick' and d.species = 'Dog' then b.dose_rec_ft_dog
when d.report_name = 'Heartworm' and d.species = 'Dog' then b.dose_rec_hw_dog
when d.report_name = 'Flea/Tick' and d.species = 'Cat' then b.dose_rec_ft_cat
when d.report_name = 'Heartworm' and d.species = 'Cat' then b.dose_rec_hw_cat
end - decode(d.doses,'1',1,'2',2,'3',3,'4',4,'5',5,'6',6,'7',7,'8',8,'9',9,'10',10,'11',11,'12+',12))  < 0 then 0
else   
(case when d.report_name = 'Flea/Tick' and d.species = 'Dog' then b.dose_rec_ft_dog   
when d.report_name = 'Heartworm' and d.species = 'Dog' then b.dose_rec_hw_dog
when d.report_name = 'Flea/Tick' and d.species = 'Cat' then b.dose_rec_ft_cat
when d.report_name = 'Heartworm' and d.species = 'Cat' then b.dose_rec_hw_cat
end - decode(d.doses,'1',1,'2',2,'3',3,'4',4,'5',5,'6',6,'7',7,'8',8,'9',9,'10',10,'11',11,'12+',12))*nvl(patients,0) end missed_doses 
from (select * from psr_markets,psr_species,psr_doses_1,(select * from process_log where flag = 'RUNID' ) hl ) d   
left join (
  select 
  s.run_id,s.h_id,s.report_name,s.species 
  ,case when s.report_name = 'Flea/Tick' and ft_doses >= 12 then '12+' 
when s.report_name = 'Heartworm' and HW_doses >= 12 then '12+' 
when s.report_name = 'Flea/Tick' and (ft_doses is null or ft_doses <=0) then '0'
when s.report_name = 'Heartworm' and (hw_doses is null or hw_doses <=0) then '0'
when s.report_name = 'Flea/Tick' then to_char(ft_doses)  
when s.report_name = 'Heartworm' then to_char(HW_doses)  
end doses  
 
  ,count(distinct case when s.report_name = 'Flea/Tick' and ft_doses > 0 then patient_id
   when s.report_name = 'Heartworm' and hw_doses > 0 then patient_id end ) patients 
  -- when the number of doses > the rec doses then return 0 missed doses else return REC_DOSES-ACTUAL_DOSES 
  from   
  (select * from psr_markets,psr_species,(select * from process_log where flag = 'RUNID') ) s
  left join psr_patient_master pm on pm.run_id = s.run_id and s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and wellness_care is not null
  left join accounts a on s.location_number = a.location_number and s.h_id = a.hid 
  --where s.run_id = 10862  
  group by s.run_id,s.h_id,s.location_number,s.species,s.report_name   
  ,case when s.report_name = 'Flea/Tick' and ft_doses >= 12 then '12+' 
when s.report_name = 'Heartworm' and HW_doses >= 12 then '12+' 
when s.report_name = 'Flea/Tick' and (ft_doses is null or ft_doses <=0) then '0'
when s.report_name = 'Heartworm' and (hw_doses is null or hw_doses <=0) then '0'
when s.report_name = 'Flea/Tick' then to_char(ft_doses)  
when s.report_name = 'Heartworm' then to_char(HW_doses)  
end
) a on d.doses = a.doses and d.species = a.species and a.run_id = d.run_id and a.report_name = d.report_name
left join accounts b on d.location_number = b.location_number and b.hid = d.h_id
where d.run_id = gv_input_run_id
order by d.run_id,d.report_name,d.species,decode(d.doses,'1',1,'2',2,'3',3,'4',4,'5',5,'6',6,'7',7,'8',8,'9',9,'10',10,'11',11,'12+',12) 
;
 
 
commit;  
 
----------------------------------------------------------------------------------------------------------------------------   
-- Puppy Kitten chart ------------------------------------------------------------------------------------------------------   
----------------------------------------------------------------------------------------------------------------------------   
 
--delete from psr_t3 where run_id = gv_input_run_id;
insert into psr_t3 
--create or replace view psr_t3 as   
select run_id  
,h_id
,location_number   
,end_date
,report_name   
,species 
,num_pats
,pats_prod 
,case when num_pats_wellness = 0 then 0 else round(pats_prod/num_pats_wellness,2) end pats_prod_pct 
,num_pats_wellness-pats_prod pats_no_prod --2/26/2015 added wellness criteria
,case when num_pats_wellness = 0 then 0 else round((num_pats_wellness-pats_prod)/num_pats_wellness,2) end pats_no_prod_pct 
,dose_rec
,avg_doses 
,partial_pats  
,case when num_pats_wellness = 0 then 0 else round(partial_pats/num_pats_wellness,2) end partial_pats_pct   
,ten_pats
,ten_doses 
,twentyfive_pats   
,twentyfive_doses  
,fifty_pats
,fifty_doses   
,fully_protected_pats 
,case when num_pats_wellness = 0 then 0 else round(fully_protected_pats/num_pats_wellness,2) end fully_protected_pats_pct
,num_pats_wellness-(fully_protected_pats+partial_pats) unprotected_pats
,1-round(case when num_pats_wellness = 0 then 0 else round(partial_pats/num_pats_wellness,2) end + case when num_pats_wellness = 0 then 0 else round(fully_protected_pats/num_pats_wellness,2) end,2) unprotected_pats_pct  
,num_pats_wellness 
from (   
  select s.run_id  
  ,s.h_id
  ,s.location_number  
  ,s.end_date  
  ,report_name 
  ,species 
  -- Total Patients
  ,count(patient_id) num_pats   
 
  -- Total Patients with wellness care
  ,count(wellness_care) num_pats_wellness 
 
  -- total flea/tick or hw paitnets receiving product  
  ,count(case when ft_doses > 0 and report_name = 'Flea/Tick' and wellness_care is not null then patient_id --2/26/2015 added wellness criteria
when hw_doses > 0 and report_name = 'Heartworm' and wellness_care is not null then patient_id --2/26/2015 added wellness criteria
end) pats_prod
 
 
  -- dose reccomendation
  ,case when s.report_name = 'Flea/Tick' and s.species = 'Dog' then a.dose_rec_ft_dog
when s.report_name = 'Heartworm' and s.species = 'Dog' then a.dose_rec_hw_dog
when s.report_name = 'Flea/Tick' and s.species = 'Cat' then a.dose_rec_ft_cat
when s.report_name = 'Heartworm' and s.species = 'Cat' then a.dose_rec_hw_cat
end dose_rec  
 
 
   --UPDATED   
  ,round( case when count(distinct(case when report_name = 'Flea/Tick' and ft_doses is not null and wellness_care is not null then pm.patient_id   
   when report_name = 'Heartworm' and hw_doses is not null and wellness_care is not null then pm.patient_id end)) = 0 then 0 else
 nvl(sum(case when report_name = 'Flea/Tick' and wellness_care is not null then ft_doses  
when report_name = 'Heartworm' and wellness_care is not null then hw_doses end) / 
 count(distinct(case when report_name = 'Flea/Tick' and ft_doses is not null and wellness_care is not null then pm.patient_id  
  when report_name = 'Heartworm' and hw_doses is not null and wellness_care is not null then pm.patient_id end)),0) end ,1)   avg_doses  
  /* 
  -- average number of doses for all patients   
  ,nvl(round(avg(case when report_name = 'Flea/Tick' then ft_doses 
  when report_name = 'Heartworm' then hw_doses end),1),0) avg_doses 
 
  */ 
 
  -- count of patients who purchased product but less than the recommended number of doses
  ,count(case when report_name = 'Flea/Tick' and vi_species_id = 3 and ft_doses between 1 and (a.dose_rec_ft_dog - 1) and wellness_care is not null then patient_id 
when report_name = 'Heartworm' and vi_species_id = 3 and hw_doses between 1 and (a.dose_rec_hw_dog - 1) and wellness_care is not null then patient_id 
when report_name = 'Flea/Tick' and vi_species_id = 7 and ft_doses between 1 and (a.dose_rec_ft_cat - 1) and wellness_care is not null then patient_id 
when report_name = 'Heartworm' and vi_species_id = 7 and hw_doses between 1 and (a.dose_rec_hw_cat - 1) and wellness_care is not null then patient_id 
end) partial_pats 
 
 
 
  ----10% of patients who purchased 1-5 doses ---------------------
  ,ceil(count(case when report_name = 'Flea/Tick' and nvl(ft_doses,0) between 0 and 5 and wellness_care is not null then patient_id
 when report_name = 'Heartworm' and nvl(hw_doses,0) between 0 and 5 and wellness_care is not null then patient_id
 end)*0.1) ten_pats 
 
  ,ceil(sum(case when report_name = 'Flea/Tick' and nvl(ft_doses,0) between 0 and 5 and wellness_care is not null then 6 - nvl(ft_doses,0)   
   when report_name = 'Heartworm' and nvl(hw_doses,0) between 0 and 5 and wellness_care is not null then 6 - nvl(hw_doses,0)   
   end)*0.1) ten_doses  
  ----10% of patients who purchased 1-5 doses ---------------------
  ,ceil(count(case when report_name = 'Flea/Tick' and nvl(ft_doses,0) between 0 and 5 and wellness_care is not null then patient_id
 when report_name = 'Heartworm' and nvl(hw_doses,0) between 0 and 5 and wellness_care is not null then patient_id
 end)*0.25) twentyfive_pats   
 
  ,ceil(sum(case when report_name = 'Flea/Tick' and nvl(ft_doses,0) between 0 and 5 and wellness_care is not null then 6 - nvl(ft_doses,0)   
   when report_name = 'Heartworm' and nvl(hw_doses,0) between 0 and 5 and wellness_care is not null then 6 - nvl(hw_doses,0)   
   end)*0.25) twentyfive_doses
  ----10% of patients who purchased 1-5 doses ---------------------
  ,ceil(count(case when report_name = 'Flea/Tick' and nvl(ft_doses,0) between 0 and 5 and wellness_care is not null then patient_id
 when report_name = 'Heartworm' and nvl(hw_doses,0) between 0 and 5 and wellness_care is not null then patient_id
 end)*0.5) fifty_pats   
 
  ,ceil(sum(case when report_name = 'Flea/Tick' and nvl(ft_doses,0) between 0 and 5 and wellness_care is not null then 6 - nvl(ft_doses,0)   
   when report_name = 'Heartworm' and nvl(hw_doses,0) between 0 and 5 and wellness_care is not null then 6 - nvl(hw_doses,0)   
   end)*0.5) fifty_doses
  -----------------------------------------------------------
  ,count(case when report_name = 'Flea/Tick' and ft_opp_doses = 0 and wellness_care is not null then patient_id   
when report_name = 'Heartworm' and hw_opp_doses = 0 and wellness_care is not null then patient_id   
end) fully_protected_pats   
 
  from   
  (select * from psr_markets,psr_species,(select * from process_log where flag = 'RUNID' ) hl ) s
  left join psr_patient_master pm on s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and pm.run_id = s.run_id and pm.baby = 1
  left join accounts a on s.location_number = a.location_number and a.hid = s.h_id 
  where s.run_id = gv_input_run_id
  group by s.run_id,s.h_id  
  ,s.location_number  
  ,s.end_date  
  ,report_name 
  ,species 
  ,case when s.report_name = 'Flea/Tick' and s.species = 'Dog' then a.dose_rec_ft_dog
when s.report_name = 'Heartworm' and s.species = 'Dog' then a.dose_rec_hw_dog
when s.report_name = 'Flea/Tick' and s.species = 'Cat' then a.dose_rec_ft_cat
when s.report_name = 'Heartworm' and s.species = 'Cat' then a.dose_rec_hw_cat
end
) ;  
 
commit;  
 
 
-- page 5
insert into psr_t5 
--create or replace view psr_t5 as   
select distinct run_id
,h_id
,location_number   
,end_Date
,report_name   
,species 
,family_name product_name   
,row_order 
,case when sum(qty_pct) over(partition by run_id,report_name,species) > 1 and qty_rank = 1
  then round(qty_pct,2)-(sum(qty_pct) over(partition by run_id,report_name,species)-1)
  when sum(qty_pct) over(partition by run_id,report_name,species) < 1 and qty_rank = 1
  then round(qty_pct,2)+(1-sum(qty_pct) over(partition by run_id,report_name,species))
else sum(qty_pct) over(partition by run_id,species,report_name,family_name) end dose_pct  
--,total_quantity  
from (   
  --sum by new family name  
  select distinct run_id
  ,h_id  
  ,location_number 
  ,end_date
  ,report_name 
  ,species 
  ,family_name 
  ,sum(qty) over (partition by run_id,report_name,species,family_name) qty 
  ,total_qty   
  ,case when total_qty = 0 then 0 else round(sum(qty) over (partition by run_id,report_name,species,family_name)/total_qty,2) end qty_pct
  ,min(row_order) over (partition by run_id,report_name,species,family_name) row_order  
  ,min(qty_rank) over (partition by run_id,report_name,species,family_name) qty_rank 
  from ( 
  --second step gets the ranking while removing the 0 percentages. also convert the product name to other family  
  select run_id
  ,h_id
  ,location_number
  ,end_date
  ,report_name 
  ,species 
  ,case when dense_rank() over(partition by run_id,species,report_name order by decode(family_name,'Frontline',1,'Certifect',1,'Heartgard',1,'Tritak',1,'NexGard',1,2),qty desc,family_name ) >= 6 then 'Other '||report_name else family_name end family_name 
 
  --,round(Rev/total_rev,2) dose_pct  
  ,sum(qty) over(partition by run_id,species,report_name,family_name) qty
  ,total_qty   
  ,case when dense_rank() over(partition by run_id,species,report_name order by decode(family_name,'Frontline',1,'Certifect',1,'Heartgard',1,'Tritak',1,'NexGard',1,2),qty desc,family_name ) > = 6 then 6 
  else dense_rank() over(partition by run_id,species,report_name order by decode(family_name,'Frontline',1,'Certifect',1,'Heartgard',1,'Tritak',1,'NexGard',1,2),qty desc,family_name ) end row_order
  ,case when dense_rank() over(partition by run_id,species,report_name order by qty desc,family_name ) > = 6 then 6  
  else dense_rank() over(partition by run_id,species,report_name order by qty desc,family_name ) end qty_rank  
  from (   
--First step gets all the percentages for each individual products 
select distinct s.run_id
,s.h_id  
,s.location_number
,s.end_date 
,report_name
,s.species  
,family_name
,sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then quantity_norm   
   when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then quantity_norm end)) over(partition by s.run_id,s.species,family_name,s.report_name) qty  
,sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then quantity_norm   
   when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then quantity_norm end)) over(partition by s.run_id,s.species,s.report_name) total_qty  
,case when sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then quantity_norm
 when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then quantity_norm end)) over(partition by s.run_id,s.species,s.report_name) = 0 then 0 else
round(
sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then quantity_norm   
when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then quantity_norm end)) over(partition by s.run_id,s.species,family_name,s.report_name)  
/sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then quantity_norm  
when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then quantity_norm end)) over(partition by s.run_id,s.species,s.report_name)
,2) 
 end 
pct 
from 
(select * from psr_species,(select * from process_log where flag = 'RUNID'  ),psr_markets hl   ) s   
left join psr_product_master pm on s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and pm.run_id = s.run_id  
left join accounts a on s.location_number = a.location_number and a.hid = s.h_id   
where quantity_norm <> 0 and ((s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null) or   
   (s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null)) 
--and s.run_id = 10720
   --   order by 5,6,11 
  ) where pct > 0 
)
)
where run_id = gv_input_run_id  
--where 1=0
order by 1,6,7,9;  
 
commit;  
 
 
-- page 5
insert into psr_t6 
--create or replace view psr_t6 as   
select distinct run_id
,h_id
,location_number   
,end_Date
,report_name   
,species 
,family_name product_name   
,row_order 
,doses_pat 
--,total_quantity  
from (   
  --sum by new family name  
  select distinct run_id
  ,h_id  
  ,location_number 
  ,end_date
  ,report_name 
  ,species 
  ,family_name 
  ,sum(qty) over (partition by run_id,report_name,species,family_name) qty 
  ,sum(patients) over (partition by run_id,report_name,species,family_name) patients 
  ,case when sum(patients) over (partition by run_id,report_name,species,family_name)= 0 then 0 else round(sum(qty) over (partition by run_id,report_name,species,family_name)/sum(patients) over (partition by run_id,report_name,species,family_name),1)
 end doses_pat 
 
  ,min(row_order) over (partition by run_id,report_name,species,family_name) row_order  
  ,min(doses_pat_rank) over (partition by run_id,report_name,species,family_name) doses_pat_rank  
  from ( 
  --second step gets the ranking while removing the 0 percentages. also convert the product name to other family  
  select run_id
  ,h_id
  ,location_number
  ,end_date
  ,report_name 
  ,species 
  ,case when row_order >= 6 then 'Other '||report_name else family_name end family_name   
  ,doses_pat   
  ,qty 
  ,patients
  ,row_order   
  ,case when dense_rank() over(partition by run_id,species,report_name order by doses_pat desc,family_name ) > = 6 then 6  
  else dense_rank() over(partition by run_id,species,report_name order by doses_pat desc,family_name ) end doses_pat_rank
  from (   
--First step gets all the percentages for each individual products 
select distinct s.run_id
,s.h_id  
,s.location_number
,s.end_date 
,report_name
,s.species  
,row_order  
,family_name
,sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then quantity_norm   
   when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then quantity_norm end)) over(partition by s.run_id,s.species,family_name,s.report_name) qty  
,sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then patients  
   when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then patients end)) over(partition by s.run_id,family_name,s.species,s.report_name) patients  
,case when sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then patients  
 when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then patients end)) over(partition by s.run_id,family_name,s.species,s.report_name) = 0 then 0 else round(
  sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then quantity_norm  
 when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then quantity_norm end)) over(partition by s.run_id,s.species,family_name,s.report_name)
  /sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then patients
 when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then patients end)) over(partition by s.run_id,family_name,s.species,s.report_name)
  ,1) end doses_pat   
,case when sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then quantity_norm
 when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then quantity_norm end)) over(partition by s.run_id,s.species,s.report_name) = 0 then 0 else round(   
  sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then quantity_norm  
 when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then quantity_norm end)) over(partition by s.run_id,s.species,family_name,s.report_name)
  /sum((case when s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null then quantity_norm 
 when s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null then quantity_norm end)) over(partition by s.run_id,s.species,s.report_name)   
  ,2) end pct 
  from 
(select run_id,h_id,species,location_number,end_Date,report_name,row_order,product_name from psr_t5) s 
left join psr_product_master pm on s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and pm.run_id = s.run_id and pm.family_name = product_name 
left join accounts a on s.location_number = a.location_number and s.h_id = a.hid   
where quantity_norm <> 0 and ((s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null) or   
   (s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null)) 
 
 
  ) where pct > 0 
) where row_order < 6   
)
where run_id = gv_input_run_id  
--and doses_pat_rank < 6
;
commit;  
 
-- page 6
insert into psr_t7 
select distinct
run_id   
,h_id
,location_number   
,end_Date
,report_name   
,species 
,num_pats
,pats_prod 
,pats_well 
,pats_prod_well
,case when pats_well = 0 then 0 else round(pats_prod_well/pats_well,2) end pats_prod_well_pct 
,pats_well - pats_prod_well pats_no_prod_well   
,case when pats_well = 0 then 0 else round((pats_well - pats_prod_well)/pats_well,2) end pats_no_prod_well_pct
,dose_rec
,round(avg_doses,1) avg_doses   
,round(avg_price_dose,2) avg_price_dose   
,round(total_rev) total_rev 
,round((pats_well - pats_prod_well) * round(avg_doses,1)) avg_pot_doses
,round(round((pats_well - pats_prod_well) * round(avg_doses,1)) * round(avg_price_dose,2)) avg_pot_rev  
,round((pats_well - pats_prod_well) * dose_rec)  rec_doses_pot_doses   
,round(round((pats_well - pats_prod_well) * dose_rec) * round(avg_price_dose,2)) rec_doses_pot_rev
,partial_pats  
,case when pats_well = 0 then 0 else round(partial_pats/pats_well,2) end partial_pats_pct 
,protected_pats fully_protected_pats  
,case when pats_well = 0 then 0 else round(protected_pats/pats_well,2) end fully_protected_pats_pct 
,pats_well - (partial_pats + protected_pats) unprotected_pats
,round(1 - (case when pats_well = 0 then 0 else round(partial_pats/pats_well,2) end + case when pats_well = 0 then 0 else round(protected_pats/pats_well,2) end ),2) unprotected_pats_pct  
from (   
select distinct s.run_id,s.h_id   
,s.location_number
,s.end_date
,report_name   
,species   
-- Total Patients 
,count(patient_id) over(partition by s.run_id,report_name,species) num_pats  
 
-- total flea/tick or hw paitnets receiving product
,count(case when ft_doses > 0 and report_name = 'Flea/Tick' then patient_id  
  when hw_doses > 0 and report_name = 'Heartworm' then patient_id  
  end) over(partition by s.run_id,report_name,species) pats_prod   
 
-- total patients with a wellness visit 
,count(case when wellness_care is not null then patient_id end) over(partition by s.run_id,report_name,species) pats_well  
-- total patients with a wellness visit and on a fleatick or hw product
,count(case when ((ft_doses > 0 and report_name = 'Flea/Tick') or  
  (hw_doses > 0 and report_name = 'Heartworm'))
   and wellness_care is not null then patient_id end) over(partition by s.run_id,report_name,species)   
pats_prod_well 
 
 
 
 
-- dose reccomendationk 
,case when s.report_name = 'Flea/Tick' and s.species = 'Dog' then a.dose_rec_ft_dog 
when s.report_name = 'Heartworm' and s.species = 'Dog' then a.dose_rec_hw_dog 
when s.report_name = 'Flea/Tick' and s.species = 'Cat' then a.dose_rec_ft_cat 
when s.report_name = 'Heartworm' and s.species = 'Cat' then a.dose_rec_hw_cat 
end dose_rec
 
 --UPDATED 
-- average number of doses for all patients 
,case when count(distinct case when report_name = 'Flea/Tick' and wellness_care is not null and ft_doses is not null then patient_id 
when report_name = 'Heartworm' and wellness_care is not null and hw_doses is not null then patient_id end) over(partition by s.run_id,report_name,species) = 0 then 0 else   
 
  nvl(round(sum(case when report_name = 'Flea/Tick' and wellness_care is not null then ft_doses 
when report_name = 'Heartworm' and wellness_care is not null then hw_doses end) over(partition by s.run_id,report_name,species)/  
 count(distinct case when report_name = 'Flea/Tick' and wellness_care is not null and ft_doses is not null then patient_id 
when report_name = 'Heartworm' and wellness_care is not null and hw_doses is not null then patient_id end) over(partition by s.run_id,report_name,species)  
 ,1),0) end   
avg_doses  
/*   
-- average number of doses for all patients 
,nvl(round(avg(case when report_name = 'Flea/Tick' and wellness_care is not null then ft_doses
when report_name = 'Heartworm' and wellness_care is not null then hw_doses end) over(partition by s.run_id,report_name,species),1),0)   
avg_doses  
*/   
 
--average price per dose
,  case when sum(case when report_name = 'Flea/Tick' and wellness_care is not null then ft_doses
when report_name = 'Heartworm' and wellness_care is not null then hw_doses
else 0 end) over(partition by s.run_id,report_name,species) =0 then 0 else
 
round(sum(case when report_name = 'Flea/Tick' and wellness_care is not null then ft_rev   
when report_name = 'Heartworm' and wellness_care is not null then hw_rev  
else 0 end) over(partition by s.run_id,report_name,species)  
/sum(case when report_name = 'Flea/Tick' and wellness_care is not null then ft_doses
when report_name = 'Heartworm' and wellness_care is not null then hw_doses
else 0 end) over(partition by s.run_id,report_name,species),2) end  
 
  avg_price_dose  
 
,nvl(round(sum(case when report_name = 'Flea/Tick' and wellness_care is not null then ft_rev  
when report_name = 'Heartworm' and wellness_care is not null then hw_rev end) over(partition by s.run_id,report_name,species)),0) total_rev 
 
-- count of patients who purchased product but less than the recommended number of doses  
,count(case when report_name = 'Flea/Tick' and vi_species_id = 3 and ft_doses between 1 and (a.dose_rec_ft_dog - 1) and wellness_care is not null then patient_id 
  when report_name = 'Heartworm' and vi_species_id = 3 and hw_doses between 1 and (a.dose_rec_hw_dog - 1) and wellness_care is not null then patient_id 
  when report_name = 'Flea/Tick' and vi_species_id = 7 and ft_doses between 1 and (a.dose_rec_ft_cat - 1) and wellness_care is not null then patient_id 
  when report_name = 'Heartworm' and vi_species_id = 7 and hw_doses between 1 and (a.dose_rec_hw_cat - 1) and wellness_care is not null then patient_id 
  end) over(partition by s.run_id,report_name,species) partial_pats
 
 
,count(case when report_name = 'Flea/Tick' and ft_opp_doses = 0 and wellness_care is not null then patient_id 
  when report_name = 'Heartworm' and hw_opp_doses = 0 and wellness_care is not null then patient_id 
  end) over(partition by s.run_id,report_name,species) protected_pats  
   -- fully_protected_pats_pct  
 
,patient_id
from 
(select * from psr_markets,psr_species,(select * from process_log where flag = 'RUNID' ) hl  ) s 
left join psr_patient_master pm on s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and pm.run_id = s.run_id  
left join accounts a on s.location_number = a.location_number and a.hid = s.h_id 
)
where run_id = gv_input_run_id  
;
 
 
commit;  
 
-- original page 1 
 
 
insert into psr_t8 
--create or replace view psr_t8 as   
select s.run_id
,s.h_id  
,s.location_number 
,s.end_date
,s.report_name 
,s.species 
,nvl(count(patient_id),0) num_pats
,count(case when baby is not null then patient_id end) num_babies
,case when count(patient_id) = 0 then 0 else round(sum(total_rev)/count(patient_id),2) end avg_rev_pat  
,nvl(sum(pm.visits),0) total_visits   
,case when sum(pm.visits) = 0 then 0 else nvl(round(sum(total_rev)/sum(pm.visits),2),0) end avg_rev_visit   
,nvl(round(median(visits)),0) median_visits 
,nvl(round(sum(case when s.report_name = 'Flea/Tick' then ft_rev 
  when s.report_name = 'Heartworm' then hw_rev end),0),0) total_market_rev
,nvl(sum(case when s.report_name = 'Flea/Tick' then ft_doses when s.report_name = 'Heartworm' then hw_doses end),0) total_doses
,nvl(sum(case when s.report_name = 'Flea/Tick' then ft_visits when s.report_name = 'Heartworm' then hw_visits end),0) total_market_trans 
 
,case when sum(case when s.report_name = 'Flea/Tick' then ft_visits when s.report_name = 'Heartworm' then hw_visits end) = 0 then 0 else 
  nvl(round(sum(case when s.report_name = 'Flea/Tick' then ft_doses when s.report_name = 'Heartworm' then hw_doses end)
  /sum(case when s.report_name = 'Flea/Tick' then ft_visits when s.report_name = 'Heartworm' then hw_visits end),2),0) end avg_doses_trans 
 
,case when s.report_name = 'Flea/Tick' and s.species = 'Dog' then a.dose_rec_ft_dog  
  when s.report_name = 'Heartworm' and s.species = 'Dog' then a.dose_rec_hw_dog  
  when s.report_name = 'Flea/Tick' and s.species = 'Cat' then a.dose_rec_ft_cat  
  when s.report_name = 'Heartworm' and s.species = 'Cat' then a.dose_rec_hw_cat  
  end dose_rec 
 
,case when count(patient_id) = 0 then 0 else round(sum(case when s.report_name = 'Flea/Tick' then ft_doses when s.report_name = 'Heartworm' then hw_doses end)/count(patient_id),2) end avg_doses_pat
 
,count(case when ((ft_doses is null and report_name = 'Flea/Tick') or  
(hw_doses is null and report_name = 'Heartworm'))
then patient_id end) pats_no_prod 
 
,count(case when ((ft_doses is null and report_name = 'Flea/Tick') or  
(hw_doses is null and report_name = 'Heartworm'))
then decode(baby,1,patient_id) end) babies_no_prod   
 
,count(case when s.report_name = 'Flea/Tick' and ft_opp_doses = 0 then patient_id
  when s.report_name = 'Heartworm' and hw_opp_doses = 0 then patient_id
  end) pats_compliant 
 
,round(case when count(patient_id) = 0 then 0 else count(case when s.report_name = 'Flea/Tick' and ft_opp_doses = 0 then patient_id
when s.report_name = 'Heartworm' and hw_opp_doses = 0 then patient_id 
end)/count(patient_id) end,2) pct_pats_compliant 
 
,round(case when nvl(round(sum(total_rev)),0) = 0 then 0 
  when s.report_name = 'Flea/Tick' then sum(ft_rev)/nvl(round(sum(total_rev)),0)
  when s.report_name = 'Heartworm' then sum(hw_rev)/nvl(round(sum(total_rev)),0) end,2)  market_rev_pct 
 
,count(case when hw_tests > 0 then hw_tests end) num_pats_hw_test
 
,count(case when ((ft_doses is null and report_name = 'Flea/Tick') or  
(hw_doses is null and report_name = 'Heartworm'))
and hw_Tests > 0 then hw_tests end) num_pats_hw_test_no_prod   
 
from 
(select * from psr_markets,psr_species,(select * from process_log where flag = 'RUNID' ) hl ) s  
left join psr_patient_master pm on s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and pm.run_id = s.run_id and wellness_care is not null  
left join accounts a on s.location_number = a.location_number and s.h_id = a.hid
where s.run_id = gv_input_run_id
group by s.run_id,s.h_id
,s.location_number 
,s.end_date
,report_name   
,species 
,case when s.report_name = 'Flea/Tick' and s.species = 'Dog' then a.dose_rec_ft_dog  
  when s.report_name = 'Heartworm' and s.species = 'Dog' then a.dose_rec_hw_dog  
  when s.report_name = 'Flea/Tick' and s.species = 'Cat' then a.dose_rec_ft_cat  
  when s.report_name = 'Heartworm' and s.species = 'Cat' then a.dose_rec_hw_cat  
  end ;
 
commit;  
 
 
insert into psr_t10
--create or replace view psr_t10 as  
select distinct run_id
,h_id
,location_number   
,end_Date
,report_name   
,species 
,family_name product_name   
,row_order 
,case when sum(rev_pct) over(partition by run_id,report_name,species) > 1 and rev_rank = 1
  then round(rev_pct,2)-(sum(rev_pct) over(partition by run_id,report_name,species)-1)
  when sum(rev_pct) over(partition by run_id,report_name,species) < 1 and rev_rank = 1
  then round(rev_pct,2)+(1-sum(rev_pct) over(partition by run_id,report_name,species))
else sum(rev_pct) over(partition by run_id,species,report_name,family_name) end rev_pct   
--,total_quantity  
from (   
  --sum by new family name  
  select distinct run_id
  ,h_id  
  ,location_number 
  ,end_date
  ,report_name 
  ,species 
  ,family_name 
  ,sum(rev) over (partition by run_id,report_name,species,family_name) rev 
  ,total_rev   
  ,case when total_rev = 0 then 0 else round(sum(rev) over (partition by run_id,report_name,species,family_name)/total_rev,2) end rev_pct
  ,min(row_order) over (partition by run_id,report_name,species,family_name) row_order  
  ,min(rev_rank) over (partition by run_id,report_name,species,family_name) rev_rank 
  from ( 
  --second step gets the ranking while removing the 0 percentages. also convert the product name to other family  
  select run_id
  ,h_id
  ,location_number
  ,end_date
  ,report_name 
  ,species 
  ,case when dense_rank() over(partition by run_id,species,report_name order by decode(family_name,'Frontline',1,'Certifect',1,'Heartgard',1,'Tritak',1,'NexGard',1,2),rev desc,family_name ) >= 6 then 'Other '||report_name else family_name end family_name 
 
  --,round(Rev/total_rev,2) dose_pct  
  ,sum(rev) over(partition by run_id,species,report_name,family_name) rev
  ,total_rev   
  ,case when dense_rank() over(partition by run_id,species,report_name order by decode(family_name,'Frontline',1,'Certifect',1,'Heartgard',1,'Tritak',1,'NexGard',1,2),rev desc,family_name ) > = 6 then 6 
  else dense_rank() over(partition by run_id,species,report_name order by decode(family_name,'Frontline',1,'Certifect',1,'Heartgard',1,'Tritak',1,'NexGard',1,2),rev desc,family_name ) end row_order
  ,case when dense_rank() over(partition by run_id,species,report_name order by rev desc,family_name ) > = 6 then 6  
  else dense_rank() over(partition by run_id,species,report_name order by rev desc,family_name ) end rev_rank  
  from (   
--First step gets all the percentages for each individual products 
select distinct s.run_id
,s.h_id  
,s.location_number
,s.end_date 
,report_name
,s.species  
,family_name
,round(sum(decode(s.report_name,'Flea/Tick',ft_rev,'Heartworm',hw_rev)) over(partition by s.run_id,s.species,family_name,s.report_name),0) rev  
,round(sum(decode(s.report_name,'Flea/Tick',ft_rev,'Heartworm',hw_rev)) over(partition by s.run_id,s.species,s.report_name),0) total_rev 
,case when sum(decode(report_name,'Flea/Tick',ft_rev,'Heartworm',hw_rev)) over(partition by s.run_id,s.species,s.report_name) = 0 then 0 else round(sum(decode(s.report_name,'Flea/Tick',ft_rev,'Heartworm',hw_rev)) over(partition by s.run_id,
s.species,family_name,s.report_name)/sum(decode(report_name,'Flea/Tick',ft_rev,'Heartworm',hw_rev)) over(partition by s.run_id,s.species,s.report_name) ,2) end pct 
 
from 
(select * from psr_species,(select * from process_log where flag = 'RUNID' ),psr_markets   ) s   
left join psr_product_master pm on s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and pm.run_id = s.run_id  
left join accounts a on s.location_number = a.location_number and s.h_id = a.hid   
where quantity_norm <> 0 and ((s.report_name = 'Flea/Tick' and ft_rev <> 0 and ft_rev is not null) or   
   (s.report_name = 'Heartworm' and hw_rev <> 0 and hw_rev is not null)) 
  ) where pct > 0 
)
)
where run_id = gv_input_run_id  
and rev_pct > 0
;
 
 
 
 
commit;  
 
 
 
insert into psr_t11
--create or replace view psr_t11 as  
select s.run_id
,s.h_id  
,s.location_number 
,s.end_date
,s.report_name 
,s.species 
,to_char(add_months(s.end_date,-1*(row_order-1)),'Mon') month
,(12-row_order)+1 column_order  
,nvl(total_patients,0) total_patients 
,nvl(total_patients-case when s.report_name = 'Flea/Tick' then pm.ft_patients
  when s.report_name = 'Heartworm' then pm.hw_patients   
  end,0) pats_no_prod   
,nvl(case when s.report_name = 'Flea/Tick' then pm.ft_patients   
  when s.report_name = 'Heartworm' then pm.hw_patients   
  end,0) pats_prod
,nvl(case when s.report_name = 'Flea/Tick' then pm.ft_doses  
  when s.report_name = 'Heartworm' then pm.hw_doses
  end,0) total_doses
from 
(select * from psr_markets,psr_month_order,psr_species,(select * from process_log where flag = 'RUNID' ) hl ) s
left join psr_month_master pm on s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and pm.run_id = s.run_id and add_months(s.end_date,-1*(row_order-1)) = pm.data_month 
left join accounts a on s.location_number = a.location_number and s.h_id = a.hid
where s.run_id = gv_input_run_id; 
 
 
commit;  
 
 
 
insert into psr_t12
--create or replace view psr_t12 as  
select d.run_id,d.h_id,d.location_number,d.end_date,d.report_name,d.species
,case when d.report_name = 'Flea/Tick' and d.species = 'Dog' then b.dose_rec_ft_dog  
  when d.report_name = 'Heartworm' and d.species = 'Dog' then b.dose_rec_hw_dog  
  when d.report_name = 'Flea/Tick' and d.species = 'Cat' then b.dose_rec_ft_cat  
  when d.report_name = 'Heartworm' and d.species = 'Cat' then b.dose_rec_hw_cat  
  end dose_rec 
,d.doses 
,nvl(patients,0) patients   
 
 
,case when (   
case when d.doses = '13+' then 0 else (case when d.report_name = 'Flea/Tick' and d.species = 'Dog' then b.dose_rec_ft_dog  
   when d.report_name = 'Heartworm' and d.species = 'Dog' then b.dose_rec_hw_dog
   when d.report_name = 'Flea/Tick' and d.species = 'Cat' then b.dose_rec_ft_cat
   when d.report_name = 'Heartworm' and d.species = 'Cat' then b.dose_rec_hw_cat
   end) - to_number(d.doses)
end) < 0 then 0
 
else 
 
case when d.doses = '13+' then 0 else (case when d.report_name = 'Flea/Tick' and d.species = 'Dog' then b.dose_rec_ft_dog  
   when d.report_name = 'Heartworm' and d.species = 'Dog' then b.dose_rec_hw_dog
   when d.report_name = 'Flea/Tick' and d.species = 'Cat' then b.dose_rec_ft_cat
   when d.report_name = 'Heartworm' and d.species = 'Cat' then b.dose_rec_hw_cat
   end) - to_number(d.doses)
end  
end  missed_doses  
 
 
,nvl(missed_doses,0) total_missed_doses   
from (select * from psr_markets,psr_species,psr_doses_2,(select * from process_log where flag = 'RUNID' ) hl ) d   
left join (
  select 
  s.run_id,s.h_id,s.report_name,s.species 
  ,case when s.report_name = 'Flea/Tick' and s.species = 'Dog' then a.dose_rec_ft_dog
when s.report_name = 'Heartworm' and s.species = 'Dog' then a.dose_rec_hw_dog
when s.report_name = 'Flea/Tick' and s.species = 'Cat' then a.dose_rec_ft_cat
when s.report_name = 'Heartworm' and s.species = 'Cat' then a.dose_rec_hw_cat
end dose_rec  
 
  ,nvl(case when s.report_name = 'Flea/Tick' and ft_doses >= 13 then '13+' 
when s.report_name = 'Heartworm' and HW_doses >= 13 then '13+' 
when s.report_name = 'Flea/Tick' and (ft_doses is null or ft_doses <= 0) then '0'   
when s.report_name = 'Heartworm' and (hw_doses is null or hw_doses <= 0) then '0'   
when s.report_name = 'Flea/Tick' then to_char(ft_doses)  
when s.report_name = 'Heartworm' then to_char(HW_doses)  
end,0) doses  
 
  ,count(distinct case when s.report_name = 'Flea/Tick' then patient_id
   when s.report_name = 'Heartworm' then patient_id end ) patients  
  -- when the number of doses > the rec doses then return 0 missed doses else return REC_DOSES-ACTUAL_DOSES 
  ,sum(case when s.report_name = 'Flea/Tick' then ft_opp_doses   
  when s.report_name = 'Heartworm' then hw_opp_doses   
  end) missed_doses   
  from   
  (select * from psr_markets,psr_species,(select * from process_log where flag = 'RUNID' ) ) s   
  left join psr_patient_master pm on pm.run_id = s.run_id and s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and WELLNESS_CARE is not null
  left join accounts a on s.location_number = a.location_number and s.h_id = a.hid 
  --where s.run_id = 10510  
  group by s.run_id,s.h_id,s.location_number,s.species,s.report_name   
  ,case when s.report_name = 'Flea/Tick' and s.species = 'Dog' then a.dose_rec_ft_dog
when s.report_name = 'Heartworm' and s.species = 'Dog' then a.dose_rec_hw_dog
when s.report_name = 'Flea/Tick' and s.species = 'Cat' then a.dose_rec_ft_cat
when s.report_name = 'Heartworm' and s.species = 'Cat' then a.dose_rec_hw_cat
end
  ,case when s.report_name = 'Flea/Tick' and ft_doses >= 13 then '13+' 
when s.report_name = 'Heartworm' and HW_doses >= 13 then '13+' 
when s.report_name = 'Flea/Tick' and (ft_doses is null or ft_doses <= 0) then '0'   
when s.report_name = 'Heartworm' and (hw_doses is null or hw_doses <= 0) then '0'   
when s.report_name = 'Flea/Tick' then to_char(ft_doses)  
when s.report_name = 'Heartworm' then to_char(HW_doses)  
end  
  ) a on d.doses = a.doses and d.species = a.species and a.run_id = d.run_id and a.report_name = d.report_name 
left join accounts b on d.location_number = b.location_number and d.h_id = b.hid
where d.run_id = gv_input_run_id
;
 
commit;  
 
 
 
insert into psr_t13a   
select run_id,h_id,location_number,end_date,report_name,species , dose_rec, doses, patients--, row_order
, case when sum(pct_pats) over(partition by run_id, report_name,species) > 1 and row_order = 1 then pct_pats - (sum(pct_pats) over(partition by run_id, report_name,species)-1) 
  when sum(pct_pats) over(partition by run_id, report_name,species) < 1 and row_order = 1 then pct_pats + (1-sum(pct_pats) over(partition by run_id, report_name,species))
  else pct_pats
  end pct_pats 
from (   
select d.run_id,d.h_id,d.location_number,d.end_date,d.report_name,d.species
,case when d.report_name = 'Flea/Tick' and d.species = 'Dog' then b.dose_rec_ft_dog  
  when d.report_name = 'Heartworm' and d.species = 'Dog' then b.dose_rec_hw_dog  
  when d.report_name = 'Flea/Tick' and d.species = 'Cat' then b.dose_rec_ft_cat  
  when d.report_name = 'Heartworm' and d.species = 'Cat' then b.dose_rec_hw_cat  
  end dose_rec 
,d.doses 
,nvl(patients,0) patients   
,round(ratio_to_report(nvl(patients,0)) over(partition by case when d.report_name = 'Flea/Tick' and d.species = 'Dog' then b.dose_rec_ft_dog 
  when d.report_name = 'Heartworm' and d.species = 'Dog' then b.dose_rec_hw_dog  
  when d.report_name = 'Flea/Tick' and d.species = 'Cat' then b.dose_rec_ft_cat  
  when d.report_name = 'Heartworm' and d.species = 'Cat' then b.dose_rec_hw_cat  
  end,d.report_name,d.species,d.run_id),2)  pct_pats 
, dense_rank() over(partition by case when d.report_name = 'Flea/Tick' and d.species = 'Dog' then b.dose_rec_ft_dog   
  when d.report_name = 'Heartworm' and d.species = 'Dog' then b.dose_rec_hw_dog  
  when d.report_name = 'Flea/Tick' and d.species = 'Cat' then b.dose_rec_ft_cat  
  when d.report_name = 'Heartworm' and d.species = 'Cat' then b.dose_rec_hw_cat  
  end,d.report_name,d.species,d.run_id order by nvl(patients,0) desc) row_order  
 
from (select * from psr_markets,psr_species,psr_doses_3,(select * from process_log where flag = 'RUNID' ) hl ) d   
left join (
select   
s.run_id,s.h_id,s.report_name,s.species   
 
,case when s.report_name = 'Flea/Tick' and ft_doses >= 12 then '12+'   
  when s.report_name = 'Heartworm' and HW_doses >= 12 then '12+'   
  when s.report_name = 'Flea/Tick' and (ft_doses is null or ft_doses <= 0) then '0' 
  when s.report_name = 'Heartworm' and (HW_doses is null or hw_doses <= 0) then '0' 
  when s.report_name = 'Flea/Tick' and ft_doses = 1 then '1' 
  when s.report_name = 'Heartworm' and HW_doses = 1 then '1' 
  else '2-11'  
  end doses
 
,count(case when s.report_name = 'Flea/Tick' then patient_id 
 when s.report_name = 'Heartworm' then patient_id end ) patients 
-- when the number of doses > the rec doses then return 0 missed doses else return REC_DOSES-ACTUAL_DOSES   
 
from 
(select * from psr_markets,psr_species,(select * from process_log where flag = 'RUNID') ) s
left join psr_patient_master pm on pm.run_id = s.run_id and s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and WELLNESS_CARE is not null  
left join accounts a on s.location_number = a.location_number and a.hid = s.h_id
--where s.run_id = 10510
group by s.run_id,s.h_id,s.location_number,s.species,s.report_name 
,case when s.report_name = 'Flea/Tick' and s.species = 'Dog' then a.dose_rec_ft_dog  
  when s.report_name = 'Heartworm' and s.species = 'Dog' then a.dose_rec_hw_dog  
  when s.report_name = 'Flea/Tick' and s.species = 'Cat' then a.dose_rec_ft_cat  
  when s.report_name = 'Heartworm' and s.species = 'Cat' then a.dose_rec_hw_cat  
  end
,case when s.report_name = 'Flea/Tick' and ft_doses >= 12 then '12+'   
  when s.report_name = 'Heartworm' and HW_doses >= 12 then '12+'   
  when s.report_name = 'Flea/Tick' and (ft_doses is null or ft_doses <= 0) then '0' 
  when s.report_name = 'Heartworm' and (HW_doses is null or hw_doses <= 0) then '0' 
  when s.report_name = 'Flea/Tick' and ft_doses = 1 then '1' 
  when s.report_name = 'Heartworm' and HW_doses = 1 then '1' 
  else '2-11'  
  end
  ) a on d.doses = a.doses and d.species = a.species and a.run_id = d.run_id and a.report_name = d.report_name 
left join accounts b on d.location_number = b.location_number and d.h_id = b.hid)  
where run_id = gv_input_run_id; 
 
commit;  
 
insert into psr_t13
select a.run_id
,a.h_id  
,a.location_number 
,a.end_date
,a.report_name 
,decode(a.species,'Canine','Dog','Cat') species 
,a.dose_rec
,a.doses 
,nvl(b.patients,0) patients 
,case when sum(nvl(percent_patients,0)) over (partition by a.run_id,a.report_name,a.species) <> 1 -- when sum is not 100%
  and nvl(percent_patients,0) = max(nvl(percent_patients,0)) over(partition by a.run_id,a.report_name,a.species)  
  then nvl(percent_patients,0) + (1-sum(nvl(percent_patients,0)) over (partition by a.run_id,a.report_name,a.species)) 
  else nvl(percent_patients,0) end pct_pats 
,order_number row_order 
from 
(select pl.run_id, d1.species, d1.report_name, d1.doses, order_number, pl.end_Date,ac.location_number,ac.hid h_id, ac.dose_rec_ft_dog dose_rec 
  from process_log pl  
  inner join accounts ac on ac.location_number = pl.location_number and ac.hid = pl.h_id 
  inner join psr_t13_doses d1 on d1.rec_doses = nvl(ac.dose_rec_ft_dog,0) and d1.species = 'Canine' and d1.report_name = 'Flea/Tick'
  where flag = 'RUNID'
  union  
  select pl.run_id, d1.species, d1.report_name, d1.doses, order_number, pl.end_Date,ac.location_number,ac.hid, ac.dose_rec_hw_dog  
  from process_log pl  
  inner join accounts ac on ac.location_number = pl.location_number and ac.hid = pl.h_id 
  inner join psr_t13_doses d1 on d1.rec_doses = nvl(ac.dose_rec_hw_dog,0) and d1.species = 'Canine' and d1.report_name = 'Heartworm'
  where flag = 'RUNID'
  union  
  select pl.run_id, d1.species, d1.report_name, d1.doses, order_number, pl.end_Date,ac.location_number,ac.hid, ac.dose_rec_ft_cat  
  from process_log pl  
  inner join accounts ac on ac.location_number = pl.location_number and ac.hid = pl.h_id 
  inner join psr_t13_doses d1 on d1.rec_doses = nvl(ac.dose_rec_ft_cat,0) and d1.species = 'Feline' and d1.report_name = 'Flea/Tick'
  where flag = 'RUNID'
  union  
  select pl.run_id, d1.species, d1.report_name, d1.doses, order_number, pl.end_Date,ac.location_number,ac.hid, ac.dose_rec_hw_cat  
  from process_log pl  
  inner join accounts ac on ac.location_number = pl.location_number and ac.hid = pl.h_id 
  inner join psr_t13_doses d1 on d1.rec_doses = nvl(ac.dose_rec_hw_cat,0) and d1.species = 'Feline' and d1.report_name = 'Heartworm'
  where flag = 'RUNID'
  ) a
left join (
select distinct
run_id   
,end_date
,species 
,report_name   
,case when doses = 0 then '0 Doses'   
  when recommended_doses = 0 and doses >= 1 then '0+ Doses'  
  when recommended_doses = 1 and doses >= 1 then '1+ Doses'  
  when recommended_doses > 1 and doses = 1 then '1 Dose' 
  when recommended_doses = 3 and doses = 2 then '2 Doses'
  when recommended_doses > 3 and doses between 2 and recommended_doses-1 then '2-'||cast(recommended_doses-1 as varchar2(2))||' Doses'   
  else cast(recommended_doses as varchar2(2)) ||'+ Doses'
  end doses
,count(distinct patient_id) over(partition by run_id,species,report_name   
  ,case when doses = 0 then '0 Doses' 
  when recommended_doses = 0 and doses >= 1 then '0+ Doses'  
  when recommended_doses = 1 and doses >= 1 then '1+ Doses'  
  when recommended_doses > 1 and doses = 1 then '1 Dose' 
  when recommended_doses = 3 and doses = 2 then '2 Doses'
  when recommended_doses > 3 and doses between 2 and recommended_doses-1 then '2-'||cast(recommended_doses-1 as varchar2(2))||' Doses'   
  else cast(recommended_doses as varchar2(2)) ||'+ Doses'
  end) patients
,case when count(distinct patient_id) over(partition by run_id,species,report_name) = 0 then 0 else round(count(distinct patient_id) over(partition by run_id,species,report_name   
  ,case when doses = 0 then '0 Doses' 
  when recommended_doses = 0 and doses >= 1 then '0+ Doses'  
  when recommended_doses = 1 and doses >= 1 then '1+ Doses'  
  when recommended_doses > 1 and doses = 1 then '1 Dose' 
  when recommended_doses = 3 and doses = 2 then '2 Doses'
  when recommended_doses > 3 and doses between 2 and recommended_doses-1 then '2-'||cast(recommended_doses-1 as varchar2(2))||' Doses'   
  else cast(recommended_doses as varchar2(2)) ||'+ Doses'
  end)/count(distinct patient_id) over(partition by run_id,species,report_name),2) end percent_patients 
from (   
select   
patient_id 
,pm.run_id 
,decode(vi_species_id,3,'Canine','Feline') species  
,'Flea/Tick' report_name
,pm.end_date   
,sum(nvl(ft_doses,0)) doses 
,decode(vi_species_id,3,nvl(ac.dose_rec_ft_dog,0),nvl(ac.dose_rec_ft_cat,0))  recommended_doses   
from psr_patient_master pm 
inner join process_log hl on hl.run_id = pm.run_id and hl.flag = 'RUNID'
inner join accounts ac on ac.location_number = hl.location_number and ac.hid = hl.h_id   
where  WELLNESS_CARE is not null
group by vi_species_id,patient_id,pm.run_id,pm.end_date,decode(vi_species_id,3,nvl(ac.dose_rec_ft_dog,0),nvl(ac.dose_rec_ft_cat,0))
union
select   
patient_id 
,pm.run_id 
,decode(vi_species_id,3,'Canine','Feline') species  
,'Heartworm' report_name
,pm.end_date   
,sum(nvl(hw_doses,0)) doses 
,decode(vi_species_id,3,nvl(ac.dose_rec_hw_dog,0),nvl(ac.dose_rec_hw_cat,0)) recommended_doses
from psr_patient_master pm 
inner join process_log hl on hl.run_id = pm.run_id and hl.flag = 'RUNID'
inner join accounts ac on ac.location_number = hl.location_number and ac.hid = hl.h_id   
where WELLNESS_CARE is not null 
group by vi_species_id,patient_id,pm.run_id,pm.end_date,decode(vi_species_id,3,nvl(ac.dose_rec_hw_dog,0),nvl(ac.dose_rec_hw_cat,0))
)
)
b on a.report_name = b.report_name
and a.run_id = b.run_id 
and a.species = b.species   
and a.doses = b.doses 
and a.end_date = b.end_date 
where a.run_id = gv_input_run_id
;
 
commit;  
 
 
insert into psr_t14
--create or replace view psr_t14 as  
select s.run_id,s.h_id,s.location_number,s.end_date,s.report_name,s.species
,count(case when s.report_name = 'Flea/Tick' and ft_opp_doses > 0 then patient_id
  when s.report_name = 'Heartworm' and hw_opp_doses > 0 then patient_id
  end) non_compliant_pats   
,nvl(sum(case when s.report_name = 'Flea/Tick' then ft_opp_doses 
when s.report_name = 'Heartworm' then hw_opp_doses 
end),0) total_missed_doses  
,nvl(case when sum(case when s.report_name = 'Flea/Tick' then ft_doses when s.report_name = 'Heartworm' then hw_doses end) = 0 then 0 else   
round(sum(case when s.report_name = 'Flea/Tick' then ft_rev when s.report_name = 'Heartworm' then hw_rev end)  
/sum(case when s.report_name = 'Flea/Tick' then ft_doses when s.report_name = 'Heartworm' then hw_doses end),2) end,0) avg_rev_dose
,nvl(case when sum(
 case when s.report_name = 'Flea/Tick' then ft_doses 
when s.report_name = 'Heartworm' then hw_doses 
end)= 0 then 0 else   
  round((sum(case when s.report_name = 'Flea/Tick' then ft_rev 
  when s.report_name = 'Heartworm' then hw_rev 
  end)/ sum(  
 case when s.report_name = 'Flea/Tick' then ft_doses 
when s.report_name = 'Heartworm' then hw_doses 
end))*sum(case when s.report_name = 'Flea/Tick' then ft_opp_doses
when s.report_name = 'Heartworm' then hw_opp_doses 
end),0) end,0) pot_rev  
,nvl(round(sum(case when s.report_name = 'Flea/Tick' then ft_rev 
when s.report_name = 'Heartworm' then hw_rev 
end),0),0) total_rev  
 
from 
(select * from psr_markets,psr_species,(select * from process_log where flag = 'RUNID' ) ) s 
left join psr_patient_master pm on pm.run_id = s.run_id and s.species = decode(pm.vi_Species_id,3,'Dog','Cat') and WELLNESS_CARE is not null  
left join accounts a on s.location_number = a.location_number and a.hid = s.h_id
where s.run_id = gv_input_run_id
 
group by s.run_id,s.h_id,s.location_number,s.end_date,s.report_name,s.species;   
 
--delete from psr_patient_master where run_id = gv_input_run_id;  
commit;  
 
build_process_log('INFO','Finished running VIEW DATA for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_PSR_VIEWS');
 
 
exception
when others then   
  rollback;
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed to init:'||SQLERRM,'BUILD_PSR_VIEWS');
  raise; 
end build_psr_views;   
 

 
PROCEDURE build_consumer as
--prod_run_failed exception;
split_count number;
count_patients number;
no_patients exception;
begin
 
 
select location_number into gv_input_location_number from process_log where flag = 'RUNID' and run_id = gv_input_run_id;  
 
select count(*) into split_count from split_clients where location_number = gv_input_location_number and h_id = GV_INPUT_HID and rownum < 2;  
 
-- write to process log indicating the start of the procedure to populate the patient data for this RUNID   
--if gv_db_run_from = 'DBRAW' then
-- this creates one row per patient in the patient master table. 
build_process_log('STARTED','Running CONSUMER for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER');  
 
--build_process_log('INFO','Finished running PATIENT_REF for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER'); 
 
-- get all cats and dogs
-- active, not deceased 
-- last active in the past 24 months  
insert into consumer_patients   
select distinct first_active,last_active  
,p.is_active, p.vi_species_id, p.dob  
,p.patient_id, client_id, h_id  
,gv_input_run_id   
,gv_input_end_date 
,months_between(gv_input_end_date,dob)/12 age_years 
,case when months_between(gv_input_end_date,dob)/12 > 6 and vi_species_id = 3 then 1 else 0 end dogs_over_6 
,case when months_between(gv_input_end_date,dob)/12 < 1 and vi_species_id = 3 then 1 else 0 end puppy   
,case when months_between(gv_input_end_date,dob)/12 > 4 and vi_species_id = 3 then 1 else 0 end dogs_over_4 
from cooked.patient_ref p   
where last_active > add_months(trunc(to_Date(gv_input_end_Date),'mm'),-23) 
and is_active = 1  
and is_deceased = 0
and vi_species_id in (3,7)  
and patient_id > 0 
and h_id = gv_input_hid;
 
select count(*) into count_patients from consumer_patients where run_id = gv_input_run_id;  
 
if count_patients = 0 then  
  raise no_patients;  
end if;  
 
build_process_log('INFO','Finished running PATIENTS for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER');
 
if split_count > 0 then 
 
	insert into consumer_clients   
	select distinct c.client_id
	,nvl(regexp_replace(regexp_replace(c.first_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]|\^|\*|\)|\(|\:|\+)', null, 1,0,'i'),'None Given') first_name   
	,nvl(regexp_replace(regexp_replace(c.last_name,  '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]|\^|\*|\)|\(|\:|\+)', null, 1,0,'i'),'None Given') last_name
	,case when regexp_like(c.title,'([^[:alpha:]\ |\.]|\\)+','i') then null else
  regexp_replace(c.title,'([^[:alpha:]\ |\.]|\\)+', null, 1,0,'i') end prefix
	,nvl(regexp_replace(regexp_replace(a.address1, 'do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i'),'None Given') address1  
	,regexp_replace(regexp_replace(a.address2, 'do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i') address2 
	,regexp_replace(a.city,'([[:cntrl:]]+)', null, 1,0,'i') city
	,regexp_replace(a.state,'([[:cntrl:]]+)', null, 1,0,'i') state  
	,regexp_replace(lpad(substr((case when regexp_like(postal_code, '([^0-9])') then regexp_replace(postal_code, '([^0-9])', null, 1, 0, 'i') else postal_code end),1,9),30),'([[:cntrl:]]+)', null, 1,0,'i') postal_code   
	,c.h_id 
	,dogs_over_6  
	,puppy  
	,p.run_id 
	,p.end_Date   
	,p.canines
	,p.felines
	,c.primary_email  
	,regexp_replace(substr((case when regexp_like(a.phone1,'([^0-9])') then regexp_replace(a.phone1,'([^0-9])',null,1,0,'i') else a.phone1 end),1,10),'([[:cntrl:]]+)', null, 1,0,'i')  phone1
	,c.title
	,first_visit  
  ,dogs_over_4 
	from cooked.client c 
	inner join cooked.address a on a.client_id = c.client_id and a.address_type_id = 2  
	inner join (select distinct client_id,h_id,run_id  
				,max(dogs_over_6) over (partition by client_id,run_id) dogs_over_6 
				,sum(dogs_over_4) over (partition by client_id,run_id) dogs_over_4 
				,max(puppy) over (partition by client_id,run_id) puppy   
				,end_date  
				,count(distinct case when vi_Species_id = 3 then patient_id end) over(partition by client_id,run_id) canines  
				,count(distinct case when vi_Species_id = 7 then patient_id end) over(partition by client_id,run_id) felines  
				,min(first_active)  over(partition by client_id,run_id) first_visit
				from consumer_patients) p   
	on p.client_id = c.client_id   
	left join split_clients sc on sc.location_number = gv_input_location_number and sc.h_id = c.h_id and sc.pms_client_id = c.pms_id   
	where p.run_id = gv_input_run_id 
	and ((split_count > 0 and sc.pms_client_id is not null) or (split_count = 0 )) ;
 
else 
 
	insert into consumer_clients   
	select distinct c.client_id
	,nvl(regexp_replace(regexp_replace(c.first_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]|\^|\*|\)|\(|\:|\+)', null, 1,0,'i'),'None Given') first_name   
	,nvl(regexp_replace(regexp_replace(c.last_name,  '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]|\^|\*|\)|\(|\:|\+)', null, 1,0,'i'),'None Given') last_name
	,case when regexp_like(c.title,'([^[:alpha:]\ |\.]|\\)+','i') then null else
  regexp_replace(c.title,'([^[:alpha:]\ |\.]|\\)+', null, 1,0,'i') end prefix
	,nvl(regexp_replace(regexp_replace(a.address1, 'do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i'),'None Given') address1  
	,regexp_replace(regexp_replace(a.address2, 'do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i') address2 
	,regexp_replace(a.city,'([[:cntrl:]]+)', null, 1,0,'i') city
	,regexp_replace(a.state,'([[:cntrl:]]+)', null, 1,0,'i') state  
	,regexp_replace(lpad(substr((case when regexp_like(postal_code, '([^0-9])') then regexp_replace(postal_code, '([^0-9])', null, 1, 0, 'i') else postal_code end),1,9),30),'([[:cntrl:]]+)', null, 1,0,'i') postal_code   
	,c.h_id 
	,dogs_over_6  
	,puppy  
	,p.run_id 
	,p.end_Date   
	,p.canines
	,p.felines
	,c.primary_email  
	,regexp_replace(substr((case when regexp_like(a.phone1,'([^0-9])') then regexp_replace(a.phone1,'([^0-9])',null,1,0,'i') else a.phone1 end),1,10),'([[:cntrl:]]+)', null, 1,0,'i')  phone1
	,c.title
	,first_visit  
  ,dogs_over_4 
	from cooked.client c 
	inner join cooked.address a on a.client_id = c.client_id and a.address_type_id = 2  
	inner join (select distinct client_id,h_id,run_id  
				,max(dogs_over_6) over (partition by client_id,run_id) dogs_over_6 
				,sum(dogs_over_4) over (partition by client_id,run_id) dogs_over_4 
				,max(puppy) over (partition by client_id,run_id) puppy   
				,end_date  
				,count(distinct case when vi_Species_id = 3 then patient_id end) over(partition by client_id,run_id) canines  
				,count(distinct case when vi_Species_id = 7 then patient_id end) over(partition by client_id,run_id) felines  
				,min(first_active)  over(partition by client_id,run_id) first_visit
				from consumer_patients) p   
	on p.client_id = c.client_id   
	where p.run_id = gv_input_run_id 
;
 
end if;  
	
delete from consumer_clients
where run_id = gv_input_run_id  
and(UPPER(First_Name) in ('OTC','OVER THE COUNTER','NEW','NEW CLIENT','TEST','NONE GIVEN')
or upper(First_Name) like '%'||chr(42)||'%' 
or upper(First_Name) like '%CLINIC%'  
or upper(First_Name) like '%VETERINARY%'
or upper(First_Name) like '%HOSPITAL%'
or upper(First_Name) like '% VET %'   
or upper(First_Name) like '%ANIMAL%'  
or upper(First_Name) like '% VC %'
or upper(First_Name) like '% VH %'
or upper(First_Name) like '% AC %'
or upper(First_Name) like '% AH %'
or UPPER(Last_Name) in ('OTC','OVER THE COUNTER','CLIENT','NEW CLIENT','TEST CLIENT','NONE GIVEN')  
or upper(Last_Name) like '%'||chr(42)||'%'  
or upper(Last_Name) like '%CLINIC%'   
or upper(Last_Name) like '%VETERINARY%' 
or upper(Last_Name) like '%HOSPITAL%' 
or upper(Last_Name) like '% VET %'
or upper(Last_Name) like '%ANIMAL%'   
or upper(Last_Name) like '% VC %' 
or upper(Last_Name) like '% VH %' 
or upper(Last_Name) like '% AC %' 
or upper(Last_Name) like '% AH %' 
or UPPER(Address1) in ('OTC','OVER THE COUNTER','RETURNED',' ','NONE GIVEN') 
or upper(Address1) like '%'||chr(42)||'%'   
or UPPER(Address2) in ('OTC','OVER THE COUNTER',
'CREDIT' , 'GRANDFATHER', 'NO MAIL','ACCOUNT' , 'DAD','INACTIVE',
'PARENTS','ALLERGIC' ,'DAUGHTER' ,'INCORRECT' , 'REFUSE' ,'BAD' , 'DEBT','INSUFFIENT' , 'REMINDER', 
'BANKRUPTCY' ,'DISCONNECTED' ,'INVALID' , 'RETURNED', 'BILLING' , 'DISCOUNT' , 'INVENTORY' , 'SEND',
'BROTHER' , 'DON''T' , 'INVOICE' , 'SISTER', 'CASH', 'EMERGENCY' , 'LAND LINE' , 'TEMPORARY' , 'CELL' , 'EMPLOYEE' , 'MAIL' , 'UNABLE' , 'CHANGE' , 'FATHER' , 'MESSAGE' ,
'UNDELIVERABLE' , 'CHECK' , 'FAX' , 'MOM' , 'UPDATE' , 'COLLECTIONS' , 'FILE' , 'MOTHER' , 'VERIFY' ,   
'CONFIRM' , 'FORWARD' , 'NEED', 'WIFE' , 'HUSBAND' , 'DISCARDED')
or upper(Address2) like '%'||chr(33)||'%'   
or upper(Address2) like '%'||chr(42)||'%'   
or (UPPER(First_Name) = 'CASH' and UPPER(Last_Name) in ('CLIENT','CASH')))   
;
 
 
 
delete from consumer_clients where run_id = gv_input_run_id
and (upper(state) like 'AB%'
or upper(state) like 'BC%'  
or upper(state) like 'MB%'  
or upper(state) like 'NB%'  
or upper(state) like 'NL%'  
or upper(state) like 'NT%'  
or upper(state) like 'NS%'  
or upper(state) like 'NU%'  
or upper(state) like 'ON%'  
or upper(state) like 'PE%'  
OR upper(state) LIKE 'QC%'  
or upper(state) like 'SK%'  
or upper(state) like 'YT%'  
or upper(state) like 'PR%'  
);   
 
build_process_log('INFO','Finished running CLIENTS for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER'); 
 
 
-- get all FTH purchaes for all FTH products
-- get fth purchaes for the past 2 years  
-- if quantity is null or cost is null set quantity = 1
-- if quantity is null or cost is null set cost = 0 
-- ignore purchases where the quantity = 0
-- delete certifect data before it was launched 
insert into consumer_fth_purchases  
select l.LINEITEM_ID, 
l.CLIENT_ID,   
l.PATIENT_ID,  
l.SERVICE_MEDICATION_ID,
l.H_ID,  
l.REGION_ID,   
l.VI_SPECIES_ID,   
l.INVOICE_DATE,
l.QUANTITY,
l.COST,  
case when l.quantity_norm is null or l.cost_norm is null then 1 else l.quantity_norm end quantity_norm, 
l.NORM_RULE_ID,
l.UPDATE_RULE_ID,  
case when l.quantity_norm is null or l.cost_norm is null then 0 else l.cost_norm end cost_norm,   
l.ORIG_QUANTITY,   
l.ORIG_COST,   
l.DATE_NORMALIZED, 
l.DESCR_PACKAGE,   
l.PRICING_OVERRIDE,
l.PRODUCT, 
l.VOIDED,
pf.type, 
pf.family_name,
c.run_id,
c.end_date,
''   
from consumer_patients c  
inner join normalization.lineitem_norm_fth4 l on c.patient_id = l.patient_id and l.h_id = c.h_id  
inner join reports.product_families pf on pf.product = l.product 
where trunc(invoice_date,'mm') between add_months(trunc(to_Date(c.end_date),'mm'),-23) and trunc(to_Date(c.end_date),'mm') 
and family_name in ('Activyl','Acuguard','Adams','Advantage','Advantage Multi','Advantix','Assurity','Capstar','Certifect','ComboGuard','Comfortis','EasySpot','Frontline','Heartgard','Interceptor' 
,'Iverhart Max','Iverhart Plus','Paradyne','Parastar','Preventic','Program','Proheart','ProMeris','QuadriGuard','Revolution','Sentinel','Seresto','SimpleGuard','Trifexis','Tri-Heart','Tritak','Vectra','Vectra 3D' 
,'NexGard','Bravecto')
and quantity_norm <> 0
and voided in ('0','F','f') 
and c.run_id = gv_input_run_id  
;
 
delete from consumer_fth_purchases where run_id = gv_input_run_id and (invoice_Date >= trunc(to_Date(gv_input_end_Date)) or invoice_Date < add_months(trunc(to_Date(gv_input_end_Date)),-24) );
delete from consumer_fth_purchases where run_id = gv_input_run_id and product = 'Certifect' and trunc(invoice_Date,'mm') < '01-Jul-2011' and  invoice_Date > gv_input_end_date;   
 
build_process_log('INFO','Finished running FTH_PURCHASES for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER'); 
 
insert into consumer_all_trans2 
select lineitem_id,invoice_Date,service_id,l.client_id, l.h_id , gv_input_run_id run_id, cost, patient_id,voided,gv_input_end_date 
from cooked.lineitem l
where trunc(invoice_date,'mm') between add_months(gv_input_end_Date,-24) and gv_input_end_Date
and l.h_id = gv_input_hid   
and voided in ('0','F','f');
 
delete from consumer_all_trans2 where run_id = gv_input_run_id and (invoice_date >= trunc(to_Date(gv_input_end_Date))-1 or invoice_date < add_months(trunc(to_Date(gv_input_end_Date)),-24) ); 
 
 
build_process_log('INFO','Finished running ALL_TRANS for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER'); 
 
------------------------------------------------------------------------------------------------------------- 
--- NON-INITIAL PULLS --------------------------------------------------------------------------------------- 
------------------------------------------------------------------------------------------------------------- 
 
insert into consumer_patient_counts 
select c.run_id
,c.h_id  
,c.end_date
,count(distinct case when c.vi_species_id = 3 then decode(type,'ft',c.patient_id,'f',c.patient_id) end) ft_canine_patients 
,count(distinct case when c.vi_species_id = 7 then decode(type,'ft',c.patient_id,'f',c.patient_id) end) ft_feline_patients 
,count(distinct case when c.vi_species_id = 3 then decode(type,'h',c.patient_id) end) hw_canine_patients
from consumer_fth_purchases p   
right join consumer_patients c on c.patient_id= p.patient_id and c.run_id = p.run_id and trunc(invoice_date) between add_months(gv_input_end_Date,-12) and gv_input_end_Date  
where c.run_id = gv_input_run_id
group by c.run_id  
,c.h_id  
,c.end_date
;
 
 
-- HEARTGARD ----------------------------------------------  
insert into consumer_hg
select distinct
c.client_id
,p.run_id
,p.end_date
,max(invoice_Date) over(partition by c.client_id) last_hg_purch  
,count(case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then invoice_date end) over(partition by c.client_id) hg_purch_12 
,count(case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then invoice_date end) over(partition by c.client_id) hg_purch_24 
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) hg_rev_12 
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) hg_rev_24
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) hg_doses_12
,sum(  case when invoice_date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) hg_doses_24
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date,lineitem_id rows between unbounded preceding and unbounded following) hg_last_pet_species
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id = p.patient_id and c.run_id = p.run_id
where c.run_id = gv_input_run_id
and product like 'Heartgard%' ; 
-- FRONTLINE ----------------------------------------------  
insert into consumer_fl
select distinct
c.client_id
,p.run_id
,p.end_date
,max(invoice_Date) over(partition by c.client_id) last_fl_purch  
,count(case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then invoice_date end) over(partition by c.client_id) fl_purch_12 
,count(case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then invoice_date end) over(partition by c.client_id) fl_purch_24 
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) fl_rev_12 
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) fl_rev_24
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) fl_doses_12
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) fl_doses_24
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date,lineitem_id rows between unbounded preceding and unbounded following) fl_last_pet_species
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id = p.patient_id and c.run_id = p.run_id
where c.run_id = gv_input_run_id
and product like 'Frontline%' and family_name <> 'Tritak' ;  
-- CERTIFECT ----------------------------------------------  
insert into consumer_ce
select distinct
c.client_id
,p.run_id
,p.end_date
,max(invoice_Date) over(partition by c.client_id) last_ce_purch  
,count(case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then invoice_date end) over(partition by c.client_id) ce_purch_12 
,count(case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then invoice_date end) over(partition by c.client_id) ce_purch_24 
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) ce_rev_12 
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) ce_rev_24
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) ce_doses_12
,sum(  case when invoice_date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) ce_doses_24
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date,lineitem_id rows between unbounded preceding and unbounded following) ce_last_pet_species
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id = p.patient_id and c.run_id = p.run_id
where c.run_id = gv_input_run_id
and product like 'Certifect%';  
-- COMBO --------------------------------------------------  
insert into consumer_combo 
select distinct
c.client_id
,p.run_id
,p.end_date
,max(invoice_Date) over(partition by c.client_id) last_combo_purch 
,count(case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then invoice_date end) over(partition by c.client_id) combo_purch_12  
,count(case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then invoice_date end) over(partition by c.client_id) combo_purch_24
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) combo_rev_12 
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) combo_rev_24   
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) combo_doses_12 
,sum(  case when invoice_date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) combo_doses_24 
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date,lineitem_id rows between unbounded preceding and unbounded following) combo_last_pet_species 
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id = p.patient_id and c.run_id = p.run_id
where c.run_id = gv_input_run_id
and type in ('fh','fth') ;  
-- COMP HG ------------------------------------------------  
insert into consumer_comp_hg 
select distinct
c.client_id
,p.run_id
,p.end_date
,max(invoice_Date) over(partition by c.client_id) comp_last_hg_purch   
,count(case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then invoice_date end) over(partition by c.client_id) comp_hg_purch_12
,count(case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then invoice_date end) over(partition by c.client_id) comp_hg_purch_24  
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) comp_hg_rev_12 
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) comp_hg_rev_24 
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) comp_hg_doses_12   
,sum(  case when invoice_date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) comp_hg_doses_24 
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date,lineitem_id rows between unbounded preceding and unbounded following) last_comp_hg_pet_species 
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id = p.patient_id and c.run_id = p.run_id
where c.run_id = gv_input_run_id
and p.product not like 'Heartgard%' and type in ('h'); 
-- COMP FL ------------------------------------------------  
insert into  consumer_comp_fl
select distinct
c.client_id
,p.run_id
,p.end_date
,max(invoice_Date) over(partition by c.client_id) comp_last_fl_purch   
,count(case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then invoice_date end) over(partition by c.client_id) comp_fl_purch_12
,count(case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then invoice_date end) over(partition by c.client_id) comp_fl_purch_24  
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) comp_fl_rev_12 
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) comp_fl_rev_24 
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) comp_fl_doses_12   
,sum(  case when invoice_date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) comp_fl_doses_24 
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date,lineitem_id rows between unbounded preceding and unbounded following) last_comp_fl_pet_species 
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id = p.patient_id and c.run_id = p.run_id
where c.run_id = gv_input_run_id
and p.product not like 'Frontline%' and p.product not like 'Certifect%' and type in ('ft','f');   
-- INTERCEPTOR --------------------------------------------  
insert into  consumer_fth_other  
select distinct
c.client_id
,p.run_id
,p.end_date
,sum(case when product like 'Interceptor%' and invoice_Date between add_months(p.end_date ,-12) and p.end_date then quantity_norm else 0 end) over(partition by c.client_id) int_doses_12  
,sum(case when product like 'Interceptor%' and invoice_Date between add_months(p.end_date ,-24) and add_months(p.end_date ,-12)-1 then quantity_norm else 0 end) over(partition by c.client_id) int_doses_24   
-- DIFF ---------------------------------------------------  
,sum(case when p.product = 'Trifexis' and invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then quantity_norm  else 0 end) over(partition by c.client_id) trif_doses 
,sum(case when p.product = 'Revolution' and invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then quantity_norm  else 0 end) over(partition by c.client_id) rev_doses
,sum(case when p.product = 'Advantage Multi' and invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then quantity_norm  else 0 end) over(partition by c.client_id) adv_doses 
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id = p.patient_id and c.run_id = p.run_id
where c.run_id = gv_input_run_id
and (product like 'Interceptor%' or p.product = 'Trifexis' or p.product = 'Revolution' or p.product = 'Advantage Multi') 
;
-- TRITAK -------------------------------------------------  
insert into  consumer_flt  
select distinct
c.client_id
,p.run_id
,p.end_date
,max(invoice_Date) over(partition by c.client_id) last_flt_purch 
,count(case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then invoice_date end) over(partition by c.client_id) flt_purch_12
,count(case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then invoice_date end) over(partition by c.client_id) flt_purch_24
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) flt_rev_12
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) flt_rev_24 
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) flt_doses_12   
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) flt_doses_24   
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date,lineitem_id rows between unbounded preceding and unbounded following) flt_last_pet_species   
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id = p.patient_id and c.run_id = p.run_id
where c.run_id = gv_input_run_id
and family_name = 'Tritak'; 
 
-- NEXGARD ------------------------------------------------- 
insert into  consumer_NEXGARD
select distinct
c.client_id
,p.run_id
,p.end_date
,max(invoice_Date) over(partition by c.client_id) last_ng_purch  
,count(case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then invoice_date end) over(partition by c.client_id) ng_purch_12 
,count(case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then invoice_date end) over(partition by c.client_id) ng_purch_24 
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) ng_rev_12 
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) ng_rev_24
,sum(  case when invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) ng_doses_12
,sum(  case when invoice_Date between add_months(p.end_date,-24) and add_months(p.end_date,-12)-1 then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) ng_doses_24
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date,lineitem_id rows between unbounded preceding and unbounded following) ng_last_pet_species
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id = p.patient_id and c.run_id = p.run_id
where c.run_id = gv_input_run_id
and family_name = 'NexGard';
 
 
build_process_log('INFO','Finished running LAST for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER');
 
 
insert into consumer_comp_fl_prod   
select rank() over(partition by client_id order by doses,LAST_DATE,last_id) product_order 
,product 
,client_id 
,run_id  
,end_date
from (   
select distinct
sum(decode(quantity_norm,null,1,0,1,quantity_norm)) over(partition by p.product,c.client_id) doses  
,c.client_id   
,max(invoice_Date) over(partition by p.product,c.client_id) LAST_DATE  
,max(lineitem_id) over(partition by p.product,c.client_id) LAST_id 
,pf.family_name product 
,p.run_id  
,p.end_date
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id= p.patient_id and c.run_id = p.run_id 
inner join reports.product_families pf on pf.product = p.product   
where p.product not like 'Frontline%' and p.product not like 'Certifect%' and pf.type in ('ft','f') 
and quantity_norm <> 0  
and c.vi_species_id in (3,7)
and invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) 
and c.run_id = gv_input_run_id
);   
 
build_process_log('INFO','Finished running COMP_FL_PROD for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER');  
 
insert into consumer_comp_hg_prod   
select rank() over(partition by client_id order by doses,LAST_DATE,last_id) product_order 
,product 
,client_id 
,run_id  
,end_date
from (   
select distinct
sum(decode(quantity_norm,null,1,0,1,quantity_norm)) over(partition by p.product,c.client_id) doses  
,c.client_id   
,max(invoice_Date) over(partition by p.product,c.client_id) LAST_DATE  
,max(lineitem_id) over(partition by p.product,c.client_id) LAST_id 
,pf.family_name product 
,p.run_id  
,p.end_date
from consumer_fth_purchases p   
inner join consumer_patients c on c.patient_id= p.patient_id and c.run_id= p.run_id  
inner join reports.product_families pf on pf.product = p.product   
where p.product not like 'Heartgard%' and pf.type in ('h')   
and quantity_norm <> 0  
and invoice_Date between add_months(p.end_date,-12) and add_months(p.end_date,0) 
and c.vi_species_id in (3,7)
and c.run_id = gv_input_run_id
);   
 
---------------------------------------------------------------------------------------------------------------------------------------  
---- ANCILLIARY DATA PULLS ------------------------------------------------------------------------------------------------------------  
---------------------------------------------------------------------------------------------------------------------------------------  
 
 
insert into consumer_pm
select l.h_id , c.run_id,c.client_id  
, count(case when invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0) then lineitem_id else null end) num_pain_purch 
, max(invoice_Date) last_pain_purch   
, sum(case when invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0)  then cost else 0 end) dollar_pain_purch
from consumer_all_trans2 l
inner join consumer_patients c on c.patient_id = l.patient_id and l.h_id = c.h_id and l.run_id = c.run_id   
inner join cooked.service_medication4 sm on sm.service_medication_id = l.service_id and sm.service_type_id = 2
where c.run_id = gv_input_run_id
and invoice_Date between add_months(to_date(c.end_date),-24) and to_date(c.end_date)-1  
and l.voided in('0','F','f')
and cost <> 0  
group by l.h_id , c.run_id,c.client_id
;
 
build_process_log('INFO','Finished pm for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONSUMER');  
 
insert into consumer_dental 
select l.h_id , c.run_id,c.client_id  
, count(case when invoice_Date between  add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0) then lineitem_id else null end) num_dental_purch  
, max(invoice_Date) last_dental_purch 
, sum(case when invoice_Date between  add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0) then cost else 0 end) dollar_dental_purch 
 from consumer_all_trans2 l 
inner join consumer_patients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_general sg on sg.service_general_id = l.service_id and product_info = 'Surgery - dental'
where cost <> 0
and invoice_Date between add_months(to_date(c.end_date),-24) and to_date(c.end_date)-1  
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,c.client_id;   
 
build_process_log('INFO','Finished DENTAL for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONSUMER'); 
 
 
insert into consumer_surg 
select l.h_id , c.run_id,c.client_id  
, count(case when invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0) then lineitem_id else null end) num_surg_purch 
, max(invoice_Date) last_surg_purch   
, sum(case when invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0) then cost else 0 end) dollar_surg_purch 
 from consumer_all_trans2 l 
inner join consumer_patients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_general sg on sg.service_general_id = l.service_id and product_info like 'Surgery%' and product_info <> 'Surgery - dental'   
where cost <> 0
and invoice_Date between add_months(to_date(c.end_date),-24) and to_date(c.end_date)-1  
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,c.client_id;   
 
build_process_log('INFO','Finished surg for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONSUMER');
 
insert into consumer_vacc 
select l.h_id , c.run_id,c.client_id  
, count(case when invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0)  then lineitem_id else null end) num_vacc_purch
, max(invoice_Date) last_vacc_purch   
, sum(case when invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0)  then cost else 0 end) dollar_vacc_purch
 from consumer_all_trans2 l 
inner join consumer_patients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_vaccine2 sg on sg.service_vaccine_id = l.service_id 
where cost <> 0
and invoice_Date between add_months(to_date(c.end_date),-24) and to_date(c.end_date)-1  
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,c.client_id;   
 
build_process_log('INFO','Finished vacc for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONSUMER');
 
 
insert into consumer_board
select l.h_id , c.run_id,c.client_id  
, count(case when invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0)  then lineitem_id else null end) num_board_purch   
, max(invoice_Date) last_board_purch  
, sum(case when invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0)  then cost else 0 end) dollar_board_purch  
 from consumer_all_trans2 l 
inner join consumer_patients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_general sg on sg.service_general_id = l.service_id and  lower(PRODUCT_INFO) in('service - boarding') 
where cost <> 0
and invoice_Date between add_months(to_date(c.end_date),-24) and to_date(c.end_date)-1  
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,c.client_id;   
 
build_process_log('INFO','Finished BOARDING for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONSUMER'); 
 
insert into consumer_groom
select l.h_id , c.run_id,c.client_id  
, count(case when invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0)  then lineitem_id else null end) num_groom_purch   
, max(invoice_Date) last_groom_purch  
, sum(case when invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0)  then cost else 0 end) dollar_groom_purch  
 from consumer_all_trans2 l 
inner join consumer_patients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_general sg on sg.service_general_id = l.service_id and  lower(PRODUCT_INFO) in('service - grooming') 
where cost <> 0
and invoice_Date between add_months(to_date(c.end_date),-24) and to_date(c.end_date)-1  
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,c.client_id;   
 
build_process_log('INFO','Finished GROOMING for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONSUMER'); 
 
 
insert into consumer_other
select l.h_id , c.run_id,c.client_id  
, count(case when l.invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0)  then l.lineitem_id else null end) num_other_purch 
, max(l.invoice_Date) last_other_purch
, sum(case when l.invoice_Date between add_months(to_date(c.end_date),-14) and add_months(to_date(c.end_date),0)  then l.cost else 0 end) dollar_other_purch
 from consumer_all_trans2 l 
inner join consumer_patients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
left join cooked.service_general sg on sg.service_general_id = l.service_id and (sg.product_info like 'Surgery%' or lower(sg.product_info) like '%dental%' or lower(sg.PRODUCT_INFO) in( 'service - boarding','service - grooming'))  
--left join cooked.service_diet sd on sd.service_diet_id = l.service_id
left join consumer_fth_purchases ft on ft.lineitem_id = l.lineitem_id
left join normalization.lineitem_norm_nsd4 n on n.lineitem_id = l.lineitem_id
left join cooked.service_vaccine2 sv on sv.service_vaccine_id = l.service_id
where l.cost <> 0  
and l.invoice_Date between add_months(to_date(c.end_date),-24) and to_date(c.end_date)-1
and sg.service_general_id is null 
--and sd.service_diet_id is null
and ft.lineitem_id is null  
and n.lineitem_id is null   
and sv.service_vaccine_id is null 
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,c.client_id;   
 
 
 
insert into consumer_last_visit 
select c.h_id , c.run_id,c.client_id  
, count(distinct case when invoice_Date between add_months(to_date(c.end_date),-12) and to_date(c.end_date)-1 then invoice_Date end) num_visits
, max(invoice_Date) last_visit  
 from consumer_all_trans2 l 
inner join consumer_patients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
where invoice_Date between add_months(to_date(c.end_date),-24) and to_date(c.end_date)-1
and l.run_id = gv_input_run_id  
group by c.h_id , c.run_id,c.client_id;   
 
 
------------------------------------------------------------------------------------------------------------------------------ 
-- PREVICOX PURCHASES -------------------------------------------------------------------------------------------------------- 
------------------------------------------------------------------------------------------------------------------------------ 
 
 
--create table consumer_nsaid as 
insert into consumer_nsaid 
select distinct run_id  
,lineitem_id   
,patient_id
,client_id 
,h_id
,invoice_Date  
,product 
,quantity_norm 
,dosage_volume_value_norm   
,cost_norm 
,pills   
,appointment_flag  
 from  ( 
select   
a.run_id 
,a.lineitem_id 
,a.patient_id  
,a.client_id   
,a.h_id  
,invoice_Date  
,product 
,quantity_norm 
,dosage_volume_value_norm   
,cost_norm 
,pills   
,appointment_flag  
--,b.*   
,case when b.patient_id is not null and pills < 10 then 'exclude' end include_exclude
from 
(
select distinct run_id, n.lineitem_id,n.patient_id,n.client_id,n.h_id,n.invoice_Date,n.product,quantity_norm,dosage_volume_value_norm, cost_norm, 
quantity_norm/dosage_volume_value_norm pills ,  
case when a.patient_id is null then 'N' else 'Y' end appointment_flag  
from consumer_patients c  
inner join normalization.lineitem_norm_nsd4 n on c.patient_id = n.patient_id and n.h_id = c.h_id  
inner join cooked.service_medication4 sm on sm.service_medication_id = n.service_id  
left join (select distinct hid,patient_id,trunc(appointment_date) appointment_date from vetstreet.pgsql_appointment) a on a.hid = n.h_id and a.patient_id = n.patient_id and a.appointment_date = n.invoice_Date 
where sm.product = 'Previcox'   
--and trunc(invoice_Date,'mm') = '01-nov-2014'  
and invoice_Date between add_months(c.end_date,-12) and add_months(c.end_date,0) 
and dosage_volume_value_norm <> 0 
and dosage_volume_value_norm is not null  
and voided in ('0','F','f') 
and quantity_norm <> 0
and run_id = gv_input_run_id
--and h_id in (7033,6198,10815,11175,9880,9274,8061,9602,6884,8483,10746,524,618,1235,1669,2238,2305,2342,2562,2559,2385,2431) 
) a  
left join
(
select distinct c.run_id, l.h_id,l.patient_id--,l.cost,sg.family,l.invoice_Date  
, trunc(l.invoice_Date)-5 surgery_start, trunc(l.invoice_Date)+5 surgery_end   
from cooked.service_general sg  
inner join consumer_all_trans2 l on l.service_id = service_general_id
inner join consumer_patients c on c.patient_id = l.patient_id and l.h_id = c.h_id and l.run_id = c.run_id   
where sg.family in ( 'Surgery - general', 
'Surgery - internal', 
'Surgery - orthopedic', 
'Surgery - unknown',  
'Surgery - ocular & cosmetic/elective',   
'Surgery - reproductive',   
'Surgery - dental')
--and trunc(invoice_Date,'mm') = '01-nov-2014'  
and voided in ('0','F','f') 
and cost > 0   
and c.run_id = gv_input_run_id  
--and l.h_id in (7033,6198,10815,11175,9880,9274,8061,9602,6884,8483,10746,524,618,1235,1669,2238,2305,2342,2562,2559,2385,2431)   
) b  
on a.patient_id = b.patient_id and a.invoice_date between b.surgery_start and b.surgery_end and a.run_id = b.run_id   
) c where 1=1  
and include_exclude is null 
order by 4,5;  
 
delete from consumer_nsaid_purchases where run_id = gv_input_run_id and (invoice_Date >= (select distinct end_date from process_log where run_id = gv_input_Run_id and flag = 'RUNID') or invoice_Date < (select distinct start_date from process_log where run_id = gv_input_Run_id and flag = 'RUNID'));  
 
 
 
insert into  consumer_prev 
--create table consumer_prev as  
select distinct
c.client_id
,c.run_id
,c.end_date
,max(invoice_Date) over(partition by c.client_id) last_prev_purch
,count(case when invoice_Date between add_months(c.end_date,-12) and add_months(c.end_date,0) then invoice_date end) over(partition by c.client_id) prev_purch_12   
,count(case when invoice_Date between add_months(c.end_date,-24) and add_months(c.end_date,-12)-1 then invoice_date end) over(partition by c.client_id) prev_purch_24 
,sum(  case when invoice_Date between add_months(c.end_date,-12) and add_months(c.end_date,0) then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) prev_rev_12  
,sum(  case when invoice_Date between add_months(c.end_date,-24) and add_months(c.end_date,-12)-1 then decode(quantity_norm,null,0,cost_norm) end) over(partition by c.client_id) prev_rev_24
,sum(  case when invoice_Date between add_months(c.end_date,-12) and add_months(c.end_date,0) then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) prev_doses_12  
,sum(  case when invoice_Date between add_months(c.end_date,-24) and add_months(c.end_date,-12)-1 then decode(quantity_norm,null,1,quantity_norm) end) over(partition by c.client_id) prev_doses_24  
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date,lineitem_id rows between unbounded preceding and unbounded following) prev_last_pet_species  
,count(distinct case when invoice_Date between add_months(c.end_date,-12) and add_months(c.end_date,0) then c.patient_id end) over(partition by c.client_id) prev_pats_12 
,sum(  case when invoice_Date between add_months(c.end_date,-12) and add_months(c.end_date,0) then decode(pills,null,1,pills) end) over(partition by c.client_id) prev_pills_12 
,sum(  case when invoice_Date between add_months(c.end_date,-24) and add_months(c.end_date,-12)-1 then decode(pills,null,1,pills) end) over(partition by c.client_id) prev_pills_24 
from consumer_nsaid p  
inner join consumer_patients c on c.patient_id = p.patient_id and c.run_id = p.run_id
where c.run_id = gv_input_run_id
;
 
---------------------------------------------------------------------------------------------------------------------------------------  
------ FINAL REPORT -------------------------------------------------------------------------------------------------------------------  
---------------------------------------------------------------------------------------------------------------------------------------  
 
 
 
build_process_log('INFO','Finished running OTHER for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER');   
 
insert into consumer_report 
select distinct
'1DT' record_type  
,cast(c.first_name as varchar2(30)) first_name  
,cast(c.last_name as varchar2(30)) last_name
,CAST(trim(lpad(c.prefix,12)) AS VARCHAR2(30)) prefix  
,cast(address1 as varchar2(40)) address1 --5
,cast(address2 as varchar2(40)) address2  
,cast(city as varchar2(30)) city
,cast(state as varchar2(2)) state 
,trim(CAST(lpad(substr((CASE WHEN REGEXP_LIKE(postal_code, '([^0-9])') THEN regexp_replace(postal_code, '([^0-9])', NULL, 1, 0, 'i') ELSE postal_code END),1,9),30) AS VARCHAR2(40))) postal_code
,cast(replace(replace(replace(replace(phone1,'(',''),')',''),' ',''),'-','') as varchar2(40)) phone --10
,cast( to_char(last_hg_purch,'mm/dd/yyyy') as varchar2(40)) last_hg_purch  
,cast(to_char(last_ce_purch,'mm/dd/yyyy') as varchar2(30)) last_ce_purch   
,to_char(last_fl_purch,'mm/dd/yyyy') last_fl_purch  
,to_char(comp_last_hg_purch,'mm/dd/yyyy') comp_last_hg_purch 
,to_char(comp_last_fl_purch,'mm/dd/yyyy') comp_last_fl_purch 
,case when regexp_count(primary_email, '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$',1,'i')>0 then primary_email end primary_email
,c.client_id   
,canines canine_count 
,felines feline_count 
,decode(fl_last_pet_species,3,'Canine',7,'Feline') fl_pet_species
,decode(hg_last_pet_species,3,'Canine',7,'Feline') hg_pet_species
,decode(last_comp_fl_pet_species,3,'Canine',7,'Feline') comp_fl_pet_species
,decode(last_comp_hg_pet_species,3,'Canine',7,'Feline') comp_hg_pet_species
,nvl(ce_purch_12,0) num_cert_purch_12 
,nvl(fl_purch_12,0) num_fl_purch_12   
,nvl(fl_purch_24,0) num_fl_purch_24 --26  
,nvl(comp_fl_purch_12,0) num_comp_fl_cert_purch_12  
,nvl(comp_fl_purch_24,0) num_comp_fl_cert_purch_24 --28
,trim(nvl(to_char(ce_rev_12,'999990.00'),'0.00')) dollar_cert_purch_12 
,trim(nvl(to_char(fl_rev_12,'999990.00'),'0.00')) dollar_fl_purch_12   
,trim(nvl(to_char(fl_rev_24,'999990.00'),'0.00')) dollar_fl_purch_24   
,trim(nvl(to_char(comp_fl_rev_12,'999990.00'),'0.00')) comp_fl_cert_rev_12 
,trim(nvl(to_char(comp_fl_rev_24,'999990.00'),'0.00')) comp_fl_cert_rev_24 --33  
,nvl(hg_purch_12,0) num_hg_purch_12   
,nvl(hg_purch_24,0) num_hg_purch_24 --35  
,nvl(comp_hg_purch_12,0) num_comp_hg_purch_12   
,nvl(comp_hg_purch_24,0) num_comp_hg_purch_24 --37  
,trim(nvl(to_char(hg_rev_12,'999990.00'),'0.00')) dollar_hg_purch_12   
,trim(nvl(to_char(hg_rev_24,'999990.00'),'0.00')) dollar_hg_purch_24   
,trim(nvl(to_char(comp_hg_rev_12,'999990.00'),'0.00')) dollar_comp_hg_12   
,trim(nvl(to_char(comp_hg_rev_24,'999990.00'),'0.00')) dollar_comp_hg_24 --41
,to_char(first_visit,'mm/dd/yyyy') first_visit  
,to_char(last_vacc_purch,'mm/dd/yyyy') last_vac_purch  
,to_char(pm.last_pain_purch,'mm/dd/yyyy')  last_pain_purch   
,to_char(de.last_dental_purch,'mm/dd/yyyy') last_dental_purch
,cast(null as varchar2(1)) last_emergency_purch 
,to_char(last_board_purch,'mm/dd/yyyy') last_boarding_purch  
,to_char(last_groom_purch,'mm/dd/yyyy') last_grooming_purch  
,to_char(s.last_surg_purch,'mm/dd/yyyy') last_surg_purch 
,cast(null as varchar2(1)) last_pet_care_purch  
,cast(null as varchar2(1)) last_pet_supply_purch
,cast(null as varchar2(1)) last_rx_purch  
,cast(null as varchar2(1)) last_lab_purch 
,to_char(last_other_purch,'mm/dd/yyyy') last_unk_purch 
,nvl(num_vacc_purch,0) num_vacc_purch --55
,nvl(pm.num_pain_purch,0) num_pain_purch  
,nvl(de.num_dental_purch,0) num_dental_purch
,cast(0 as varchar2(1)) num_emergency_purch 
,nvl(num_board_purch,0) num_boarding_purch
,nvl(num_groom_purch,0) num_grooming_purch --60 
,nvl(s.num_surg_purch,0) num_surg_purch   
,cast(0 as varchar2(1)) num_pet_care_purch
,cast(0 as varchar2(1)) num_pet_supply_purch
,cast(0 as varchar2(1)) num_rx_purch  
,cast(0 as varchar2(1)) num_lab_purch --65
,nvl(o.num_other_purch,0) num_unk_purch   
,case when ce_doses_12 < 0 then 0 else nvl(ce_doses_12,0) end ce_doses_12  
,case when fl_doses_12 < 0 then 0 else nvl(fl_doses_12,0) end fl_doses_12  
,case when fl_doses_24 < 0 then 0 else nvl(fl_doses_24,0) end fl_doses_24  
,case when comp_fl_doses_12 < 0 then 0 else nvl(comp_fl_doses_12,0) end num_comp_fl_cert_doses_12 --70  
,fp.product top_fl_cert_comp_prod 
,case when comp_fl_doses_24 < 0 then 0 else nvl(comp_fl_doses_24,0) end num_comp_fl_cert_doses_24 
,case when hg_doses_12 < 0 then 0 else nvl(hg_doses_12,0) end hg_doses_12  
,case when hg_doses_24 < 0 then 0 else nvl(hg_doses_24,0) end hg_doses_24  
,case when comp_hg_doses_12 < 0 then 0 else nvl(comp_hg_doses_12,0) end comp_hg_doses_12 --75 
,hp.product top_hg_comp_prod
,case when comp_hg_doses_24 < 0 then 0 else nvl(comp_hg_doses_24,0) end comp_hg_doses_24 --77 
,trim(nvl(to_char(dollar_vacc_purch,'999990.00'),'0.00')) dollar_vacc_purch
,trim(nvl(to_char(pm.dollar_pain_purch,'999990.00'),'0.00')) dollar_pain_purch   
,trim(nvl(to_char(de.dollar_dental_purch,'999990.00'),'0.00')) dollar_dental_purch --80 
,'0.00' dollar_es_purch 
,trim(nvl(to_char(dollar_board_purch,'999990.00'),'0.00')) dollar_boarding_purch 
,trim(nvl(to_char(dollar_groom_purch,'999990.00'),'0.00')) dollar_grooming_purch 
,trim(nvl(to_char(s.dollar_surg_purch,'999990.00'),'0.00')) dollar_surg_purch
,'0.00' dollar_pc_purch 
,'0.00' dollar_ps_purch 
,'0.00' dollar_pmed_purch   
,'0.00' dollar_lab_purch
,trim(nvl(to_char(dollar_other_purch,'999990.00'),'0.00')) dollar_unk_purch
,dogs_over_6 --90  
,to_char(last_combo_purch,'mm/dd/yyyy') last_combo_purch 
,nvl(combo_purch_12,0) num_combo_purch_12 
,nvl(combo_purch_24,0) num_combo_purch_24 
,trim(nvl(to_char(combo_rev_12,'999990.00'),'0.00')) dollar_combo_purch_12 
,trim(nvl(to_char(combo_rev_24,'999990.00'),'0.00')) dollar_combo_purch_24 --95  
,case when combo_doses_12 < 0 then 0 else nvl(combo_doses_12,0) end combo_doses_12   
,case when combo_doses_24 < 0 then 0 else nvl(combo_doses_24,0) end combo_doses_24   
,case when trif_doses < 0 then 0 else nvl(trif_doses,0) end trifexis_doses_12
,case when rev_doses < 0 then 0 else nvl(rev_doses,0) end revolution_doses_12
,case when adv_doses < 0 then 0 else nvl(adv_doses,0) end adv_multi_doses_12 --100   
,case when int_doses_12 < 0 then 0 else nvl(int_doses_12,0) end int_doses_12 
,case when int_doses_24 < 0 then 0 else nvl(int_doses_24,0) end int_doses_24 
,puppy --103   
 
,to_char(lv.last_visit,'mm/dd/yyyy') last_visit 
,nvl(lv.num_visits,0) num_visits --105
,to_char(last_flt_purch,'mm/dd/yyyy') last_flt_purch
,nvl(flt_purch_12,0) flt_purch_12 
,case when flt_doses_12 < 0 then 0 else nvl(flt.flt_doses_12,0) end flt_doses_12 
,trim(nvl(to_char(flt_rev_12,'999990.00'),'0.00')) flt_rev_12
,nvl(ce.ce_purch_24,0) ce_purch_24 --110  
 
,nvl(flt_purch_24,0) flt_purch_24 
,trim(nvl(to_char(flt_rev_24,'999990.00'),'0.00')) flt_rev_24
,case when flt_doses_24 <0 then 0 else nvl(flt_doses_24,0) end flt_doses_24
,to_char(last_ng_purch,'mm/dd/yyyy') last_ng_purch  
,nvl(ng_purch_12,0) ng_purch_12 --115 
,trim(nvl(to_char(ng_rev_12,'999990.00'),'0.00')) ng_rev_12  
,case when ng_doses_12 < 0 then 0 else nvl(ng_doses_12,0) end ng_doses_12  
,nvl(dogs_over_4,0) dogs_over_4 
 
,to_char(last_prev_purch,'mm/dd/yyyy') last_prev_purch 
,nvl(prev_purch_12,0) prev_purch_12 --120 
,trim(nvl(to_char(prev_rev_12,'999990.00'),'0.00')) prev_rev_12  
,trim(nvl(to_char(prev_pills_12,'999990.0'),'0.0')) prev_pills_12
--,nvl(prev_pills_12,0) prev_pills_12 
,nvl(prev_pats_12,0) prev_pats_12 --123   
 
,c.h_id  
,c.run_id run_id   
,c.end_date end_date --126  
from consumer_clients  c  
left join consumer_last_visit lv on lv.client_id = c.client_id and lv.run_id = c.run_id  
left join consumer_hg hg on hg.client_id = c.client_id and hg.run_id = c.run_id 
left join consumer_fl fl on fl.client_id = c.client_id and fl.run_id = c.run_id 
left join consumer_ce ce on ce.client_id = c.client_id and ce.run_id = c.run_id 
left join consumer_combo com  on com.client_id = c.client_id and com.run_id = c.run_id   
left join consumer_comp_hg chg on chg.client_id = c.client_id and chg.run_id = c.run_id  
left join consumer_comp_fl cfl on cfl.client_id = c.client_id and cfl.run_id = c.run_id  
left join consumer_fth_other oth on oth.client_id = c.client_id and oth.run_id = c.run_id
left join consumer_flt flt on flt.client_id = c.client_id and flt.run_id = c.run_id
left join consumer_comp_fl_prod fp on fp.client_id = c.client_id and fp.run_id = c.run_id and fp.product_order = 1
left join consumer_comp_hg_prod hp on hp.client_id = c.client_id and hp.run_id = c.run_id and hp.product_order = 1
left join consumer_pm pm on pm.client_id = c.client_id and pm.run_id = c.run_id --and pm.dollar_pain_purch > 0  
left join consumer_dental de on de.client_id = c.client_id and de.run_id = c.run_id --and dollar_dental_purch > 0 
left join consumer_surg s on s.client_id = c.client_id and s.run_id = c.run_id --and dollar_surg_purch > 0  
left join consumer_other o on o.client_id = c.client_id and o.run_id = c.run_id --and dollar_other_purch > 0
left join consumer_vacc v on v.client_id = c.client_id and v.run_id = c.run_id --and dollar_other_purch > 0 
left join consumer_board b on b.client_id = c.client_id and b.run_id = c.run_id
left join consumer_groom g on g.client_id = c.client_id and g.run_id = c.run_id
left join consumer_nexgard ng on ng.client_id = c.client_id and ng.run_id = c.run_id 
left join consumer_prev prev on prev.client_id = c.client_id and prev.run_id = c.run_id  
 
where c.run_id = gv_input_run_id; 
 
delete from consumer_report c   
where c.run_id = gv_input_run_id
and flt_rev_12 = 0 
and last_vac_purch is null  
and last_pain_purch is null 
and last_dental_purch is null   
and last_emergency_purch is null
and last_surg_purch is null 
and last_pet_care_purch is null 
and last_pet_supply_purch is null 
and last_rx_purch is null   
and last_lab_purch is null  
and last_unk_purch is null  
and dollar_combo_purch_12 = 0   
and dollar_combo_purch_24 = 0   
and dollar_comp_hg_12 = 0   
and dollar_comp_hg_24 = 0   
and dollar_hg_purch_12 = 0  
and dollar_hg_purch_24 = 0  
and comp_fl_cert_rev_12 = 0 
and comp_fl_cert_rev_24 = 0 
and dollar_fl_purch_12 = 0  
and dollar_fl_purch_24 = 0  
and dollar_cert_purch_12 = 0
and (last_boarding_purch is not null or last_grooming_purch is not null)   
;
 
 
--remove clients without a first, last or address1  
delete from consumer_report where run_id = gv_input_run_id and record_type = '1DT' and ( 
   first_name = 'None Given'
   or last_name = 'None Given' or address1 = ' ' or address1 = 'None Given' or address1 like '%*%' or address2 like '%*%'
 
   or UPPER(first_name) like ('%DO NOT%') 
   or UPPER(first_name) like ('%DELETE%') 
   or UPPER(first_name) like ('%DECEASE%')
   or UPPER(first_name) like ('%SERVICE%')
   or UPPER(first_name) like ('%WRONG%')  
   or UPPER(first_name) like ('%MOVED%')  
   or UPPER(first_name) like ('%DEAD%')   
   or UPPER(first_name) like ('%TEST%')   
 
   or UPPER(last_name) like ('%DO NOT%')  
   or UPPER(last_name) like ('%DELETE%')  
   or UPPER(last_name) like ('%DECEASE%') 
   or UPPER(last_name) like ('%SERVICE%') 
   or UPPER(last_name) like ('%WRONG%')   
   or UPPER(last_name) like ('%MOVED%')   
   or UPPER(last_name) like ('%DEAD%')
 
 
   or UPPER(first_name) like ('OTC')  
   or UPPER(first_name) like ('OVER THE COUNTER')   
   or UPPER(first_name) like ('NEW')  
   or UPPER(first_name) like ('NEW CLIENT') 
   or UPPER(last_name) like ('OTC')   
   or UPPER(last_name) like ('OVER THE COUNTER')
   or UPPER(last_name) like ('CLIENT')
   or UPPER(last_name) like ('NEW CLIENT')
   or UPPER(last_name) like ('TEST CLIENT') 
   or UPPER(Address2) like ('%OVER THE COUNTER%')   
   or upper(first_name) like '%'||chr(33)||'%' or upper(first_name) like '%'||chr(42)||'%'
   or upper(first_name) like '%'||chr(33)||'%' or upper(first_name) like '%'||chr(42)||'%'
   or upper(last_name) like '%'||chr(33)||'%' or upper(last_name) like '%'||chr(42)||'%'
   or upper(Address1) like '%'||chr(33)||'%' or upper(Address1) like '%'||chr(42)||'%'  
   or UPPER(Address1) like ('%OTC%')  
   or UPPER(Address1) like ('%OVER THE COUNTER%')   
   or UPPER(Address1) like ('%RETURNED%') 
   or UPPER(Address2) like ('%OTC%')  
   or UPPER(Address2) like ('%OVER THE COUNTER%')   
   or UPPER(Address2) like ('%CREDIT%')   
   or UPPER(Address2) like ('%GRANDFATHER%')
   or UPPER(Address2) like ('%NO MAIL%')  
   or UPPER(Address2) like ('%ACCOUNT%')  
   or UPPER(Address2) like ('%DAD%')  
   or UPPER(Address2) like ('%INACTIVE%') 
   or UPPER(Address2) like ('%PARENTS%')  
   or UPPER(Address2) like ('%ALLERGIC%') 
   or UPPER(Address2) like ('%DAUGHTER%') 
   or UPPER(Address2) like ('%INCORRECT%')
   or UPPER(Address2) like ('%REFUSE%')   
   or UPPER(Address2) like ('%BAD%')  
   or UPPER(Address2) like ('%DEBT%') 
   or UPPER(Address2) like ('%INSUFFIENT%') 
   or UPPER(Address2) like ('%REMINDER%') 
   or UPPER(Address2) like ('%BANKRUPTCY%') 
   or UPPER(Address2) like ('%DISCONNECTED%')   
   or UPPER(Address2) like ('%INVALID%')  
   or UPPER(Address2) like ('%RETURNED%') 
   or UPPER(Address2) like ('%BILLING%')  
   or UPPER(Address2) like ('%DISCOUNT%') 
   or UPPER(Address2) like ('%INVENTORY%')
   or UPPER(Address2) like ('%SEND%') 
   or UPPER(Address2) like ('%BROTHER%')  
   or UPPER(Address2) like ('%DON''T%')   
   or UPPER(Address2) like ('%INVOICE%')  
   or UPPER(Address2) like ('%SISTER%')   
   or UPPER(Address2) like ('%CASH%') 
   or UPPER(Address2) like ('%EMERGENCY%')
   or UPPER(Address2) like ('%LAND LINE%')
   or UPPER(Address2) like ('%TEMPORARY%')
   or UPPER(Address2) like ('%CELL%') 
   or UPPER(Address2) like ('%EMPLOYEE%') 
   or UPPER(Address2) like ('%MAIL%') 
   or UPPER(Address2) like ('%UNABLE%')   
   or UPPER(Address2) like ('%CHANGE%')   
   or UPPER(Address2) like ('%FATHER%')   
   or UPPER(Address2) like ('%MESSAGE%')  
   or UPPER(Address2) like ('%UNDELIVERABLE%')  
   or UPPER(Address2) like ('%CHECK%')
   or UPPER(Address2) like ('%FAX%')  
   or UPPER(Address2) like ('%MOM%')  
   or UPPER(Address2) like ('%UPDATE%')   
   or UPPER(Address2) like ('%COLLECTIONS%')
   or UPPER(Address2) like ('%FILE%') 
   or UPPER(Address2) like ('%MOTHER%')   
   or UPPER(Address2) like ('%VERIFY%')   
   or UPPER(Address2) like ('%CONFIRM%')  
   or UPPER(Address2) like ('%FORWARD%')  
   or UPPER(Address2) like ('%NEED%') 
   or UPPER(Address2) like ('%WIFE%') 
   or UPPER(Address2) like ('%HUSBAND%')  
   or UPPER(Address2) like ('%DISCARDED%')
   or UPPER(Address2) like ('%'||chr(33)||'%')  
   or length(regexp_replace(address2,'[^[:digit:]]', null, 1,0,'i')) = 10  
   or length(regexp_replace(address2,'[^[:digit:]]', null, 1,0,'i')) = 16  
   or regexp_like(Address2,'[[:digit:]]{16}|[[:digit:]]{4}\s[[:digit:]]{4}\s[[:digit:]]{4}\s[[:digit:]]{4}|[[:digit:]]{4}\-[[:digit:]]{4}\-[[:digit:]]{4}\-[[:digit:]]{4}') -- cc numbers  
   or regexp_like(Address2,'[[:digit:]]{10}|[[:digit:]]{3}\s[[:digit:]]{3}\s[[:digit:]]{4}|[[:digit:]]{3}\-[[:digit:]]{3}\-[[:digit:]]{4}') -- phone numbers
   or (UPPER(First_Name) = 'CASH' and UPPER(Last_Name) in ('CLIENT','CASH')) 
) ;  
 
 
insert into consumer_report 
columns(record_type
,first_name
,last_name 
,prefix --4
,address1
,address2
,city
,state --8 
,postal_code   
,phone   
,last_hg_purch 
,last_ce_purch --12
,last_fl_purch 
,comp_last_hg_purch
,comp_last_fl_purch --15
,primary_email 
,client_id 
,canine_count  
,h_id
,run_id  
) select distinct  
'1HD'
,to_char(trunc(localtimestamp),'YYYY/MM/DD') todays_date 
, count(*) over(partition by r.run_id)+1 record_count  
,e.account_number --4 
,e.location_number 
,e.clinic_contact_first_name
,e.clinic_contact_last_name 
, '' contact_prefix --8 
,CAST(e.clinic_name AS VARCHAR2(40)) clinic_name
,cast(e.clinic_address_1 AS VARCHAR2(40)) clinic_address_1   
,e.clinic_address_line_2
,e.clinic_city --12
,e.clinic_state
,e.clinic_postal_code 
,cast(lpad(replace(replace(replace(replace(e.clinic_phone,'(',''),')',''),' ',''),'-',''),10) as varchar2(10)) phone  
,pc.ft_canine_patients --16 
,pc.hw_canine_patients
,pc.ft_feline_patients
,r.h_id  
,r.run_id
from consumer_report r 
inner join enrollment e on e.location_number = gv_input_location_number 
inner join consumer_patient_counts pc on pc.run_id = r.run_id
where r.run_id = gv_input_run_id; 
 
build_process_log('INFO','Finished running REPORT for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER');  
 
 
 
--delete from last where run_id = gv_input_run_id;
--delete from patients where run_id = gv_input_run_id;  
--delete from clients where run_id = gv_input_run_id;   
--delete from first_visits where run_id = gv_input_run_id;  
--delete from fth_purchases where run_id = gv_input_run_id; 
--delete from comp_fl_prod where run_id = gv_input_run_id;  
--delete from comp_hg_prod where run_id = gv_input_run_id;  
--delete from comp_hg_prod where run_id = gv_input_run_id;  
delete from consumer_all_trans2 where run_id = gv_input_run_id;  
 
--build_process_log('INFO','Finished running DELETE for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER');
 
commit;  
 
--else   
--  build_process_log('STARTED','Running PATIENTS for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER');  
  --raise_application_error(-20050,'An error occurred in PROD when attempting to run BUILD_CONSUMER:'||SQLERRM);  
  --end if;
  --copy_pat_master_prod(time_period_name);
--end if;
 
build_process_log('SUCCESS','Completed CONSUMER for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_CONSUMER');
 
-- write to the process log to indicate the completion of the build patient master procedure. 
 
exception
when no_patients then 
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' has no patients.','BUILD_CONSUMER');  
  raise; 
when others then   
  rollback;
--  delete from last where run_id = gv_input_run_id;
--  delete from patients where run_id = gv_input_run_id;
--  delete from clients where run_id = gv_input_run_id; 
--  delete from first_visits where run_id = gv_input_run_id;
--  delete from fth_purchases where run_id = gv_input_run_id; 
--  delete from comp_fl_prod where run_id = gv_input_run_id;
--  delete from comp_hg_prod where run_id = gv_input_run_id;
  commit;
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed:'||SQLERRM,'BUILD_CONSUMER');  
  raise; 
end; 


procedure build_control (input_run_id in number default null) as  
hid_to_run number; 
in_control_file number; 
not_in_control_file exception;  
 
--cursor clinics is select distinct hid from control_clinics; 
 
begin
 
select end_date into gv_input_end_Date from process_log where flag = 'RUNID' and run_id = gv_input_run_id;   
 
 
 
select location_number into gv_input_location_number from process_log where flag = 'RUNID' and run_id = gv_input_run_id;  
select h_id into gv_input_hid from process_log where flag = 'RUNID' and run_id = gv_input_run_id;  
 
select count(*) into in_control_file from control_clinics where hid = gv_input_hid;
if in_control_file = 0 then 
  raise not_in_control_file;
  end if;
 
--hid_to_run := input_hid;  
--select runid.nextval into gv_input_run_id from dual;  
--gv_input_report_type := 'CONTROL_ADDENDUM';   
 
build_process_log('STARTED','The RUNID '||gv_input_run_id||' for '||gv_input_report_type||' is started.','BUILD_CONTROL');
 
 
 
insert into /*+append*/ control_all_trans2 (LINEITEM_ID,INVOICE_DATE,SERVICE_ID,CLIENT_ID,H_ID,RUN_ID,COST,PATIENT_ID,VOIDED,QUANTITY)  
select lineitem_id,invoice_Date,service_id,l.client_id, l.h_id , gv_input_run_id run_id, cost, patient_id,voided,quantity
from cooked.lineitem l
where trunc(invoice_date,'mm') between add_months(trunc(to_Date(gv_input_end_Date),'mm'),-24) and trunc(to_date(to_Date(gv_input_end_Date)),'mm')  
and l.h_id = gv_input_hid   
and l.client_id is not null
and voided in( '0','f','F') 
;
 
delete from control_all_trans2 where (invoice_Date >= trunc(localtimestamp) or invoice_Date < add_months(trunc(localtimestamp),-24) ) and h_id = gv_input_hid and run_id = gv_input_run_id ;
 
 
insert into control_num_visits   
select l.run_id, l.client_id, l.h_id , count(distinct invoice_Date) num_visits   
from control_all_trans2 l  
where invoice_date between add_months(gv_input_end_Date,-12) and gv_input_end_Date   
and l.run_id = gv_input_run_id  
group by l.run_id, l.client_id, l.h_id ;  
 
 
insert into control_last_active  
select l.run_id, l.client_id, l.h_id , max(invoice_date) last_active, min(invoice_date) first_active
from control_all_trans2 l  
where trunc(invoice_date,'mm') between add_months(gv_input_end_Date,-24) and gv_input_end_Date
and l.run_id = gv_input_run_id  
group by l.run_id, l.client_id, l.h_id ;  
 
--delete from control_all_trans2 where run_id = gv_input_run_id and voided <> '0' and voided is not null;
 
build_process_log('INFO','Finished ALL_TRANS for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL'); 
 
insert into control_client_temp  
select distinct  l.run_id,l.h_id, nvl(l.client_id,0) client_id, cli.start_date
from control_all_trans2 l  
inner join (select distinct hid,start_Date from control_clinics) cli  on cli.hid = l.h_id
where l.run_id= gv_input_run_id 
;
 
insert into control_clients
select distinct c.CLIENT_ID ,   
c.H_ID,  
c.PMS_ID,
c.ENABLED, 
NVL(regexp_replace(c.first_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', NULL, 1,0,'i'),'None Given') first_name,   
NVL(regexp_replace(c.last_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', NULL, 1,0,'i'),'None Given') last_name, 
c.TITLE, 
c.DATE_DISABLED,   
c.PMS_CLIENT_CODE_ID, 
c.PRIMARY_EMAIL,   
c.PHONE1,
c.PHONE1_TYPE_ID,  
c.PHONE2,
c.PHONE2_TYPE_ID,  
c.PHONE3,
c.PHONE3_TYPE_ID,  
c.PHONE4,
c.PHONE4_TYPE_ID,  
c.LAST_UPDATE_DATE,
c.LAST_UPDATE_USER,
c.CREATE_DATE, 
c.CREATE_USER, 
c.SYSTEM_SRC_ID,   
c.DATE_MODIFIED
, p.is_active  
, p.dob  
, p.patient_id 
, l.start_date 
, l.run_id 
,max(case when months_between(gv_input_end_Date,p.dob)/12 > 5 and vi_species_id = 3 then 1 else 0 end) over(partition by c.client_id) dogs_over_5  
, vi_species_id
,min(p.first_active) over(partition by l.run_id,l.client_id) first_active  
,p.last_active 
,'' formal_name
,sum(case when months_between(gv_input_end_Date,p.dob)/12 > 4 and vi_species_id = 3 then 1 else 0 end) over(partition by c.client_id) dogs_over_4  
from control_client_temp l 
inner join cooked.client c on c.h_id = l.h_id and c.client_id = l.client_id
inner join cooked.patient_ref p on p.client_id = c.client_id and is_active = 1 and is_deceased = 0
where l.run_id= gv_input_run_id 
and vi_species_id in (3,7)  
;
 
 
--delete from control_clients where vi_species_id not in (3,7) and run_id = gv_input_run_id; 
 
build_process_log('INFO','Finished control_clients for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');   
 
/*   
insert into control_first_visit  
select min(invoice_date) first_visit ,l.client_id, l.h_id , gv_input_run_id
from cooked.lineitem l
where trunc(invoice_date,'mm') between '01-Jan-2009' and gv_input_end_Date 
and l.h_id = gv_input_hid   
and voided = '0'   
group by l.client_id, l.h_id;   
 
delete from control_first_visit v
where run_id = gv_input_run_id  
and client_id not in (select cc.client_id from control_clients cc where cc.run_id = gv_input_run_id ); 
 
delete from control_first_visit v
where run_id = gv_input_run_id  
and first_visit > trunc(localtimestamp) -1 ;
 
*/   
 
build_process_log('INFO','Finished first_visit for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');   
 
--purchases
insert into control_fth_purchases
select l.LINEITEM_ID  
,l.CLIENT_ID   
,l.PATIENT_ID  
,l.SERVICE_MEDICATION_ID
,l.H_ID  
,l.REGION_ID   
,l.VI_SPECIES_ID   
,l.INVOICE_DATE
,l.QUANTITY
,cost
,case when quantity_norm is null or cost_norm is null then 1 else quantity_norm end quantity_norm 
,l.NORM_RULE_ID
,l.UPDATE_RULE_ID  
,case when quantity_norm is null or cost_norm is null then 0 else cost_norm end cost_norm 
,l.ORIG_QUANTITY   
,l.ORIG_COST   
,l.DATE_NORMALIZED 
,l.DESCR_PACKAGE   
,l.PRICING_OVERRIDE
,l.PRODUCT 
,l.VOIDED
,c.run_id
,c.start_date  
from control_clients c 
inner join normalization.lineitem_norm_fth4 l on c.patient_id = l.patient_id and l.h_id = c.h_id  
where trunc(invoice_Date,'mm') between add_months(trunc(to_Date(gv_input_end_Date),'mm'),-24) and trunc(to_Date(gv_input_end_Date),'mm') 
and voided in ('0','f','F') 
and quantity_norm <> 0
and quantity_norm is not null   
and c.vi_species_id in (3,7)
and product in ('Activyl','Activyl Plus','Acuguard','Advantage','Advantage II','Advantage II Puppy/Kitten Pack','Advantage Multi','Advantage Multi Puppy/Kitten Pack','Advantage Puppy/Kitten Pack','Assurity','Capstar','Certifect','ComboGuard','Comfort
is','EasySpot','Frontline Plus','Frontline Plus Puppy/Kitten Pack','Frontline Top Spot','Frontline Tritak','Frontline Tritak Puppy/Kitten Pack','Heartgard Kitten Pack','Heartgard Plus','Heartgard Puppy Pack','Heartgard Puppy/Kitten Pack','Heartgard T
ablet','Interceptor Puppy/Kitten Pack','Iverhart Max','Iverhart Max Puppy Pack','Iverhart Plus','Iverhart Plus Puppy Pack','K9 Advantix','K9 Advantix II','K9 Advantix II Puppy Pack','K9 Advantix Puppy Pack','Parastar','Parastar Plus','Preventic Colla
r','Preventic Plus Collar','Program Flavor Tabs','Program Injection','Program Suspension','Proheart 6 Injectable','ProMeris','Revolution','Revolution Puppy/Kitten Pack','Sentinel','Sentinel Puppy Pack','Seresto','SimpleGd','SimpleGd3','Trifexis','Tri
fexis Puppy Pack','Tri-Heart Plus','Tri-Heart Plus Puppy Pack','Vectra','Vectra 3D','Vectra 3D Puppy Pack','Vectra Puppy/Kitten Pack','NexGard'
 
,'Adams Flea and Tick Collar','Adams Flea and Tick Mist','Adams Flea and Tick Mist Spray','Adams Flea and Tick Room Fogger','Adams Flea and Tick Shampoo','Cheristin','Frontline Spray','Heartgard Chewable','Interceptor','Paradyne','Preventic L.A. Spra
y','Sentinel Spectrum'
 
,'Bravecto'
)
and c.run_id = gv_input_run_id; 
 
delete from control_fth_purchases where (invoice_Date >= trunc(localtimestamp) or invoice_Date <add_months(trunc(localtimestamp),-24) ) and h_id = gv_input_hid and run_id = gv_input_run_id ;  
 
build_process_log('INFO','Finished fth_purchases for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL'); 
 
insert into control_last_combo   
select distinct max(invoice_Date) over(partition by c.client_id) last_combo_purch
,count(distinct case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) over(partition by c.client_id) combo_purch  
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost_norm else null end) over(partition by c.client_id) combo_rev
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then quantity_norm else null end) over(partition by c.client_id) combo_doses 
,'' last_pet_name  
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_species 
,c.client_id   
,p.run_id
from control_fth_purchases p 
inner join control_clients c on c.patient_id= p.patient_id and p.run_id = c.run_id 
inner join reports.product_families pf on pf.product = p.product 
where pf.type in ('fh','fth')   
and p.run_id = gv_input_run_id  
;
build_process_log('INFO','Finished last_combo for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');
 
insert into control_last_hg
select distinct max(invoice_Date) over(partition by c.client_id) last_hg_purch   
,count(distinct case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) over(partition by c.client_id) hg_purch 
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost_norm else null end) over(partition by c.client_id) hg_rev   
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then quantity_norm else null end) over(partition by c.client_id) hg_doses 
,last_value(formal_name) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_name   
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_species 
,c.client_id   
, p.run_id 
from control_fth_purchases p 
inner join control_clients c on c.patient_id= p.patient_id and p.run_id = c.run_id 
where product like 'Heartgard%' 
and p.run_id = gv_input_run_id; 
 
build_process_log('INFO','Finished last_hg  for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');
 
 
insert into control_last_fl
select distinct max(invoice_Date) over(partition by c.client_id) last_fl_purch   
,count(distinct case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) over(partition by c.client_id) fl_purch 
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost_norm else null end) over(partition by c.client_id) fl_rev   
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then quantity_norm else null end) over(partition by c.client_id) fl_doses 
,last_value(formal_name) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_name   
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_species 
,c.client_id   
,p.run_id
from control_fth_purchases p 
inner join control_clients c on c.patient_id= p.patient_id and p.run_id = c.run_id 
where product like 'Frontline%' and product <> 'Frontline Tritak'
and p.run_id = gv_input_run_id; 
 
build_process_log('INFO','Finished last_fl for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL'); 
 
insert into control_last_flt 
select distinct max(invoice_Date) over(partition by c.client_id) last_flt_purch  
,count(distinct case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) over(partition by c.client_id) flt_purch
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost_norm else null end) over(partition by c.client_id) flt_rev  
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then quantity_norm else null end) over(partition by c.client_id) flt_doses
,last_value(formal_name) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_name   
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_species 
,c.client_id   
,p.run_id
from control_fth_purchases p 
inner join control_clients c on c.patient_id= p.patient_id and p.run_id = c.run_id 
where product = 'Frontline Tritak'
and p.run_id = gv_input_run_id; 
 
build_process_log('INFO','Finished last_flt for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');
 
insert into control_last_int 
select distinct sum(case when invoice_Date between add_months(trunc(localtimestamp) ,-12) and trunc(localtimestamp) then quantity_norm else 0 end) over(partition by c.client_id) int_doses_12   
,sum(case when invoice_Date between add_months(trunc(localtimestamp) ,-24) and add_months(trunc(localtimestamp) ,-12)-1 then quantity_norm else 0 end) over(partition by c.client_id) int_doses_24   
,c.client_id   
,p.run_id
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then quantity_norm else null end) over(partition by c.client_id) int_doses
from control_fth_purchases p 
inner join control_clients c on c.patient_id= p.patient_id and p.run_id = c.run_id 
where product like 'Interceptor%' 
and p.run_id = gv_input_run_id; 
 
build_process_log('INFO','Finished last_int for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');
 
insert into control_last_ce
select distinct max(invoice_Date) over(partition by c.client_id) last_ce_purch   
,count(distinct case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) over(partition by c.client_id) ce_purch 
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost_norm else null end) over(partition by c.client_id) ce_rev   
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then quantity_norm else null end) over(partition by c.client_id) ce_doses 
,last_value(formal_name) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_name   
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_species 
,c.client_id   
,p.run_id run_id   
from control_fth_purchases p 
inner join control_clients c on c.patient_id= p.patient_id and p.run_id = c.run_id 
where product = 'Certifect' 
and p.run_id = gv_input_run_id; 
 
build_process_log('INFO','Finished last_ce for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL'); 
 
insert into control_nex
select distinct max(invoice_Date) over(partition by c.client_id) last_nex_purch  
,count(distinct case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) over(partition by c.client_id) nex_purch
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost_norm else null end) over(partition by c.client_id) nex_rev  
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then quantity_norm else null end) over(partition by c.client_id) nex_doses
,c.client_id   
,p.run_id run_id   
from control_fth_purchases p 
inner join control_clients c on c.patient_id= p.patient_id and p.run_id = c.run_id 
where product = 'NexGard'   
and p.run_id = gv_input_run_id; 
 
insert into control_last_comp_hg 
select distinct
 max(invoice_Date) over(partition by c.client_id) comp_last_hg_purch   
,count(distinct case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) over(partition by c.client_id) hg_purch 
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost_norm else null end) over(partition by c.client_id) hg_rev   
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then quantity_norm else null end) over(partition by c.client_id) hg_doses 
,last_value(formal_name) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_name   
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_species 
,last_value(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then p.product else null end) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) comp_hg_prod
,c.client_id   
,p.run_id run_id   
from control_fth_purchases p 
inner join control_clients c on c.patient_id= p.patient_id and p.run_id = c.run_id 
inner join reports.product_families pf on pf.product = p.product 
where p.product not like 'Heartgard%' and pf.type in ('h')   
and p.run_id = gv_input_run_id; 
 
build_process_log('INFO','Finished last_comp_hg for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');  
 
insert into control_last_comp_fl 
select distinct max(invoice_Date) over(partition by c.client_id) comp_last_fl_purch  
,count(distinct case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) over(partition by c.client_id) comp_fl_purch
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost_norm else null end) over(partition by c.client_id) comp_fl_rev  
,sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then quantity_norm else null end) over(partition by c.client_id) comp_fl_doses 
,last_value(formal_name) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_name   
,last_value(c.vi_species_id) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) last_pet_species 
,last_value(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then p.product else null end) over(partition by c.client_id order by invoice_date rows between unbounded preceding and unbounded following) comp_fl_prod
,c.client_id   
,p.run_id run_id   
from control_fth_purchases p 
inner join control_clients c on c.patient_id= p.patient_id and c.run_id = p.run_id 
inner join reports.product_families pf on pf.product = p.product 
where p.product not like 'Frontline%' and p.product not like 'NexGard%' and p.product not like 'Certifect%' and pf.type in ('ft','f')
and p.run_id = gv_input_run_id; 
 
build_process_log('INFO','Finished last_comp_fl for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');  
 
insert into control_diff   
select distinct sum(case when p.product = 'Trifexis' then quantity_norm  else 0 end) over(partition by c.client_id) trif_doses 
,sum(case when p.product = 'Revolution' then quantity_norm  else 0 end) over(partition by c.client_id) rev_doses  
,sum(case when p.product = 'Advantage Multi' then quantity_norm  else 0 end) over(partition by c.client_id) adv_doses 
,c.client_id   
,p.run_id run_id   
from control_fth_purchases p 
inner join control_clients c on c.patient_id= p.patient_id and c.run_id = p.run_id 
inner join reports.product_families pf on pf.product = p.product 
where invoice_Date > c.start_date 
and p.run_id = gv_input_run_id; 
 
build_process_log('INFO','Finished diff  for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');
 
insert into control_pm 
select l.h_id , c.run_id,l.client_id  
, count(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) num_pain_purch 
, max(invoice_Date) last_pain_purch   
, sum(case when cost is null then 0 else case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost else 0 end end) dollar_pain_purch
from control_all_trans2 l  
inner join cooked.service_medication4 sm on sm.service_medication_id = l.service_id and sm.service_Type_id = 2
inner join control_clients c on c.patient_id = l.patient_id and l.h_id = c.h_id 
where trunc(invoice_date,'mm') between add_months(gv_input_end_Date,-23) and gv_input_end_Date
and c.run_id = gv_input_run_id  
and (l.voided in( '0' ,'f','F'))
and cost <> 0  
group by l.h_id , c.run_id,l.client_id
;
 
 
 
insert into control_prev   
select l.h_id , c.run_id,l.client_id  
, count(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) num_prev_purch 
, max(invoice_Date) last_prev_purch   
, sum(case when cost is null then 0 else case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost else 0 end end) dollar_prev_purch
, sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then quantity_norm/dosage_volume_value_norm else 0 end) prev_pills   
, count(distinct case when c.vi_species_id =3 and invoice_Date between c.start_Date and trunc(localtimestamp)-1 then l.patient_id end) num_dogs
from normalization.lineitem_norm_nsd4 l   
inner join control_clients c on c.patient_id = l.patient_id and l.h_id = c.h_id 
where 1=1
and trunc(invoice_date,'mm') between add_months(gv_input_end_Date,-23) and gv_input_end_Date  
and c.run_id = gv_input_run_id  
and (l.voided in( '0' ,'f','F'))
and product = 'Previcox'
and cost <> 0  
and quantity_norm is not null and quantity_norm <> 0
group by l.h_id , c.run_id,l.client_id
;
 
 
 
build_process_log('INFO','Finished pm  for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');  
 
 
insert into control_dental 
select l.h_id , c.run_id,l.client_id  
, count(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) num_dental_purch   
, max(invoice_Date) last_dental_purch 
, sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost else 0 end) dollar_dental_purch  
 from control_all_trans2 l 
inner join control_clients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_general sg on sg.service_general_id = l.service_id and product_info = 'Surgery - dental'
where cost <> 0
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,l.client_id;   
 
 
insert into control_dental_hyg   
select l.h_id , c.run_id,l.client_id  
,max(case when product_group = 'Oravet' then invoice_Date end) last_oravet_purch 
,count(case when product_group = 'Oravet' and invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) num_oravet_purch
,sum(case when product_group = 'Oravet' and invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost else 0 end) dollar_oravet_purch
,sum(case when product_group = 'Oravet' and invoice_Date between c.start_Date and trunc(localtimestamp)-1 then dental_doses(l.cost,l.quantity,l.service_id,l.patient_id) else 0 end) doses_oravet_purch   
 
,max(case when product_group = 'Competitive' then invoice_Date end) last_dental_hyg_purch 
,count(case when product_group = 'Competitive' and invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) num_dental_hyg_purch 
,sum(case when product_group = 'Competitive' and invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost else 0 end) dollar_dental_hyg_purch
,sum(case when product_group = 'Competitive' and invoice_Date between c.start_Date and trunc(localtimestamp)-1 then dental_doses(l.cost,l.quantity,l.service_id,l.patient_id) else 0 end) doses_dental_hyg_purch
 
from control_all_trans2 l  
inner join control_clients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_general sg on sg.service_general_id = l.service_id --and product_info = 'Surgery - dental'  
inner join dental_hyg_prod p on p.product = sg.product_info 
where cost <> 0
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,l.client_id;   
 
insert into control_surg   
select l.h_id , c.run_id,l.client_id  
, count(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) num_surg_purch 
, max(invoice_Date) last_surg_purch   
, sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost else 0 end) dollar_surg_purch 
 from control_all_trans2 l 
inner join control_clients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_general sg on sg.service_general_id = l.service_id and product_info like 'Surgery%' and product_info <> 'Surgery - dental'   
where cost <> 0
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,l.client_id;   
 
build_process_log('INFO','Finished surg for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL'); 
 
insert into control_vacc   
select l.h_id , c.run_id,l.client_id  
, count(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) num_vacc_purch 
, max(invoice_Date) last_vacc_purch   
, sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost else 0 end) dollar_vacc_purch 
 from control_all_trans2 l 
inner join control_clients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_vaccine2 sg on sg.service_vaccine_id = l.service_id 
where cost <> 0
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,l.client_id;   
 
build_process_log('INFO','Finished vacc for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL'); 
 
insert into control_other  
select l.h_id , c.run_id,l.client_id  
, count(case when l.invoice_Date between c.start_Date and trunc(localtimestamp)-1 then l.lineitem_id else null end) num_other_purch
, max(l.invoice_Date) last_other_purch
, sum(case when l.invoice_Date between c.start_Date and trunc(localtimestamp)-1 then l.cost else 0 end) dollar_other_purch 
 from control_all_trans2 l 
inner join control_clients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
left join cooked.service_general sg on sg.service_general_id = l.service_id and (sg.product_info like 'Surgery%' or lower(sg.product_info) like '%dental%' or lower(PRODUCT_INFO) in('service - boarding') or lower(PRODUCT_INFO) in('service - grooming')
)
 
left join cooked.service_diet2 sd on sd.service_diet_id = l.service_id 
left join control_fth_purchases ft on ft.lineitem_id = l.lineitem_id  
left join cooked.service_medication4 n on n.service_medication_id = l.service_id and n.service_type_id = 2  
left join cooked.service_vaccine2 sv on sv.service_vaccine_id = l.service_id
where l.cost <> 0  
and sg.service_general_id is null 
and sd.service_diet_id is null  
and sv.service_vaccine_id is null 
and ft.lineitem_id is null  
and n.service_medication_id is null   
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,l.client_id;   
 
build_process_log('INFO','Finished other for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONTROL');
 
--   
 
insert into control_board  
select l.h_id , c.run_id,l.client_id  
, count(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) num_board_purch
, max(invoice_Date) last_board_purch  
, sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost else 0 end)  dollar_board_purch  
from control_all_trans2 l  
inner join control_clients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_general sg on sg.service_general_id = l.service_id and  lower(PRODUCT_INFO) in('service - boarding') 
where cost <> 0
--and invoice_Date between add_months(to_date(c.end_date),-24) and to_date(gv_input_end_Date)-1   
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,l.client_id;   
 
build_process_log('INFO','Finished BOARDING for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONSUMER'); 
 
insert into control_groom  
select l.h_id , c.run_id,l.client_id  
, count(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then lineitem_id else null end) num_groom_purch
, max(invoice_Date) last_groom_purch  
, sum(case when invoice_Date between c.start_Date and trunc(localtimestamp)-1 then cost else 0 end) dollar_groom_purch
from control_all_trans2 l  
inner join control_clients c on c.run_id= l.run_id and c.patient_id= l.patient_id  
inner join cooked.service_general sg on sg.service_general_id = l.service_id and  lower(PRODUCT_INFO) in('service - grooming') 
where cost <> 0
--and invoice_Date between add_months(to_date(c.end_date),-24) and to_date(c.end_date)-1
and l.run_id = gv_input_run_id  
group by l.h_id , c.run_id,l.client_id;   
 
build_process_log('INFO','Finished GROOMING for RUNID '||gv_input_run_id||' for '||gv_input_report_type||' hid '||gv_input_hid||'.','BUILD_CONSUMER'); 
 
 
 
 
 
insert into control_report_detail 
select   
distinct '1DT' record_type  
,cast(NVL(regexp_replace(c.first_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', NULL, 1,0,'i'),'None Given') as varchar2(30)) first_name
,cast(NVL(regexp_replace(c.last_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', NULL, 1,0,'i'),'None Given') as varchar2(30)) last_name  
,trim(cast (lpad(c.title,6) as varchar2(12))) prefix --4 
,trim(cast(NVL(lpad(regexp_replace(a.address1, 'do not|delete|decease|service|wrong|moved|dead',NULL,1,0,'i'),40),'None Given') as varchar2(40))) address1
,trim(cast(lpad(regexp_replace(a.address2, 'do not|delete|decease|service|wrong|moved|dead',NULL,1,0,'i'),40) as varchar2(40))) address2 
,trim(cast(lpad(a.city,30) as varchar2(30))) city   
,cast(lpad(a.state,2) as varchar2(6)) state --8 
,trim(cast(lpad(substr((case when regexp_like(a.postal_code, '([^0-9])') then regexp_replace(a.postal_code, '([^0-9])', null, 1, 0, 'i') else a.postal_code end),1,9),30) as varchar2(40))) postal_code
,cast(regexp_replace(substr((case when regexp_like(a.phone1,'([^0-9])') then regexp_replace(a.phone1,'([^0-9])',null,1,0,'i') else a.phone1 end),1,10),'([[:cntrl:]]+)', null, 1,0,'i') as varchar2(10)) phone 
,cast(to_char(lh.last_hg_purch,'mm/dd/yyyy') as varchar2(40)) last_hg_purch
,cast(to_char(lce.last_ce_purch,'mm/dd/yyyy') as varchar2(30)) last_ce_purch --12
,cast(to_char(lf.last_fl_purch,'mm/dd/yyyy') as varchar2(10)) last_fl_purch
,cast(to_char(ch.comp_last_hg_purch,'mm/dd/yyyy') as varchar2(30)) comp_last_hg_purch
,cast(to_char(cf.comp_last_fl_purch,'mm/dd/yyyy') as varchar2(10)) comp_last_fl_purch --15
,case when regexp_count(primary_email, '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$',1,'i')>0 then primary_email end primary_email
,c.client_id   
,count(distinct case when c.vi_species_id = 3 then c.patient_id end) over(partition by c.client_id) canine_count  
,count(distinct case when c.vi_species_id = 7 then c.patient_id end) over(partition by c.client_id) feline_count  
,decode(lf.last_pet_species,3,'Canine',7,'Feline') fl_pet_species --20 
,decode(lh.last_pet_species,3,'Canine',7,'Feline') hg_pet_species
,decode(cf.last_pet_species,3,'Canine',7,'Feline') comp_fl_pet_species 
,decode(ch.last_pet_species,3,'Canine',7,'Feline') comp_hg_pet_species 
,nvl(ce_purch,0) num_ce_purch   
,nvl(fl_purch,0) num_fl_purch --25
,nvl(comp_fl_purch,0) num_comp_fl_purch   
,trim(nvl(to_char(ce_rev,'999990.00'),'0.00')) dollar_ce_purch   
,trim(nvl(to_char(fl_rev,'999990.00'),'0.00')) dollar_fl_purch   
,trim(nvl(to_char(comp_fl_rev,'999990.00'),'0.00')) dollar_comp_fl_purch   
,nvl(hg_purch,0) num_hg_purch --30
,nvl(comp_hg_purch,0) num_comp_hg_purch   
,trim(nvl(to_char(hg_rev,'999990.00'),'0.00')) dollar_hg_purch   
,trim(nvl(to_char(comp_hg_rev,'999990.00'),'0.00')) dollar_comp_hg_purch   
,to_char(c.first_active,'mm/dd/yyyy') first_visit   
,to_char(last_vacc_purch,'mm/dd/yyyy') last_vacc_purch --35  
,to_char(pm.last_pain_purch,'mm/dd/yyyy') last_pain_purch
,to_char(de.last_dental_purch,'mm/dd/yyyy') last_dental_purch
,cast(null as varchar2(1)) last_es_purch  
,to_char(last_board_purch,'mm/dd/yyyy') last_boarding_purch  
,to_char(last_groom_purch,'mm/dd/yyyy') last_grooming_purch --40 
,to_char(s.last_surg_purch,'mm/dd/yyyy') last_surg_purch 
,cast(null as varchar2(1)) last_pc_purch  
,cast(null as varchar2(1)) last_ps_purch  
,cast(null as varchar2(1)) last_pmed_purch
,cast(null as varchar2(1)) last_lab_purch --45  
,to_char(last_other_purch,'mm/dd/yyyy') last_unk_purch-- 46  
,nvl(num_vacc_purch,0) num_vacc_purch 
,nvl(pm.num_pain_purch,0) num_pain_purch  
,nvl(de.num_dental_purch,0) num_dental_purch
,cast(0 as varchar2(1)) num_es_purch --50 
,nvl(num_board_purch,0) num_boarding_purch
,nvl(num_groom_purch,0) num_grooming_purch
,nvl(s.num_surg_purch,0) num_surg_purch   
,cast(0 as varchar2(1)) num_pc_purch  
,cast(0 as varchar2(1)) num_ps_purch --55 
,cast(0 as varchar2(1)) num_pmed_purch
,cast(0 as varchar2(1)) num_lab_purch 
,nvl(o.num_other_purch,0) num_unk_purch --58
,case when ce_doses < 0 then 0 else nvl(ce_doses,0) end ce_doses 
,case when fl_doses < 0 then 0 else nvl(fl_doses,0) end fl_doses --60  
,case when comp_fl_doses < 0 then 0 else nvl(comp_fl_doses,0) end comp_fl_doses  
,comp_fl_prod comp_fl_prod -- name of comp fl/ce prod  
,case when hg_doses < 0 then 0 else nvl(hg_doses,0) end hg_doses 
,case when comp_hg_doses < 0 then 0 else nvl(comp_hg_doses,0) end comp_hg_doses  
,comp_hg_prod comp_hg_prod -- name of comp hg prod --65
,trim(nvl(to_char(dollar_vacc_purch,'999990.00'),'0.00')) dollar_vacc_purch
,trim(nvl(to_char(pm.dollar_pain_purch,'999990.00'),'0.00')) dollar_pain_purch   
,trim(nvl(to_char(de.dollar_dental_purch,'999990.00'),'0.00')) dollar_dental_purch   
,'0.00' dollar_es_purch 
,trim(nvl(to_char(dollar_board_purch,'999990.00'),'0.00')) dollar_boarding_purch --70
,trim(nvl(to_char(dollar_groom_purch,'999990.00'),'0.00')) dollar_grooming_purch 
,trim(nvl(to_char(s.dollar_surg_purch,'999990.00'),'0.00')) dollar_surg_purch
,'0.00' dollar_pc_purch 
,'0.00' dollar_ps_purch 
,'0.00' dollar_pmed_purch --75  
,'0.00' dollar_lab_purch
,trim(nvl(to_char(dollar_other_purch,'999990.00'),'0.00')) dollar_unk_purch
,dogs_over_5   
,to_char(last_combo_purch,'mm/dd/yyyy') last_combo_purch 
,nvl(lc.combo_purch,0) num_combo_purch --80 
,trim(nvl(to_char(lc.combo_rev,'999990.00'),'0.00')) dollar_combo_purch
,case when lc.combo_doses < 0 then 0 else nvl(lc.combo_doses,0) end doses_combo_purch
,case when trif_doses < 0 then 0 else nvl(trif_doses,0) end trif_doses 
,case when rev_doses < 0 then 0 else nvl(rev_doses,0) end rev_doses
,case when adv_doses < 0 then 0 else nvl(adv_doses,0) end adv_doses --85   
,case when i.int_doses < 0 then 0 else nvl(i.int_doses,0) end int_doses
,case when int_doses_12 < 0 then 0 else nvl(int_doses_12,0) end int_doses_12 
,case when int_doses_24 < 0 then 0 else nvl(int_doses_24,0) end int_doses_24 
,to_char(t.last_flt_purch,'mm/dd/yyyy') last_flt_purch 
,nvl(t.flt_purch,0) flt_purch --90
,trim(nvl(to_char(flt_rev,'999990.00'),'0.00')) dollar_flt_purch 
,case when t.flt_doses < 0 then 0 else nvl(t.flt_doses,0) end flt_doses
,to_char(la.last_active,'mm/dd/yyyy') last_active   
,nvl(nv.num_visits,0) --94  
,c.h_id  
,c.run_id
 
,to_char(n.last_nex_purch,'mm/dd/yyyy') last_nex_purch 
,nvl(n.nex_purch,0) nex_purch   
,trim(nvl(to_char(nex_rev,'999990.00'),'0.00')) dollar_nex_purch 
,case when n.nex_doses < 0 then 0 else nvl(n.nex_doses,0) end nex_doses
,to_char(p.last_prev_purch,'mm/dd/yyyy') last_prev_purch 
,nvl(p.num_prev_purch,0) prev_purch --100 
,trim(nvl(to_char(dollar_prev_purch,'999990.00'),'0.00')) dollar_prev_purch
,case when prev_pills < 0 then 0 else nvl(prev_pills,0) end prev_pills 
-- aded 9/25/2015  
,dogs_over_4   
,nvl(p.num_dogs,0) num_dogs_prev
,to_char(last_oravet_purch,'mm/dd/yyyy') last_oravet_purch --105 
,nvl(NUM_ORAVET_PURCH,0) NUM_ORAVET_PURCH 
,trim(nvl(to_char(DOLLAR_ORAVET_PURCH,'999990.00'),'0.00')) DOLLAR_ORAVET_PURCH  
,case when DOSES_ORAVET_PURCH < 0 then 0 else nvl(DOSES_ORAVET_PURCH,0) end DOSES_ORAVET_PURCH
,to_char(LAST_DENTAL_HYG_PURCH,'mm/dd/yyyy')
,nvl(NUM_DENTAL_HYG_PURCH,0) NUM_DENTAL_HYG_PURCH--110 
,trim(nvl(to_char(DOLLAR_DENTAL_HYG_PURCH,'999990.00'),'0.00')) DOLLAR_DENTAL_HYG_PURCH 
,case when DOSES_DENTAL_HYG_PURCH < 0 then 0 else nvl(DOSES_DENTAL_HYG_PURCH,0) end DOSES_DENTAL_HYG_PURCH  
 
 
from control_clients c 
inner join cooked.address a on a.client_id = c.client_id and a.h_id = c.h_id and address_type_id = 2
left join control_last_hg lh on lh.client_id = c.client_id and lh.run_id = c.run_id --and hg_rev <> 0  
left join control_last_ce lce on lce.client_id = c.client_id and lce.run_id = c.run_id --and ce_rev <> 0 
left join control_last_fl lf on lf.client_id = c.client_id and lf.run_id = c.run_id --and fl_rev <> 0  
left join control_last_comp_hg ch on ch.client_id = c.client_id and ch.run_id = c.run_id --and comp_hg_rev <> 0  
left join control_last_comp_fl cf on cf.client_id = c.client_id and cf.run_id = c.run_id --and comp_fl_rev <> 0  
left join control_first_visit fv on fv.client_id = c.client_id and fv.run_id = c.run_id  
left join control_last_combo lc on lc.client_id = c.client_id and lc.run_id = c.run_id-- and combo_rev <> 0  
left join control_diff d on d.client_id = c.client_id and d.run_id = c.run_id   
left join control_last_int i on i.client_id = c.client_id and i.run_id = c.run_id  
left join control_pm pm on pm.client_id = c.client_id and pm.run_id = c.run_id --and pm.dollar_pain_purch > 0
left join control_dental de on de.client_id = c.client_id and de.run_id = c.run_id --and dollar_dental_purch > 0 
left join control_surg s on s.client_id = c.client_id and s.run_id = c.run_id --and dollar_surg_purch > 0
left join control_other o on o.client_id = c.client_id and o.run_id = c.run_id --and dollar_other_purch > 0  
left join control_vacc v on v.client_id = c.client_id and v.run_id = c.run_id --and dollar_other_purch > 0   
left join control_last_flt t on t.client_id = c.client_id and t.run_id = c.run_id  
left join control_num_visits nv on nv.client_id = c.client_id and nv.run_id = c.run_id   
left join control_last_active la on la.client_id = c.client_id and la.run_id = c.run_id  
left join control_prev p on p.client_id = c.client_id and p.run_id = c.run_id   
left join control_nex n on n.client_id = c.client_id and n.run_id = c.run_id
left join control_board b on b.client_id = c.client_id and b.run_id = c.run_id  
left join control_groom g on g.client_id = c.client_id and g.run_id = c.run_id  
left join control_dental_hyg hyg on hyg.client_id = c.client_id and hyg.run_id = c.run_id
where a.state not in ('AB','BC','MB','NB','NL','NT','NS','NU','ON','PE','QC','SK','YT','PR')  
and c.run_id = gv_input_run_id  
--and 1=0
;
 
delete from control_report_detail where run_id = gv_input_run_id and (first_name = 'None Given' or last_name = 'None Given' or address1 = 'None Given') ;
delete from control_report_detail where run_id = gv_input_run_id and first_visit is null ;
 
--------------------------------------------------------------------------------------------------------------------------------   
--------- HEADER FILE ----------------------------------------------------------------------------------------------------------   
--------------------------------------------------------------------------------------------------------------------------------   
--create table control_header as 
insert into control_report_header
columns(record_type
,first_name
,last_name 
, prefix --4   
,address1
,address2
,city
,state --8 
,postal_code   
,phone   
,last_hg_purch 
,last_ce_purch --12
,last_fl_purch 
,comp_last_hg_purch
,comp_last_fl_purch --15
,h_id
,run_id  
) select distinct '1HD' 
,to_char(trunc(localtimestamp),'MM/DD/YYYY') todays_date 
, count(*) over(partition by h_id)+1 record_count   
,e.account_number  
,e.location_number 
,e.clinic_contact_first_name
,e.clinic_contact_last_name 
, '' contact_prefix
,cast(e.clinic_name as varchar2(40)) clinic_name
,cast(e.clinic_address_1 as varchar2(40)) 
,e.clinic_address_line_2
,e.clinic_city 
,e.clinic_state
,e.clinic_postal_code 
,regexp_replace(substr((case when regexp_like(e.clinic_phone,'([^0-9])') then regexp_replace(e.clinic_phone,'([^0-9])',null,1,0,'i') else e.clinic_phone end),1,10),'([[:cntrl:]]+)', null, 1,0,'i')  phone
,c.hid   
,r.run_id
from control_clinics c 
inner join enrollment e on e.location_number = c.location_number  
left join control_report_detail r on r.h_id = c.hid --and c.run_id = r.run_id
where r.run_id = gv_input_run_id; 
 
 
 
delete from control_all_trans2 where run_id = gv_input_run_id;
delete from control_fth_purchases where run_id = gv_input_run_id; 
 
 
build_process_log('SUCCESS','The RUNID '||gv_input_run_id||' for '||gv_input_report_type||' is complete.','BUILD_CONTROL');   
 
--end loop;
--close clinics;   
 
exception
when not_in_control_file then   
build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' is not in the control file.','BUILD_CONTROL'); 
  raise; 
 
when others then   
build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed:'||SQLERRM,'BUILD_CONTROL'); 
  raise; 
 
end build_control; 

 
 
procedure build_last_dose_clients  as
split_count number;
pat_count number;  
fth_sales_count number; 
 
 no_patients exception; 
 no_fth_sales exception;
 
begin

 
 
select location_number into gv_input_location_number from process_log where flag = 'RUNID' and run_id = gv_input_run_id;  
select end_Date into gv_input_end_date from process_log where flag = 'RUNID' and run_id = gv_input_run_id;   
 
select count(*) into split_count from split_clients where location_number = gv_input_location_number and h_id = GV_INPUT_HID and rownum < 2;  
 
 
build_process_log('STARTED','Running LAST DOSE for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_LAST_DOSE');
 
--insert into last_dose_pat_ref select pr.*, gv_input_run_id run_id from cooked.patient_ref pr where h_id = gv_input_hid; 
 
insert into last_dose_patients   
select distinct pr.first_active,pr.last_active  
,p.is_active, p.vi_species_id, p.dob  
,p.patient_id, p.client_id, p.h_id
,gv_input_run_id   
,gv_input_start_date  
,pl.end_Date   
,months_between(pl.end_Date,p.dob)/12 age_years   
,case when months_between(pl.end_Date,p.dob)/12 > 6 and p.vi_species_id = 3 then 1 else 0 end dogs_over_6   
,case when months_between(pl.end_Date,p.dob)/12 < 1 and p.vi_species_id = 3 then 1 else 0 end puppy   
,weight  
,case when months_between(pl.end_Date,p.dob)/12 > 4 and p.vi_species_id = 3 then 1 else 0 end dogs_over_4   
from cooked.patient p 
inner join cooked.patient_ref pr on pr.patient_id = p.patient_id and pr.h_id = p.h_id
inner join process_log pl on pl.h_id = p.h_id and flag = 'RUNID' and pr.last_active > add_months(pl.end_date,-24)
left join (select distinct hid,patient_id,first_value(case when weight_unit = 'lbs' then normalization.safe_to_number(weight)  
when upper(weight_unit) = upper('pounds') then normalization.safe_to_number(weight)   
when upper(weight_unit) = upper('Ounces') then normalization.safe_to_number(weight)/16
when upper(weight_unit) = upper('kilograms') then normalization.safe_to_number(weight)*2.2
when upper(weight_unit) = upper('grams') then (normalization.safe_to_number(weight)/1000)*2.2   
else normalization.safe_to_number(weight)
end) over(partition by patient_id order by visit_date desc) weight from vetstreet.pgsql_patient_vitals ) pv on pv.patient_id = p.patient_id and pv.hid = p.h_id and weight < 500   
LEFT JOIN Vetstreet.Pgsql_Pms_Patient_Code_Lookup pcl ON p.Pms_Patient_Code_Id = pcl.Id
where p.is_active = 1
and p.is_deceased = 0
and p.vi_species_id in (3,7)  
and p.patient_id > 0  
and pl.run_id = gv_input_Run_id 
AND Coalesce(pcl.VI_CODE_ID, 2) IN (2, 3, 4)
;
gv_row_count := SQL%ROWCOUNT; 

-- Check for patients 
if gv_row_count  = 0 then
  raise no_patients;  
end if;  
 

build_process_log('INFO','Finished '||gv_row_count||' PATIENTS for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_LAST_DOSE');
gv_row_count := 0;


if split_count = 0 then 
 
  insert into last_dose_clients  
  select distinct c.client_id   
  ,nvl(regexp_replace(regexp_replace(c.first_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]|\^|\*|\)|\(|\:|\+)', null, 1,0,'i'),'None Given') first_name  
  ,nvl(regexp_replace(regexp_replace(c.last_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]|\^|\*|\)|\(|\:|\+)', null, 1,0,'i'),'None Given') last_name
  ,regexp_replace(c.title,'([[:cntrl:]]+)', null, 1,0,'i') prefix
  ,nvl(regexp_replace(regexp_replace(a.address1, 'do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i'),'None Given') address1 
  ,regexp_replace(regexp_replace(a.address2, 'do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i') address2
  ,regexp_replace(a.city,'([[:cntrl:]]+)', null, 1,0,'i') city   
  ,regexp_replace(a.state,'([[:cntrl:]]+)', null, 1,0,'i') state 
  ,regexp_replace(lpad(substr((case when regexp_like(a.postal_code, '([^0-9])') then regexp_replace(a.postal_code, '([^0-9])', null, 1, 0, 'i') else a.postal_code end),1,9),30),'([[:cntrl:]]+)', null, 1,0,'i') postal_code 
  ,c.h_id
  ,p.dogs_over_6   
  ,p.puppy 
  ,p.run_id
  ,p.start_Date
  ,p.end_Date  
  ,p.canines   
  ,p.felines   
   ,replace(regexp_replace(c.primary_email ,'([[:cntrl:]]|\^|\*|\)|\(|\:|\+)', null, 1,0,'i'), chr(10), chr(32)) primary_email 
  ,regexp_replace(substr((case when regexp_like(a.phone1,'([^0-9])') then regexp_replace(a.phone1,'([^0-9])',null,1,0,'i') else a.phone1 end),1,10),'([[:cntrl:]]+)', null, 1,0,'i')  phone1 
  ,c.title 
  ,p.first_visit   
  ,dogs_over_4 
  from cooked.client c
  inner join cooked.address a on a.client_id = c.client_id and a.address_type_id = 2 
  inner join (select distinct client_id,h_id,run_id,start_date   
,max(dogs_over_6) over (partition by client_id,run_id) dogs_over_6 
,sum(dogs_over_4) over (partition by client_id,run_id) dogs_over_4 
,max(puppy) over (partition by client_id,run_id) puppy   
,end_date
,count(distinct case when vi_Species_id = 3 then patient_id end) over(partition by client_id,run_id) canines 
,count(distinct case when vi_Species_id = 7 then patient_id end) over(partition by client_id,run_id) felines 
,min(first_active)  over(partition by client_id,run_id) first_visit
from last_dose_patients) P
  on p.client_id = c.client_id  
  LEFT JOIN Vetstreet.Pgsql_Pms_Client_Code_Lkup Ccl ON C.Pms_Client_Code_Id = Ccl.Id
  where p.run_id = gv_input_run_id
  and C.Enabled = '1'
   AND Coalesce(Ccl.Vi_Code_Id, 2) IN (2, 3, 4)
  ;  
gv_row_count := SQL%ROWCOUNT; 

build_process_log('INFO','Finished '||gv_row_count||' CLIENTS for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_LAST_DOSE');
gv_row_count := 0;
else 
 
  insert into last_dose_clients  
  select distinct c.client_id   
  ,nvl(regexp_replace(regexp_replace(c.first_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]|\^|\*|\)|\(|\:|\+)', null, 1,0,'i'),'None Given') first_name  
  ,nvl(regexp_replace(regexp_replace(c.last_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]|\^|\*|\)|\(|\:|\+)', null, 1,0,'i'),'None Given') last_name
  ,regexp_replace(c.title,'([[:cntrl:]]+)', null, 1,0,'i') prefix
  ,nvl(regexp_replace(regexp_replace(a.address1, 'do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i'),'None Given') address1 
  ,regexp_replace(regexp_replace(a.address2, 'do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i') address2
  ,regexp_replace(a.city,'([[:cntrl:]]+)', null, 1,0,'i') city   
  ,regexp_replace(a.state,'([[:cntrl:]]+)', null, 1,0,'i') state 
  ,regexp_replace(lpad(substr((case when regexp_like(a.postal_code, '([^0-9])') then regexp_replace(a.postal_code, '([^0-9])', null, 1, 0, 'i') else a.postal_code end),1,9),30),'([[:cntrl:]]+)', null, 1,0,'i') postal_code 
  ,c.h_id
  ,p.dogs_over_6   
  ,p.puppy 
  ,p.run_id
  ,p.start_Date
  ,p.end_Date  
  ,p.canines   
  ,p.felines   
  ,replace(regexp_replace(c.primary_email ,'([[:cntrl:]]|\^|\*|\)|\(|\:|\+)', null, 1,0,'i'), chr(10), chr(32)) primary_email 
  ,regexp_replace(substr((case when regexp_like(a.phone1,'([^0-9])') then regexp_replace(a.phone1,'([^0-9])',null,1,0,'i') else a.phone1 end),1,10),'([[:cntrl:]]+)', null, 1,0,'i')  phone1 
  ,c.title 
  ,p.first_visit   
  ,dogs_over_4 
  from cooked.client c
  inner join cooked.address a on a.client_id = c.client_id and a.address_type_id = 2 
  inner join (select distinct client_id,h_id,run_id,start_date   
,max(dogs_over_6) over (partition by client_id,run_id) dogs_over_6 
,sum(dogs_over_4) over (partition by client_id,run_id) dogs_over_4 
,max(puppy) over (partition by client_id,run_id) puppy   
,end_date 
,count(distinct case when vi_Species_id = 3 then patient_id end) over(partition by client_id,run_id) canines 
,count(distinct case when vi_Species_id = 7 then patient_id end) over(partition by client_id,run_id) felines 
,min(first_active)  over(partition by client_id,run_id) first_visit
from last_dose_patients) p   
  on p.client_id = c.client_id  
  left join split_clients sc on sc.location_number = gv_input_location_number and sc.h_id = c.h_id and sc.pms_client_id = c.pms_id  
  where p.run_id = gv_input_run_id
  and ((split_count > 0 and sc.pms_client_id is not null) or (split_count = 0 )) ;   
gv_row_count := SQL%ROWCOUNT; 

build_process_log('INFO','Finished '||gv_row_count||' SPLIT CLIENTS for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_LAST_DOSE');
gv_row_count := 0;

end if;  

 
--remove clients without a first, last or address1  
delete from last_dose_clients where run_id = gv_input_run_id and (state is null or city is null);  
delete from last_dose_clients where run_id = gv_input_run_id
and (upper(state) like 'AB%' or upper(state) like 'BC%' or upper(state) like 'MB%' or upper(state) like 'NB%' or upper(state) like 'NL%' or upper(state) like 'NT%' or upper(state) like 'NS%'   
or upper(state) like 'NU%' or upper(state) like 'ON%' or upper(state) like 'PE%' OR upper(state) LIKE 'QC%' or upper(state) like 'SK%' or upper(state) like 'YT%' or upper(state) like 'PR%' 
);   
 
delete from last_dose_clients where (postal_code is null or postal_code like '%0000%' or postal_code = '000000' or postal_code = '0' or postal_code = '00' or postal_code = '000') and run_id = gv_input_run_id;
 
delete from last_dose_clients
where run_id = gv_input_run_id  
and(UPPER(First_Name) in ('OTC','OVER THE COUNTER','NEW','NEW CLIENT','TEST','NONE GIVEN')
or upper(First_Name) like '%'||chr(42)||'%' 
or upper(First_Name) like '%CLINIC%'  
or upper(First_Name) like '%VETERINARY%'
or upper(First_Name) like '%HOSPITAL%'
or upper(First_Name) like '% VET %'   
or upper(First_Name) like '%ANIMAL%'  
or upper(First_Name) like '% VC %'
or upper(First_Name) like '% VH %'
or upper(First_Name) like '% AC %'
or upper(First_Name) like '% AH %'
or UPPER(Last_Name) in ('OTC','OVER THE COUNTER','CLIENT','NEW CLIENT','TEST CLIENT','NONE GIVEN','PAPERCHART','TNC CLIENT','EMPLOYEE','NM','SEE ALERT','PAY AS GOES')  
or upper(Last_Name) like '%'||chr(42)||'%'  
or upper(Last_Name) like '%CLINIC%'   
or upper(Last_Name) like '%VETERINARY%' 
or upper(Last_Name) like '%HOSPITAL%' 
or upper(Last_Name) like '% VET %'
or upper(Last_Name) like '%ANIMAL%'   
or upper(Last_Name) like '% VC %' 
or upper(Last_Name) like '% VH %' 
or upper(Last_Name) like '% AC %' 
or upper(Last_Name) like '% AH %' 
or UPPER(Address1) in ('OTC','OVER THE COUNTER','RETURNED',' ','NONE GIVEN','PAPERCHART','TNC CLIENT','EMPLOYEE','NM','SEE ALERT','PAY AS GOES') 
or upper(Address1) like '%'||chr(42)||'%'   
or UPPER(Address2) in ('OTC','OVER THE COUNTER',
'CREDIT' , 'GRANDFATHER', 'NO MAIL','ACCOUNT' , 'DAD','INACTIVE',
'PARENTS','ALLERGIC' ,'DAUGHTER' ,'INCORRECT' , 'REFUSE' ,'BAD' , 'DEBT','INSUFFIENT' , 'REMINDER', 
'BANKRUPTCY' ,'DISCONNECTED' ,'INVALID' , 'RETURNED', 'BILLING' , 'DISCOUNT' , 'INVENTORY' , 'SEND',
'BROTHER' , 'DON''T' , 'INVOICE' , 'SISTER', 'CASH', 'EMERGENCY' , 'LAND LINE' , 'TEMPORARY' , 'CELL' , 'EMPLOYEE' , 'MAIL' , 'UNABLE' , 'CHANGE' , 'FATHER' , 'MESSAGE' ,
'BROTHERS','UNDELIVERABLE' , 'CHECK' , 'FAX' , 'MOM' , 'UPDATE' , 'COLLECTIONS' , 'FILE' , 'MOTHER' , 'VERIFY' ,   
'CONFIRM' , 'FORWARD' , 'NEED', 'WIFE' , 'HUSBAND' , 'DISCARDED')
or upper(Address2) like '%'||chr(33)||'%'   
or upper(Address2) like '%'||chr(42)||'%'   
or (UPPER(First_Name) = 'CASH' and UPPER(Last_Name) in ('CLIENT','CASH')))   
;


exception
when no_patients then 
  build_process_log('ERROR','This hid '||gv_input_hid||' location_number '||gv_input_location_number||' has no patients from the patient_ref table.','BUILD_LAST_DOSE_CLIENTS'); 
  raise; 
when others then   
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed :'||SQLERRM,'BUILD_LAST_DOSE_CLIENTS');
  raise; 
 


end;



procedure build_last_dose  as
pat_count number;  
fth_sales_count number; 
 
no_patients exception; 
no_fth_sales exception;
no_detail_records exception;
 
begin



--create table last_dose_sales as 
insert into last_dose_sales
select l.*,c.run_id 
from cooked.lineitem l 
inner join last_dose_patients c   on c.patient_id = l.patient_id and l.h_id = c.h_id  
and trunc(l.invoice_Date,'mm') between trunc(to_Date(c.start_date),'mm') and trunc(to_Date(c.end_date),'mm')
and l.voided in ('0','F','f') 
--and quantity_norm <> 0
and c.run_id = gv_input_run_id
;
gv_row_count := SQL%ROWCOUNT;

build_process_log('INFO','Finished '||gv_row_count||' last_dose_sales for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_LAST_DOSE');
gv_row_count := 0;

-- SAME TABLE IS USED FOR LAST DOSE AND CONSUMER 
-- THIS IS LAST DOSE
insert into consumer_FTH_PURCHASES  
select distinct l.LINEITEM_ID,  
l.CLIENT_ID,   
l.PATIENT_ID,  
l.SERVICE_MEDICATION_ID,
l.H_ID,  
l.REGION_ID,   
l.VI_SPECIES_ID,   
l.INVOICE_DATE,
l.QUANTITY,
l.COST,  
quantity_norm, 
l.NORM_RULE_ID,
l.UPDATE_RULE_ID,  
case when l.quantity_norm is null or l.cost_norm is null then 0 else l.cost_norm end cost_norm,   
l.ORIG_QUANTITY,   
l.ORIG_COST,   
l.DATE_NORMALIZED, 
l.DESCR_PACKAGE,   
l.PRICING_OVERRIDE,
sm.PRODUCT,
l.VOIDED,
pf.type, 
pf.family_name,
c.run_id,
c.end_date,
case when a.patient_id is null then 'N' else 'Y' end appointment_flag  
from last_dose_patients c  
inner join normalization.lineitem_norm_fth4 l on c.patient_id = l.patient_id and l.h_id = c.h_id  
inner join last_dose_sales n on l.lineitem_id = n.lineitem_id and l.service_medication_id = n.service_id and n.h_id = l.h_id  and n.run_id = c.run_id
inner join cooked.service_medication4 sm on sm.service_medication_id = l.service_medication_id and sm.hid = l.h_id
inner join reports.product_families pf on pf.product = sm.product
left join (select distinct hid,patient_id,trunc(appointment_date) appointment_date from vetstreet.pgsql_appointment) a on a.hid = l.h_id and a.patient_id = l.patient_id and a.appointment_date = l.invoice_Date 
where l.quantity_norm <> 0
and pf.family_name in ('Activyl','Acuguard','Adams','Advantage','Advantage Multi','Advantix','Assurity','Capstar','Certifect','ComboGuard','Comfortis','EasySpot','Frontline',   
'Heartgard','Interceptor','Iverhart Max','Iverhart Plus','Paradyne','Parastar','Preventic','Program','Proheart','ProMeris','QuadriGuard','Revolution','Sentinel','Seresto'
,'SimpleGuard','Trifexis','Tri-Heart','Tritak','Vectra','Vectra 3D','NexGard','Bravecto','Sentinel Spectrum','Effitix','Cheristin','Ecto Advance Plus','Effipro','Scalibor','Ecto Advance') 
and c.run_id = gv_input_run_id  
;
gv_row_count := SQL%ROWCOUNT;

 
 
--delete from consumer_FTH_PURCHASES where run_id = gv_input_run_id 
--and (invoice_Date >= (select distinct end_date from process_log 
--  where run_id = gv_input_Run_id and flag = 'RUNID') 
--  or invoice_Date < (select distinct start_date from process_log 
--where run_id = gv_input_Run_id and flag = 'RUNID'));
 
--delete from consumer_FTH_PURCHASES 
--where run_id = gv_input_run_id 
--and product = 'Certifect' 
--and trunc(invoice_Date,'mm') < '2011-Jul-01' 
--and  invoice_Date > (select distinct end_date from process_log where run_id = gv_input_Run_id and flag = 'RUNID');   

 
 
 
-- Check for FTH Sales
if gv_row_count  = 0 then
  raise no_FTH_SALES; 
end if;  
build_process_log('INFO','Finished '||gv_row_count||' FTH PURCHASES for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_LAST_DOSE');
gv_row_count := 0;
 
 
--create table last_dose_nsaid_purchases as
insert into last_dose_nsaid_purchases
select distinct run_id  
,lineitem_id   
,patient_id
,client_id 
,h_id
,invoice_Date  
,product 
,quantity_norm 
,dosage_volume_value_norm   
,cost 
,round(pills) pills
,appointment_flag  
 from  ( 
select   
a.run_id 
,a.lineitem_id 
,a.patient_id  
,a.client_id   
,a.h_id  
,invoice_Date  
,product 
,a.quantity_norm
,dosage_volume_value_norm   
,a.cost 
,pills   
,appointment_flag  
--,b.*   
,case when b.patient_id is not null and pills < 10 then 'exclude' end include_exclude
from 
(
select c.run_id, n.lineitem_id,n.patient_id,n.client_id,n.h_id,n.invoice_Date,n.product,decode(n.quantity_norm,null,1,0,1,n.quantity_norm) quantity_norm,dosage_volume_value_norm
, decode(cost_norm,null,n.cost,0,1,cost_norm) cost
, nvl(quantity_norm/decode(dosage_volume_value_norm,null,1,0,1,dosage_volume_value_norm),decode(n.quantity_norm,null,1,0,1,n.quantity_norm)) pills ,  
case when a.patient_id is null then 'N' else 'Y' end appointment_flag  
from last_dose_patients c  
inner join normalization.lineitem_norm_nsd4 n on c.patient_id = n.patient_id and n.h_id = c.h_id  
inner join last_dose_sales l on l.lineitem_id = n.lineitem_id and l.service_id = n.service_id and n.h_id = l.h_id  and c.run_id = l.run_id
inner join cooked.service_medication4 sm on sm.service_medication_id = n.service_id  and sm.hid = l.h_id
inner join last_dose_nsd_products p on sm.product = p.product
left join (select distinct hid,patient_id,trunc(appointment_date) appointment_date from vetstreet.pgsql_appointment) a on a.hid = n.h_id and a.patient_id = n.patient_id and a.appointment_date = n.invoice_Date 
where 1=1
--dosage_volume_value_norm <> 0 
--and dosage_volume_value_norm is not null  
--and quantity_norm <> 0
and n.cost <> 0 
and ((min_dvv <> 0 and quantity_norm is not null and quantity_norm <> 0) or (min_dvv = 0))
and c.run_id = gv_input_run_id
) a  
left join
(
select distinct c.run_id, l.h_id,l.patient_id--,l.cost,sg.family,l.invoice_Date
, l.invoice_Date-5 surgery_start, l.invoice_Date+5 surgery_end   
from cooked.service_general sg  
inner join last_dose_sales l on l.service_id = service_general_id
inner join last_dose_patients c on c.patient_id = l.patient_id and l.h_id = c.h_id and c.run_id = l.run_id
where sg.family in ( 'Surgery - general', 
'Surgery - internal', 
'Surgery - orthopedic', 
'Surgery - unknown',  
'Surgery - ocular & cosmetic/elective',   
'Surgery - reproductive',   
'Surgery - dental')
--and trunc(invoice_Date,'mm') = '01-nov-2014'  
and cost > 0   
and c.run_id = gv_input_run_id
--and l.h_id in (7033,6198,10815,11175,9880,9274,8061,9602,6884,8483,10746,524,618,1235,1669,2238,2305,2342,2562,2559,2385,2431)   
) b  
on a.patient_id = b.patient_id and a.invoice_date between b.surgery_start and b.surgery_end and a.run_id = b.run_id   
) c where 1=1  
and include_exclude is null 
order by 4,5;  
gv_row_count := SQL%ROWCOUNT ;

build_process_log('INFO','Finished '||gv_row_count||' NSD PURCHASES for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_LAST_DOSE');
gv_row_count := 0;
 
--delete from last_dose_nsaid_purchases where run_id = gv_input_run_id
--and (invoice_Date >= (select distinct end_date from process_log where run_id = gv_input_Run_id and flag = 'RUNID') 
--or invoice_Date < (select distinct start_date from process_log where run_id = gv_input_Run_id and flag = 'RUNID')); 
--gv_row_count := SQL%ROWCOUNT ;
-- 
--
--
--build_process_log('INFO','Finished deleting '||gv_row_count||' from NSD PURCHASES for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_LAST_DOSE');
-- 



insert into last_dose_dental_purch 
select distinct 
c.run_id,
l.LINEITEM_ID,
l.PATIENT_ID,
l.CLIENT_ID,
l.H_ID,
l.INVOICE_DATE,
sg.PRODUCT_info,
dental_doses(l.cost,l.quantity,l.service_id,l.patient_id) quantity, 
0 dosage_volume_value, 
case when l.quantity is null or l.cost is null then 0 else l.cost end cost, 
case when a.patient_id is null then 'N' else 'Y' end appointment_flag,
'DENTALHYGIENE' type
from last_dose_patients c 
inner join last_dose_sales l on c.patient_id = l.patient_id and l.h_id = c.h_id and l.run_id = c.run_id
inner join cooked.service_general sg on sg.service_general_id = l.service_id  and l.h_id = sg.hid
inner join dental_hyg_prod p on p.product = sg.product_info
left join (select distinct hid,patient_id,trunc(appointment_date) appointment_date from vetstreet.pgsql_appointment) a on a.hid = l.h_id and a.patient_id = l.patient_id and a.appointment_date = l.invoice_Date
where quantity <> 0 
and c.run_id = gv_input_run_id
;
gv_row_count := SQL%ROWCOUNT ;

build_process_log('INFO','Finished '||gv_row_count||' DENTAL PURCHASES for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_LAST_DOSE');
gv_row_count := 0;

 
insert into last_dose_report_detail
select distinct
'2DT' record_type  
,cast(case when pur.lineitem_id < 1 then power(2,32) + 1 + (pur.lineitem_id*-1) else pur.lineitem_id end as varchar2(11)) lineitem_id
,cast(c.client_id as varchar2(40)) client_id
,cast(c.first_name as varchar2(30)) first_name  
,cast(c.last_name as varchar2(30)) last_name --5
,cast(address1 as varchar2(40)) address1  
,cast(address2 as varchar2(40)) address2  
,cast(city as varchar2(30)) city
,cast(state as varchar2(2)) state 
,trim(CAST(lpad(substr((CASE WHEN REGEXP_LIKE(postal_code, '([^0-9])') THEN regexp_replace(postal_code, '([^0-9])', NULL, 1, 0, 'i') ELSE postal_code END),1,9),30) AS VARCHAR2(40))) postal_code
,case when regexp_count(primary_email, '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$',1,'i')>0 then primary_email end primary_email
,canines 
,felines 
,to_char(pur.invoice_date,'MM/DD/YYYY') invoice_date
,nvl(case when pur.type in ('fh','fth') then 'COMBO' when pur.type in ('f','ft') then 'FLEATICK' when pur.type = 'h' then 'HEARTWORM' end,type) product_category 
,pur.product family_name
,round(case when pur.type <> 'NSAID' then pur.quantity_norm else pills end,1) quantity_norm   
,trim(nvl(to_char(pur.cost_norm,'999990.00'),'0.00')) cost_norm  
,cast(pur.appointment_flag as varchar2(10)) appointment
,c.h_id  
,c.run_id run_id   
,c.start_date  
,c.end_date
,case when p.vi_species_id = 3 and product = 'Previcox' then pills else null end pills  
,case when floor(case when p.vi_species_id = 3 then age_years else null end) < 0 then 0 
when floor(case when p.vi_species_id = 3 then age_years else null end)  >99 then 99  
else floor(case when p.vi_species_id = 3 then age_years else null end) end 
age_years
,round(case when p.vi_species_id = 3 then p.weight else null end) weight   
,c.dogs_over_4 dogs_over_4  
from last_dose_clients  c  
inner join last_dose_patients p on p.client_id = c.client_id and p.run_id = c.run_id 
--left join fth_purchases fp on fp.client_id = c.client_id and fp.run_id = c.run_id and p.patient_id = fp.patient_id
--left join last_dose_nsaid_purchases np on np.client_id = c.client_id and np.run_id = c.run_id and np.patient_id = p.patient_id
left join (select  
RUN_ID,  
LINEITEM_ID,   
PATIENT_ID,
CLIENT_ID, 
H_ID,
INVOICE_DATE,  
family_name product,  
QUANTITY_NORM, 
0 dosage_volume_value_norm, 
COST_NORM, 
0 pills, 
APPOINTMENT_FLAG,  
type 
from consumer_FTH_PURCHASES 
union all
select RUN_ID, 
LINEITEM_ID,   
PATIENT_ID,
CLIENT_ID, 
H_ID,
INVOICE_DATE,  
product, 
QUANTITY_NORM, 
DOSAGE_VOLUME_VALUE_NORM,   
COST_NORM, 
decode(PILLS,0,1,pills) pills,   
APPOINTMENT_FLAG,  
'NSAID' type   
from last_dose_nsaid_purchases
union all
select 
RUN_ID,
LINEITEM_ID,
PATIENT_ID,
CLIENT_ID,
H_ID,
INVOICE_DATE,
PRODUCT_INFO,
QUANTITY,
DOSAGE_VOLUME_VALUE,
COST,
QUANTITY,
APPOINTMENT_FLAG,
TYPE
from last_dose_dental_purch
) pur on pur.client_id= c.client_id and pur.patient_id = p.patient_id and c.run_id = pur.run_id 
where c.run_id = gv_input_run_id
and (pur.client_id is not null ); 
gv_row_count := SQL%ROWCOUNT ;

if gv_row_count = 0 then 
raise no_detail_records;
end if;


insert into last_dose_report_header
columns(record_type
,lineitem_id   
,client_id 
,first_name
,last_name 
,address1
,address2
,city
,h_id
,run_id  
,start_Date
,end_Date
) select distinct  
'1HD'
,to_char(trunc(localtimestamp),'MM/DD/YYYY') todays_date 
,e.account_number --4 
,e.location_number 
,CAST(e.clinic_name AS VARCHAR2(40)) clinic_name
,update_flag + 1   
,gv_input_ccyear   
,'PH' -- ph for purchase historyt (last dose) PPH for previcox.  
,r.h_id  
,r.run_id
,r.start_date  
,r.end_date
from process_log r 
inner join enrollment e on e.location_number = r.location_number --and c.run_id = r.run_id   
where r.run_id = gv_input_run_id
and r.report_Type = 'Last Dose' 
and flag = 'RUNID';
 
 
insert into last_dose_report_trailer
columns(record_type
,lineitem_id   
,h_id
,run_id  
,start_Date
,end_Date
) select distinct  
'3TR'
,count(*) over(partition by r.run_id) + 2 
,r.h_id  
,r.run_id
,r.start_date  
,r.end_date
from process_log r 
inner join last_dose_report_detail e on e.run_id = r.run_id --and c.run_id = r.run_id  
where r.run_id = gv_input_run_id
and flag = 'RUNID' 
and r.report_Type = 'Last Dose' 
;
 
 
 
exception
when no_FTH_SALES then
  build_process_log('ERROR','This hid '||gv_input_hid||' location_number '||gv_input_location_number||' has no FTH SALES for the time period.','BUILD_LAST_DOSE');   
  raise; 
when no_detail_records then
  build_process_log('ERROR','This hid '||gv_input_hid||' location_number '||gv_input_location_number||' has no DETAIL RECORDS for the time period.','BUILD_LAST_DOSE');   
  raise; 
when others then   
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed :'||SQLERRM,'BUILD_LAST_DOSE');
  raise; 
 
 
 
 
 
 
 
end build_last_dose;   



procedure build_nic_list as
--newly inactive owners
begin
-- this must persist as a real table as it's called every month 
merge into last_dose_nic a using 
--insert into last_dose_nic (record_type,location_number,client_id, run_id, update_flag )
(select distinct  '2DT' record_type,d.location_number,d.client_id, gv_input_ccyear run_id, gv_input_update_flag update_flag from
(select * from last_dose_report_detail d 
  inner join 
    (
      select h_id,location_number,update_flag,max(run_id) run_id 
      from hid_log 
      where report_type = 'Last Dose' 
      and update_flag < gv_input_update_flag --current update flag
      and h_id = (Select h_id from process_log where flag = 'RUNID' and run_id = gv_input_run_id)--input_run_id
      group by h_id,location_number,update_flag
    ) h
  on h.run_id = d.run_id and canines > 0
) d
left join (
  select * from last_dose_clients c 
  where run_id = gv_input_run_id --input run_id
  and canines <> 0
) a on a.client_id = d.client_id
where a.client_id is null) b
on ( a.client_id = b.client_id and a.update_flag = b.update_flag and a.run_id = b.run_id)
when not matched then 
insert  (a.record_type,a.location_number,a.client_id, a.run_id, a.update_flag )
values(b.record_type,b.location_number,b.client_id, b.run_id, b.update_flag )
;

merge into last_dose_nic_report_header a
using (select distinct 
'1HD' RECORD_TYPE,
cast(to_char(cooked.log_process_status.convert_current_time,'mm/dd/yyyy') as varchar2(20)) creation_date,
gv_input_ccyear report_year,
gv_input_update_flag UPDATE_FLAG
from dual) b
on (a.report_year = b.report_year and a.update_flag =b.update_flag) 
when matched then
update set a.creation_date = b.creation_date
when not matched then
insert (a.RECORD_TYPE,a.creation_date,a.report_year,a.UPDATE_FLAG)
values(b.record_type ,b.creation_date,b.report_year,b.update_flag)
;


merge into last_dose_nic_report_trailer a
using (select distinct 
'3TR' RECORD_TYPE,
count(*)+2 record_count,
run_id report_year,
update_flag UPDATE_FLAG
from last_dose_nic 
where update_flag = gv_input_update_flag
and run_id = gv_input_ccyear
and record_type = '2DT'
group by run_id,update_flag) b
on (a.report_year = b.report_year and a.update_flag =b.update_flag) 
when matched then
update set a.record_count = b.record_count
when not matched then
insert (a.RECORD_TYPE,a.record_count,a.report_year,a.UPDATE_FLAG)
values(b.record_type ,b.record_count,b.report_year,b.update_flag)
;

null;
end;




procedure build_nsaid_purchase  as   
split_count number;
begin
 
 
 
select location_number into gv_input_location_number from process_log where flag = 'RUNID' and report_Type = 'NSAID' and run_id = gv_input_run_id;
 
--select count(*) into split_count from split_clients where location_number = gv_input_location_number and h_id = GV_INPUT_HID and rownum < 2;
 
 
build_process_log('STARTED','Running NSAID PURCHASE for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_NSAID_PURCHASE');
 
--insert into last_dose_pat_ref select pr.*, gv_input_run_id run_id from cooked.v_patient_ref pr where h_id = gv_input_hid;   
 
insert into nsaid_purch_patients 
select distinct first_active,last_active  
,p.is_active, p.vi_species_id, p.dob  
,p.patient_id, p.client_id, h_id
,gv_input_run_id   
,gv_input_start_date  
,gv_input_end_date 
,round(months_between(gv_input_end_date,dob)/12,3) age_years 
,case when months_between(gv_input_end_date,dob)/12 > 5 and vi_species_id = 3 then 1 else 0 end dogs_over_5 
,weight  
from cooked.patient_ref p left join (select distinct hid,patient_id,first_value(case when normalization.isnumeric(weight) = 1 then 
case when weight_unit = 'lbs' then to_number(weight)   
when upper(weight_unit) = upper('pounds') then to_number(weight)   
when upper(weight_unit) = upper('Ounces') then to_number(weight)/16
when upper(weight_unit) = upper('kilograms') then to_number(weight)*2.2 
when upper(weight_unit) = upper('grams') then (to_number(weight)/1000)*2.2  
else to_number(weight)  
end 
end) over(partition by patient_id order by visit_date desc) weight from vetstreet.pgsql_patient_vitals) a on p.patient_id = a.patient_id and p.h_id = a.hid 
where last_active > add_months(trunc(to_Date(gv_input_end_Date),'mm'),-23) 
and is_active = 1  
and is_deceased = 0
and vi_species_id in (3)
and p.patient_id > 0  
and h_id = gv_input_hid 
;
 
build_process_log('INFO','Finished running PATIENTS for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_NSAID_PURCHASE');
 
insert into nsaid_purch_clients  
select distinct c.client_id 
,nvl(regexp_replace(regexp_replace(c.first_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i'),'None Given') first_name 
,nvl(regexp_replace(regexp_replace(c.last_name, '\*|[[:digit:]]|do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i'),'None Given') last_name
,regexp_replace(c.title,'([[:cntrl:]]+)', null, 1,0,'i') prefix  
,nvl(regexp_replace(regexp_replace(a.address1, 'do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i'),'None Given') address1   
,regexp_replace(regexp_replace(a.address2, 'do not|delete|decease|service|wrong|moved|dead', null, 1,0,'i'),'([[:cntrl:]]+)', null, 1,0,'i') address2  
,regexp_replace(a.city,'([[:cntrl:]]+)', null, 1,0,'i') city 
,regexp_replace(a.state,'([[:cntrl:]]+)', null, 1,0,'i') state   
,regexp_replace(lpad(substr((case when regexp_like(a.postal_code, '([^0-9])') then regexp_replace(a.postal_code, '([^0-9])', null, 1, 0, 'i') else a.postal_code end),1,9),30),'([[:cntrl:]]+)', null, 1,0,'i') postal_code 
,c.h_id  
,p.dogs_over_5 
,p.run_id
,p.start_Date  
,p.end_Date
,p.canines 
,c.primary_email   
,regexp_replace(substr((case when regexp_like(a.phone1,'([^0-9])') then regexp_replace(a.phone1,'([^0-9])',null,1,0,'i') else a.phone1 end),1,10),'([[:cntrl:]]+)', null, 1,0,'i')  phone1 
,c.title 
,p.first_visit 
from cooked.client c  
inner join cooked.address a on a.client_id = c.client_id and a.address_type_id = 2   
inner join (select distinct client_id,h_id,run_id,start_date 
  ,max(dogs_over_5) over (partition by client_id,run_id) dogs_over_5   
  ,end_date 
  ,count(distinct case when vi_Species_id = 3 then patient_id end) over(partition by client_id,run_id) canines 
  ,min(first_active)  over(partition by client_id,run_id) first_visit  
  from nsaid_purch_patients) p   
on p.client_id = c.client_id
where p.run_id = gv_input_run_id
;
 
 
--remove clients without a first, last or address1  
delete from nsaid_purch_clients where run_id = gv_input_run_id and (first_name = 'None Given' or last_name = 'None Given' or address1 = ' ' or address1 = 'None Given' or address1 like '%*%' or address2 like '%*%') ;
delete from nsaid_purch_clients where run_id = gv_input_run_id and (state is null or city is null);
delete from nsaid_purch_clients where run_id = gv_input_run_id
and (upper(state) like 'AB%' or upper(state) like 'BC%' or upper(state) like 'MB%' or upper(state) like 'NB%' or upper(state) like 'NL%' or upper(state) like 'NT%' or upper(state) like 'NS%'   
or upper(state) like 'NU%' or upper(state) like 'ON%' or upper(state) like 'PE%' OR upper(state) LIKE 'QC%' or upper(state) like 'SK%' or upper(state) like 'YT%' or upper(state) like 'PR%' 
);   
 
delete from nsaid_purch_clients where (postal_code is null or postal_code like '%0000%' or postal_code = '000000' or postal_code = '0' or postal_code = '00' or postal_code = '000') and run_id = gv_input_run_id;  
 
delete from nsaid_purch_clients  
where run_id = gv_input_run_id  
and (UPPER(First_Name) in ('OTC','OVER THE COUNTER','NEW','NEW CLIENT','TEST')   
or UPPER(Last_Name) in ('OTC','OVER THE COUNTER','CLIENT','NEW CLIENT','TEST CLIENT') 
or UPPER(Address1) in ('OTC','OVER THE COUNTER') or UPPER(Address2) in ('OTC','OVER THE COUNTER')   
or (UPPER(First_Name) = 'CASH' and UPPER(Last_Name) in ('CLIENT','CASH')))   
;
 
 
 
build_process_log('INFO','Finished running CLIENTS for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_NSAID_PURCHASE'); 
 
insert into nsaid_purchases
select l.LINEITEM_ID, 
l.CLIENT_ID,   
l.PATIENT_ID,  
l.SERVICE_ID,  
l.H_ID,  
l.REGION_ID,   
l.VI_SPECIES_ID,   
l.INVOICE_DATE,
l.QUANTITY,
l.COST,  
l.quantity_norm/dosage_volume_value quantity_norm,  
l.NORM_RULE_ID,
case when l.quantity_norm is null or l.cost_norm is null then 0 else l.cost_norm end cost_norm,   
l.DATE_NORMALIZED, 
case when l.PRODUCT like 'Rimadyl%' then 'Rimadyl' else l.product end product,   
l.VOIDED,
c.run_id,
c.end_date,
round(c.weight) weight
from nsaid_purch_patients c
inner join normalization.lineitem_norm_nsd4 l on c.patient_id = l.patient_id and l.h_id = c.h_id  
where trunc(invoice_date,'mm') between trunc(to_Date(c.start_date),'mm') and trunc(to_Date(c.end_date),'mm')
--and (quantity_norm <> 0 or quantity_norm is null) 
and voided in ('0','F','f') 
and cost <> 0  
and product in ('Novox','Deramaxx','Previcox','Etogesic','Zubrin','Onsior','Vetprofen','Rimadyl Caplet','Rimadyl Chewable Tablet','Carprofen') 
and c.run_id = gv_input_run_id  
;
 
build_process_log('INFO','Finished running FTH PURCHASES for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_NSAID_PURCHASE');  
 
 
insert into nsaid_purch_report   
select distinct
'2DT' record_type  
,cast(case when lineitem_id < 1 then power(2,32) + 1 + (lineitem_id*-1) else lineitem_id end as varchar2(11)) lineitem_id
,cast(c.client_id as varchar2(40)) client_id
,cast(c.first_name as varchar2(30)) first_name  
,cast(c.last_name as varchar2(30)) last_name --5
,cast(address1 as varchar2(40)) address1  
,cast(address2 as varchar2(40)) address2  
,cast(city as varchar2(30)) city
,cast(state as varchar2(2)) state 
,trim(CAST(lpad(substr((CASE WHEN REGEXP_LIKE(postal_code, '([^0-9])') THEN regexp_replace(postal_code, '([^0-9])', NULL, 1, 0, 'i') ELSE postal_code END),1,9),30) AS VARCHAR2(40))) postal_code
,case when regexp_count(primary_email, '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$',1,'i')>0 then primary_email end primary_email
,canines 
,to_char(invoice_date,'MM/DD/YYYY') purchase_date   
,product 
,quantity_norm pills  
,trim(nvl(to_char(cost_norm,'999990.00'),'0.00')) purchase_price 
,nvl(fp.Weight,0) weight
,c.h_id  
,c.run_id run_id   
,c.start_date  
,c.end_date
from nsaid_purch_clients  c
inner join nsaid_purchases fp on fp.client_id = c.client_id and fp.run_id = c.run_id 
where c.run_id = gv_input_run_id; 
 
 
insert into nsaid_purch_report   
columns(record_type
,lineitem_id   
,client_id 
,first_name
,last_name 
,h_id
,run_id  
,start_Date
,end_Date
) select distinct  
'1HD'
,to_char(trunc(localtimestamp),'MM/DD/YYYY') todays_date 
,e.account_number --4 
,e.location_number 
,CAST(e.clinic_name AS VARCHAR2(40)) clinic_name
,r.h_id  
,r.run_id
,r.start_date  
,r.end_date
from process_log r 
inner join enrollment e on e.location_number = r.location_number --and c.run_id = r.run_id   
where r.run_id = gv_input_run_id
and r.report_Type = 'NSAID' 
and flag = 'RUNID';
 
 
insert into nsaid_purch_report   
columns(record_type
,lineitem_id   
,h_id
,run_id  
,start_Date
,end_Date
) select distinct  
'3TR'
,count(*) over(partition by r.run_id) + 1 
,r.h_id  
,r.run_id
,r.start_date  
,r.end_date
from process_log r 
inner join nsaid_purch_report e on e.run_id = r.run_id --and c.run_id = r.run_id
where r.run_id = gv_input_run_id
and r.report_Type = 'NSAID' 
and flag = 'RUNID' 
;
 
 
 
exception
when others then   
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed to init:'||SQLERRM,'BUILD_NSAID_PURCHASE'); 
  raise; 
 
 
 
 
 
 
 
end build_nsaid_purchase;  
 

 
PROCEDURE  build_hid_log as
-- this procedue is designed to be called once per RUNID when the run_clinic procedure is completed successfully  
-- It needs no direct inputs it gets all the input values from the global variables. 
-- when the record is written it's implicit of a successful run of the RUNID through the run_clinic function. 
Region_Run_Id Number; 
input_account_number number;
input_state varchar2(20);   
begin
 
region_run_id := 0 ;  
 
select distinct account_number into input_account_number from accounts where location_number = gv_input_location_number;  
select distinct state into input_state from cooked.address where address_type_id = 1 and h_id = gv_input_hid; 
 
insert into hid_log values(gv_input_hid,gv_input_end_date,gv_input_run_id,gv_input_location_number,localtimestamp,user,region_run_id,gv_db_run_from,input_account_number,gv_input_report_type,input_state,gv_input_update_flag,gv_input_Start_Date); 
commit;  
end; 
 
 
 
PROCEDURE build_process_log (input_flag in varchar2, input_message in varchar2,input_process in varchar2 ) as
-- this procedure is called mutlitple times through the running of various procedures and functions in this package.  
-- the run_id, flag, message, and process are all inputs.
-- INPUT_FLAG VALUES: 
-- STARTED - this is for the start of a function or procedure. There is only one for each time a procedure or function is called.
-- INFO - this is for an informational piece either at the end of a procedure that doesn't have a start, or along the way in a procedure to indicate steps. 
-- ERROR - this indicates the unsuccessful completion of a procedure or function. There is only one for each time a procedure or function is called.  
-- SUCCESS - this indicates the successful completion of a procedure or function. There is only one for each time a procedure or function is called.  
-- INPUT_MESSAGE: This is a lengthy text string with helpful information on the exact nature of the reason for the log.  
-- INPUT_PROCESS: This is the name of the procedure or function that wrote the log.  
 
  --PRAGMA AUTONOMOUS_TRANSACTION;
begin
 
insert into process_log values(gv_input_run_id,localtimestamp,user,input_flag ,input_message,input_process,gv_input_hid,gv_input_location_number,gv_input_end_date,gv_input_start_date,gv_input_report_type,gv_input_update_flag); 
 
commit;  
 
end; 



function last_dose_start (input_hid in number ,input_update_flag in number, input_end_date in date) return date as
 
PRAGMA AUTONOMOUS_TRANSACTION;  
 
last_run_id number;
new_start_date date;  
 
begin
 
select max(run_id) into last_run_id from hid_log h where h_id = input_hid and h.update_flag < input_update_flag  and report_type = 'Last Dose';   
if last_run_id is null or input_update_flag = 11 then  
  --raise no_previous_run;  
  return add_months(input_end_date,-12);  
  --gv_input_start_date := add_months(gv_input_end_Date,-12);
else 
  select max(to_date(invoice_Date,'mm/dd/yyyy'))+1 into new_start_date from last_dose_report_detail h where run_id = last_run_id and record_type = '2DT';
  return new_start_date;
end if;  
 
end last_dose_start;   
 
-- PPR CODE
 
PROCEDURE ppr_build_pat_master (start_adjustment in number, end_adjustment in number, time_period_name in varchar2) as
--prod_run_failed exception;
begin
 
--if start_adjustment is null or end_adjustment is null or time_period_name is null  
--  then RAISE gv_null_value;   
--  end if;
 
-- write to process log indicating the start of the procedure to populate the patient data for this RUNID   
-- this creates one row per patient in the patient master table. 
  build_process_log('STARTED','Running PATIENTS for time period '||time_period_name||' for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_PPR_PATIENT_MASTER'); 
  insert into ppr_patient_master 
  select gv_input_run_id
  ,time_period 
  ,patient_id  
  ,cast(h_id as number) 
  ,vi_species_id   
  ,month 
  ,''
  --,start_date
  --,end_date  
  -- 0's can not exist they must be nulled out the decode will be quicker than running update statements.   
  -- If we have 0's those without a FT products for example, will average in to the avg cost per dose.  
  ,case when ft_doses <= 0 then null else ft_doses end ft_doses  
  ,case when hw_doses <= 0 then null else hw_doses end hw_doses  
  ,case when ft_doses <= 0 then null else ft_rev end ft_rev  
  ,case when hw_doses <= 0 then null else hw_rev end hw_rev  
  -- can't use Decode here since we need a less than 0. So if someone buys 24 doses they will not take away from the oppotunity.   
  ,visits
  ,ft_visits   
  ,hw_visits   
  ,gv_input_end_date  
  ,sum(wellness_care) over(partition by patient_id,time_period) wellness_care
  from ( 
select 
time_period_name time_period
,l.patient_id  
,l.h_id
,vi_species_id 
,trunc(l.invoice_Date,'mm') month 
-- we have to generate 0's here beause you can't sum a null  
,sum(decode(f.type,'ft',f.quantity_norm,'fh',f.quantity_norm,'f',f.quantity_norm,0)) ft_doses -- total FT doses   
,sum(decode(f.type,'h',f.quantity_norm,'fh',f.quantity_norm,0)) hw_doses -- total FT doses
,round(sum(case when f.type in ('fh') then (f.cost_norm*.66) 
  when f.type in ('f','ft') then  f.cost_norm
   else 0 end)  -- total FTrev
,2) ft_rev -- total FT doses 2/3 of combo cost goes to FT
,round(sum(case when f.type in ('fh') then (f.cost_norm*.33) 
  when f.type in ('h') then f.cost_norm   
   else 0 end),2) hw_rev -- total FT doses 1/3 of combo cost goes to HW
-- This also assumes one fecal and one HW test per day   
,count(distinct l.invoice_date) visits
,count(distinct decode(f.type,'ft',f.invoice_date,'fh',f.invoice_date,'f',f.invoice_date)) ft_visits
,count(distinct decode(f.type,'h',f.invoice_date,'fh',f.invoice_date)) hw_visits 
,count(v.service_vaccine_id) wellness_care  
from cooked.lineitem l -- all transactions must be looked at to get total visits, and tests.  
inner join cooked.patient_ref p -- to link to pg_sql_pms_sepcies_lookup get vi_species_id 
on p.patient_id = l.patient_id
-- need a subquery here so we're only pulling the products we want from lineitem_norm 
left join (select f.invoice_date,f.lineitem_id,f.quantity_norm,f.cost_norm,  pf.type
 from normalization.lineitem_norm_fth4 f 
 inner join reports.product_families pf  
 on pf.product = f.product  
 --and pf.vi_species_id = f.vi_species_id
 where quantity_norm is not null  
   and f.norm_rule_id is not null 
   and quantity_norm <> 0   
   and cost_norm <> 0 and (voided in('0','f','F') or voided is null)) f
on l.lineitem_id = f.lineitem_id -- join on LI ID because we're only pulling normalized rows for FTH calculations.
left join psr_wellness v on v.service_vaccine_id = l.service_id and p.h_id = v.h_id and v.run_id = gv_input_run_id
where trunc(l.invoice_date,'mm') between add_months(gv_input_end_date,start_adjustment) and add_months(gv_input_end_date,end_adjustment) 
and l.h_id = gv_input_hid   
and (voided in('0','f','F') or voided is null)  
and vi_species_id in (3,7)  
  --  and ((v.service_vaccine_id is not null and l.cost > 0) -- if it's a test the cost must be > 0 
  --  )
 
group by l.patient_id,l.h_id,vi_species_id,trunc(l.invoice_Date,'mm')  
 
  )  
  ;  
 
 
delete from ppr_patient_master where (wellness_care is null or wellness_care = 0) and run_id = gv_input_run_id and time_period = time_period_name;
 
build_process_log('SUCCESS','Completed PATIENTS for time period '||time_period_name||' for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_PPR_PATIENT_MASTER'); 
 
 
 
-- PRODUCT_MASTER  
-- PRODUCT_MASTER  
-- PRODUCT_MASTER  
 
 
insert into ppr_product_master   
select   
 gv_input_run_id run_id 
--10000000 run_id  
,f.h_id  
--,input_time_period time_period
,time_period_name time_period   
,cast(add_months(gv_input_end_Date,start_adjustment) as date) start_date   
,cast(add_months(gv_input_end_Date,end_adjustment) as date) end_date   
--,cast(add_months('01-Mar-2014',-2) as date) start_date 
--,cast(add_months('01-Mar-2014',0) as date) end_date  
,decode(pr.type,'f','ft',type) type   
,family_name   
,p.vi_species_id   
,count(distinct p.patient_id) patients
,sum(f.quantity_norm) doses 
from normalization.lineitem_norm_fth4 f   
inner join cooked.patient_ref p on p.patient_id = f.patient_id   
-- to link to pg_sql_pms_sepcies_lookup get vi_species_id
inner join reports.product_families pr on pr.product = f.product 
inner join (select distinct run_id,patient_id,time_period from ppr_patient_master group by run_id,patient_id,time_period) pm on pm.patient_id= f.patient_id and pm.run_id= gv_input_run_id and pm.time_period = time_period_name 
where trunc(f.invoice_date,'mm') between add_months(gv_input_end_date,start_adjustment) and add_months(gv_input_end_date,end_adjustment) 
and f.h_id = gv_input_hid   
and (voided in('0','f','F') or voided is null)  
and p.vi_species_id in (3,7)
and quantity_norm is not null   
and f.norm_rule_id is not null  
and quantity_norm <> 0
and cost_norm <> 0 
group by f.h_id,p.vi_species_id,family_name,decode(pr.type,'f','ft',type)  
;
 
-- PRODUCT_MASTER  
-- PRODUCT_MASTER  
-- PRODUCT_MASTER  
 
 
 
 
build_process_log('SUCCESS','Completed PRODUCTS for time period '||time_period_name||' for HID '||gv_input_hid||' and '||gv_input_end_date||' end date.','BUILD_PPR_PATIENT_MASTER'); 
 
 
 
 
 
-- write to the process log to indicate the completion of the build patient master procedure. 
 
exception
when others then   
  rollback;
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed:'||SQLERRM,'BUILD_PPR_PATIENT_MASTER'); 
  raise; 
end; 



procedure ppr_build_report_data(input_run_id in number default null) as 
 
begin
 
if input_run_id is not null 
  then gv_input_run_id := input_run_id;   
end if;  
 
 
 
 
insert into ppr_t1_pre 
select distinct
run_id   
,end_date
,vi_species_id 
,'Flea/Tick' report_name
--,vi_species_id   
,pm.time_period
,count(distinct patient_id) over(partition by vi_species_id, run_id, time_period ) unique_patients
,count(distinct patient_id) over(partition by vi_species_id, run_id, time_period )-count(distinct decode(ft_doses,null,null,patient_id)) over(partition by vi_species_id, run_id, time_period ) unique_pat_no_prod   
,count(distinct decode(ft_doses,null,null,patient_id)) over(partition by vi_species_id, run_id, time_period ) unique_pat_with_hw   
,sum(ft_doses) over(partition by vi_species_id, run_id, time_period ) hw_rev 
,sum(ft_visits) over(partition by vi_species_id, run_id, time_period ) hw_trans  
,round(sum(ft_doses) over(partition by vi_species_id, run_id, time_period ) /sum(ft_visits) over(partition by vi_species_id, run_id, time_period ),2) prod_doses_per_trans
,sum(ft_rev) over(partition by vi_species_id, run_id, time_period ) ft_rev 
from ppr_patient_master pm 
where run_id = gv_input_run_id  
union all
select distinct
run_id   
,end_date
,vi_species_id 
,'Heartworm'   
--,vi_species_id   
,pm.time_period
,count(distinct patient_id) over(partition by vi_species_id, run_id, time_period )   
,count(distinct patient_id) over(partition by vi_species_id, run_id, time_period )-count(distinct decode(hw_doses,null,null,patient_id)) over(partition by vi_species_id, run_id, time_period ) unique_pat_no_prod   
,count(distinct decode(hw_doses,null,null,patient_id)) over(partition by vi_species_id, run_id, time_period ) unique_pat_with_hw   
,sum(hw_doses) over(partition by vi_species_id, run_id, time_period )  
,sum(hw_visits) over(partition by vi_species_id, run_id, time_period ) hw_trans  
,round(sum(hw_doses) over(partition by vi_species_id, run_id, time_period ) /sum(hw_visits) over(partition by vi_species_id, run_id, time_period ),2) prod_doses_per_trans
,sum(hw_rev) over(partition by vi_species_id, run_id, time_period )
from ppr_patient_master pm 
where run_id = gv_input_run_id  
;
 
build_process_log('SUCCESS','Completed inserting into table T1_PRE for run_id '||gv_input_run_id||' for HID '||gv_input_hid||'.','BUILD_REPORT_DATA'); 
 
 
insert into ppr_t1 
select distinct a.run_id
,a.end_date
,decode(a.report_number,3,'Canine','Feline') species
,a.report_name 
,case when column_order in (1,2,3) then 'Summary Totals: '||report_month_desc
else report_month_desc end report_month   
,column_order  
-- since each column in the final report is more like a row, meaning the column headers change with each report while the rows don't,
-- I thought it made more sence to format the data as each row of data was a column in the report. That way there's no confusion around  
-- column headers as they are supplied in the data. This caused me to have to put 2 different "types" of data into one column in the 
-- database. I had to put totals and % change in the same column, that is the reason for the case satement on each calcualted value. 
-- the case statement says if this is a certian column order number then calculate percent the difference between the value before this  
-- row and the value before the value before this row. 
,nvl(case when column_order not in (3,6,9,12) then nvl(unique_patients,0)  
else decode(lag(unique_patients,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,0, 
 decode(lag(unique_patients,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,-1,null,-1,
 round((lag(unique_patients,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) -  
 lag(unique_patients,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) )  
 / lag(unique_patients,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),3))) 
end,0) unique_patients
--,lag(unique_patients) over(partition by a.run_id, a.report_name, to_char(a.report_month,'Month'),decode(a.report_number,3,'Canine','Feline') order by column_order ) prev_value   
--,nvl(unique_pat_no_prod,0) unique_pat_no_prod 
 
,nvl(case when column_order not in (3,6,9,12) then nvl(unique_pat_no_prod,0) 
else decode(lag(unique_pat_no_prod,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,0,  
 decode(lag(unique_pat_no_prod,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,-1,null,-1,   
 round((lag(unique_pat_no_prod,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) - 
 lag(unique_pat_no_prod,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) )   
 / lag(unique_pat_no_prod,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),3))) 
end,0) unique_pat_no_prod   
 
,nvl(case when column_order not in (3,6,9,12) then nvl(unique_pat_with_hw,0) 
else decode(lag(unique_pat_with_hw,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,0,  
 decode(lag(unique_pat_with_hw,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,-1,null,-1,   
 round((lag(unique_pat_with_hw,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) - 
 lag(unique_pat_with_hw,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) )   
 / lag(unique_pat_with_hw,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),3))) 
end,0) unique_pat_with_prod 
 
,nvl(case when column_order not in (3,6,9,12) then nvl(prod_doses,0)   
else decode(lag(prod_doses,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,0, 
 decode(lag(prod_doses,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,-1,null,-1, 
 round((lag(prod_doses,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) -
 lag(prod_doses,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) ) 
 / lag(prod_doses,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),3)))  
end,0) prod_doses  
 
,nvl(case when column_order not in (3,6,9,12) then nvl(hw_trans,0) 
else decode(lag(hw_trans,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,0,   
 decode(lag(hw_trans,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,-1,null,-1,   
 round((lag(hw_trans,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) -  
 lag(hw_trans,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) )   
 / lag(hw_trans,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),3)))
end,0) trans   
 
,nvl(case when column_order not in (3,6,9,12) then nvl(prod_doses_per_trans,0)   
else decode(lag(prod_doses_per_trans,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,0,
 decode(lag(prod_doses_per_trans,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,-1,null,-1, 
 round((lag(prod_doses_per_trans,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) -   
 lag(prod_doses_per_trans,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) ) 
  / lag(prod_doses_per_trans,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),3)))
end,0) prod_doses_per_trans 
 
,nvl(case when column_order not in (3,6,9,12) then nvl(hw_rev,0) 
else decode(lag(hw_rev,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,0, 
 decode(lag(hw_rev,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),0,-1,null,-1, 
 round((lag(hw_rev,1) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) -
 lag(hw_rev,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ) ) 
 / lag(hw_rev,2) over(partition by a.run_id, a.report_name, a.report_number order by column_order ),3)))
end,0) rev 
 
,report_year_desc  
-- the left join here for the entire resulting data set to make sure there's a row for each column in the report. 
from (select run_id,report_name,report_number, to_char(report_month) report_month ,end_date, column_order,report_month_desc, report_year_desc
  from ppr_t1_reports  
 
  , (select run_id, report_month, hl.end_date, column_order,report_month_desc, report_year_desc from process_log hl inner join ppr_report_months rm on rm.end_date = hl.end_date   
  where run_id = gv_input_run_id and flag = 'RUNID') c   
  ) a
left join (
select distinct
run_id   
,end_date
, vi_species_id
,'Flea/Tick' report_name
,to_char(pm.month,'dd-Mon-yyyy') month
,count(distinct patient_id) over(partition by vi_species_id, run_id, pm.month ) unique_patients   
,count(distinct patient_id) over(partition by vi_species_id, run_id, pm.month )-count(distinct decode(ft_doses,null,null,patient_id)) over(partition by vi_species_id, run_id, pm.month ) unique_pat_no_prod   
,count(distinct decode(ft_doses,null,null,patient_id)) over(partition by vi_species_id, run_id, pm.month ) unique_pat_with_hw  
,sum(ft_doses) over(partition by vi_species_id, run_id, pm.month) prod_doses 
,sum(ft_visits) over(partition by vi_species_id, run_id, pm.month) hw_trans
,decode(sum(ft_visits) over(partition by vi_species_id, run_id, pm.month ),0,0,round(sum(ft_doses) over(partition by vi_species_id, run_id, pm.month )/sum(ft_visits) over(partition by vi_species_id, run_id, pm.month ),2)) prod_doses_per_trans
,sum(ft_rev) over(partition by vi_species_id, run_id, pm.month ) hw_rev
from ppr_patient_master pm 
where run_id = gv_input_run_id  
union all
select distinct
run_id   
,end_date
, vi_species_id
,'Heartworm' report_name
,to_char(pm.month,'dd-Mon-yyyy') month
,count(distinct patient_id) over(partition by vi_species_id, run_id, pm.month ) unique_patients   
,count(distinct patient_id) over(partition by vi_species_id, run_id, pm.month )-count(distinct decode(hw_doses,null,null,patient_id)) over(partition by vi_species_id, run_id, pm.month ) unique_pat_no_prod   
,count(distinct decode(hw_doses,null,null,patient_id)) over(partition by vi_species_id, run_id, pm.month ) unique_pat_with_prod
,sum(hw_doses) over(partition by vi_species_id, run_id, pm.month ) prod_doses
,sum(hw_visits) over(partition by vi_species_id, run_id, pm.month ) prod_trans   
,decode(sum(hw_visits) over(partition by vi_species_id, run_id, pm.month ),0,0,round(sum(hw_doses) over(partition by vi_species_id, run_id, pm.month )/sum(hw_visits) over(partition by vi_species_id, run_id, pm.month ),2)) prod_doses_per_trans
,sum(hw_rev) over(partition by vi_species_id, run_id, pm.month ) prod_rev  
from ppr_patient_master pm 
where run_id = gv_input_run_id  
union all
select run_id  
,end_date
,vi_species_id 
,report_name   
--,vi_species_id   
,to_char(time_period) 
--,row_number() over(partition by report_name, vi_species_id,run_id  order by time_period )   
,unique_patients   
--,lag(unique_patients) over(partition by run_id, report_name,decode(vi_species_id ,3,'Canine','Feline')  order by time_period )   
,unique_pat_no_prod
,unique_pat_with_hw
,hw_rev  
,hw_trans
,prod_doses_per_trans 
,ft_rev  
from (ppr_t1_pre)
where run_id = gv_input_run_id  
) b on a.run_id = b.run_id  
and a.report_name = b.report_name 
and a.report_number  = b.vi_species_id
and a.report_month = b.month
;
 
 
 
build_process_log('SUCCESS','Completed inserting into report table T1 for run_id '||gv_input_run_id||' for HID '||gv_input_hid||'.','BUILD_REPORT_DATA');  
 
 
insert into ppr_t2 
select   
a.run_id 
,a.species 
,a.report_name 
,a.report_year 
,a.doses_received  
,case when decode(a.report_name,'Heartworm',decode(a.species,'Canine',nvl(ac.dose_rec_hw_dog,0),nvl(ac.dose_rec_hw_cat,0))-a.doses_received,decode(a.species,'Canine',nvl(ac.dose_rec_ft_dog,0),nvl(ac.dose_rec_ft_cat,0))-a.doses_received) < 0
  then 0   
  else decode(a.report_name,'Heartworm',decode(a.species,'Canine',nvl(ac.dose_rec_hw_dog,0),nvl(ac.dose_rec_hw_cat,0))-a.doses_received,decode(a.species,'Canine',nvl(ac.dose_rec_ft_dog,0),nvl(ac.dose_rec_ft_cat,0))-a.doses_received) end missed_doses  
 
--,nvl(sum(hw_doses),0) hw_doses
,nvl(count(distinct patient_id),0)  unique_patients 
,nvl(sum(b.missed_doses),0)  total_missed_doses 
from (select r.*,hl.run_id from ppr_t2_reports r ,process_log hl where flag = 'RUNID' and run_id = gv_input_run_id  and trunc(HL.END_DATE,'yyyy') = r.end_Date) a   
inner join process_log hl on hl.run_id = a.run_id and hl.flag = 'RUNID' 
inner join ppr_accounts ac on ac.location_number = hl.location_number  and ac.end_date = hl.end_date and ac.hid = hl.h_id 
left join (
select   
pm.time_period 
,patient_id
,pm.run_id 
,decode(vi_species_id,3,'Canine','Feline') species  
,'Heartworm' report_name
,pm.end_date   
,count(distinct decode(hw_doses,null,null,patient_id)) unique_pat_with_hw  
,sum(nvl(hw_doses,0)) doses 
,case when decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0)) - sum(nvl(hw_doses,0)) < 0 then 0
  when decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0)) - sum(nvl(hw_doses,0)) > 
decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0)) then decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0))
 else  decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0)) - sum(nvl(hw_doses,0)) end missed_doses   
,sum(visits) total_visits   
,sum(hw_visits) hw_trans
from ppr_patient_master pm 
inner join process_log hl on hl.run_id = pm.run_id and hl.flag = 'RUNID'
inner join ppr_accounts a on a.location_number = hl.location_number  and a.end_date = hl.end_date and a.hid = hl.h_id 
where pm.run_id = gv_input_run_id 
group by pm.time_period,vi_species_id,patient_id,pm.run_id,pm.end_date,decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0))   
union
select   
pm.time_period 
,patient_id
,pm.run_id 
,decode(vi_species_id,3,'Canine','Feline')
,'Flea/Tick' report_name
,pm.end_date   
,count(distinct decode(ft_doses,null,null,patient_id)) 
,sum(nvl(ft_doses,0)) doses 
,case when decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0)) - sum(nvl(ft_doses,0)) < 0 then 0
  when decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0)) - sum(nvl(ft_doses,0)) > 
decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0)) then decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0))
 else  decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0)) - sum(nvl(ft_doses,0)) end missed_doses   
,sum(visits)   
,sum(ft_visits)
from ppr_patient_master pm 
inner join process_log hl on hl.run_id = pm.run_id and hl.flag = 'RUNID'
inner join ppr_accounts a on a.location_number = hl.location_number and a.end_date = hl.end_date and a.hid = hl.h_id
where pm.run_id = gv_input_run_id 
group by pm.time_period,vi_species_id,patient_id,pm.run_id,pm.end_date,decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0))   
) b  
on a.species = b.species
and a.report_name = b.report_name 
and a.report_year = substr(time_period,7,4) 
and a.doses_received = case when doses < 0 then 0 when doses <= 12 then doses else 13 end 
and a.run_id = b.run_id 
group by a.run_id  
,a.doses_received  
,a.report_year 
,a.report_name 
,a.species 
,case when decode(a.report_name,'Heartworm',decode(a.species,'Canine',nvl(ac.dose_rec_hw_dog,0),nvl(ac.dose_rec_hw_cat,0))-a.doses_received,decode(a.species,'Canine',nvl(ac.dose_rec_ft_dog,0),nvl(ac.dose_rec_ft_cat,0))-a.doses_received) < 0
  then 0   
  else decode(a.report_name,'Heartworm',decode(a.species,'Canine',nvl(ac.dose_rec_hw_dog,0),nvl(ac.dose_rec_hw_cat,0))-a.doses_received,decode(a.species,'Canine',nvl(ac.dose_rec_ft_dog,0),nvl(ac.dose_rec_ft_cat,0))-a.doses_received) end
;
 
 
build_process_log('SUCCESS','Completed inserting into report table T2 for run_id '||gv_input_run_id||' for HID '||gv_input_hid||'.','BUILD_REPORT_DATA');  
 
 
insert into ppr_t3 
select a.run_id
,a.time_period 
,a.report_time_period 
,a.doses 
,a.report_name 
,a.species 
,nvl(b.patients,0) total_patients 
,case when sum(nvl(percent_patients,0)) over (partition by a.run_id,a.time_period,a.report_name,a.species) <> 1 -- when sum is not 100%  
  and nvl(percent_patients,0) = max(nvl(percent_patients,0)) over(partition by a.run_id,a.time_period,a.report_name,a.species) 
  then nvl(percent_patients,0) + (1-sum(nvl(percent_patients,0)) over (partition by a.run_id,a.time_period,a.report_name,a.species)) 
  else nvl(percent_patients,0) end percent_patients
,order_number  
from 
(select pl.run_id,r.end_Date,r.time_period  ,r.report_time_period , d1.species, d1.report_name, d1.doses, order_number
  from ppr_t3_reports r
  inner join process_log pl on pl.end_date = r.end_date 
  inner join ppr_accounts ac on ac.location_number = pl.location_number and ac.end_date = pl.end_date and ac.hid = pl.h_id
  inner join ppr_t3_doses d1 on d1.rec_doses = nvl(ac.dose_rec_ft_dog,0) and d1.species = 'Canine' and d1.report_name = 'Flea/Tick' 
  where flag = 'RUNID'
  union  
  select pl.run_id,r.end_Date,r.time_period  ,r.report_time_period , d1.species, d1.report_name, d1.doses, order_number  
  from ppr_t3_reports r
  inner join process_log pl on pl.end_date = r.end_date 
  inner join ppr_accounts ac on ac.location_number = pl.location_number and ac.end_date = pl.end_date and ac.hid = pl.h_id
  inner join ppr_t3_doses d1 on d1.rec_doses = nvl(ac.dose_rec_hw_dog,0) and d1.species = 'Canine' and d1.report_name = 'Heartworm' 
  where flag = 'RUNID'
  union  
  select pl.run_id,r.end_Date,r.time_period ,r.report_time_period , d1.species, d1.report_name, d1.doses, order_number
  from ppr_t3_reports r
  inner join process_log pl on pl.end_date = r.end_date 
  inner join ppr_accounts ac on ac.location_number = pl.location_number and ac.end_date = pl.end_date and ac.hid = pl.h_id
  inner join ppr_t3_doses d1 on d1.rec_doses = nvl(ac.dose_rec_ft_cat,0) and d1.species = 'Feline' and d1.report_name = 'Flea/Tick' 
  where flag = 'RUNID'
  union  
  select pl.run_id,r.end_Date,r.time_period  ,r.report_time_period , d1.species, d1.report_name, d1.doses, order_number  
  from ppr_t3_reports r
  inner join process_log pl on pl.end_date = r.end_date 
  inner join ppr_accounts ac on ac.location_number = pl.location_number and ac.end_date = pl.end_date and ac.hid = pl.h_id
  inner join ppr_t3_doses d1 on d1.rec_doses = nvl(ac.dose_rec_hw_cat,0) and d1.species = 'Feline' and d1.report_name = 'Heartworm' 
  where flag = 'RUNID') a   
left join (
select distinct
run_id   
,end_date
,substr(time_period,7,4) time_period  
,species 
,report_name   
,case when doses = 0 then '0 Doses'   
  when recommended_doses = 0 and doses >= 1 then '0+ Doses'  
  when recommended_doses = 1 and doses >= 1 then '1+ Doses'  
  when recommended_doses > 1 and doses = 1 then '1 Dose' 
  when recommended_doses = 3 and doses = 2 then '2 Doses'
  when recommended_doses > 3 and doses between 2 and recommended_doses-1 then '2-'||cast(recommended_doses-1 as varchar2(2))||' Doses'   
  else cast(recommended_doses as varchar2(2)) ||'+ Doses'
  end doses
,count(distinct patient_id) over(partition by run_id,species,report_name,substr(time_period,7,4)  
  ,case when doses = 0 then '0 Doses' 
  when recommended_doses = 0 and doses >= 1 then '0+ Doses'  
  when recommended_doses = 1 and doses >= 1 then '1+ Doses'  
  when recommended_doses > 1 and doses = 1 then '1 Dose' 
  when recommended_doses = 3 and doses = 2 then '2 Doses'
  when recommended_doses > 3 and doses between 2 and recommended_doses-1 then '2-'||cast(recommended_doses-1 as varchar2(2))||' Doses'   
  else cast(recommended_doses as varchar2(2)) ||'+ Doses'
  end) patients
,round(count(distinct patient_id) over(partition by run_id,species,report_name,substr(time_period,7,4)  
  ,case when doses = 0 then '0 Doses' 
  when recommended_doses = 0 and doses >= 1 then '0+ Doses'  
  when recommended_doses = 1 and doses >= 1 then '1+ Doses'  
  when recommended_doses > 1 and doses = 1 then '1 Dose' 
  when recommended_doses = 3 and doses = 2 then '2 Doses'
  when recommended_doses > 3 and doses between 2 and recommended_doses-1 then '2-'||cast(recommended_doses-1 as varchar2(2))||' Doses'   
  else cast(recommended_doses as varchar2(2)) ||'+ Doses'
  end)/count(distinct patient_id) over(partition by run_id,substr(time_period,7,4),species,report_name),2) percent_patients
from (   
select   
time_period
,patient_id
,pm.run_id 
,decode(vi_species_id,3,'Canine','Feline') species  
,'Flea/Tick' report_name
,pm.end_date   
,sum(nvl(ft_doses,0)) doses 
,decode(vi_species_id,3,nvl(ac.dose_rec_ft_dog,0),nvl(ac.dose_rec_ft_cat,0))  recommended_doses   
from ppr_patient_master pm 
inner join process_log hl on hl.run_id = pm.run_id and hl.flag = 'RUNID'
inner join ppr_accounts ac on ac.location_number = hl.location_number  and ac.end_date = hl.end_date and ac.hid = hl.h_id 
group by pm.time_period,vi_species_id,patient_id,pm.run_id,pm.end_date,decode(vi_species_id,3,nvl(ac.dose_rec_ft_dog,0),nvl(ac.dose_rec_ft_cat,0)) 
 
union
 
select   
time_period
,patient_id
,pm.run_id 
,decode(vi_species_id,3,'Canine','Feline') species  
,'Heartworm' report_name
,pm.end_date   
,sum(nvl(hw_doses,0)) doses 
,decode(vi_species_id,3,nvl(ac.dose_rec_hw_dog,0),nvl(ac.dose_rec_hw_cat,0)) recommended_doses
from ppr_patient_master pm 
inner join process_log hl on hl.run_id = pm.run_id and hl.flag = 'RUNID'
inner join ppr_accounts ac on ac.location_number = hl.location_number and ac.end_date = hl.end_date and ac.hid = hl.h_id  
group by pm.time_period,vi_species_id,patient_id,pm.run_id,pm.end_date,decode(vi_species_id,3,nvl(ac.dose_rec_hw_dog,0),nvl(ac.dose_rec_hw_cat,0)) 
)
)
b on a.report_name = b.report_name
and a.run_id = b.run_id 
and a.species = b.species   
and a.time_period = b.time_period 
and a.doses = b.doses 
and a.end_date = b.end_date 
where a.run_id = gv_input_run_id
;
 
build_process_log('SUCCESS','Completed inserting into report table T3 for run_id '||gv_input_run_id||' for HID '||gv_input_hid||'.','BUILD_REPORT_DATA');  
 
 
insert into ppr_t4 
select   
a.run_id 
,a.end_Date
,a.time_period 
,a.species 
,a.report_name 
,a.report_time_period 
,nvl(count(distinct case when compliance = 'Non_compliant' then patient_id end),0) non_compliant_patients   
,nvl(sum(missed_doses),0) total_missed_doses
,nvl(round(sum(rev)/sum(doses),2),0) avg_price_per_dose
,nvl(round(sum(missed_doses)*(sum(rev)/sum(doses)),2),0) pot_addl_rev  
,nvl(sum(rev),0) actual_rev 
from (select pl.run_id, r.* from ppr_t4_reports r inner join process_log pl on r.end_date = pl.end_Date where flag = 'RUNID' ) a   
left join (
select   
substr(time_period,7,4) time_period   
,patient_id
,pm.run_id 
,decode(vi_species_id,3,'Canine','Feline') species  
,'Flea/Tick' report_name
,pm.end_date   
,decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0)) rec_doses 
,sum(nvl(ft_doses,0)) doses 
,case when decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0)) > sum(nvl(ft_doses,0)) then 'Non_compliant'   
 else 'compliant' 
 end compliance
,case when decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0)) - sum(nvl(ft_doses,0)) < 0 then 0
  when decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0)) - sum(nvl(ft_doses,0)) > 
decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0)) then decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0))
  else decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0)) - sum(nvl(ft_doses,0)) end missed_doses   
,sum(ft_rev) rev   
from ppr_patient_master pm 
inner join process_log hl on hl.run_id = pm.run_id and flag = 'RUNID' 
inner join ppr_accounts a on a.location_number = hl.location_number and a.end_date = hl.end_date and a.hid = hl.h_id
group by pm.time_period,vi_species_id,patient_id,pm.run_id,pm.end_date,decode(vi_species_id,3,nvl(a.dose_rec_ft_dog,0),nvl(a.dose_rec_ft_cat,0))   
union
select   
substr(time_period,7,4) time_period   
,patient_id
,pm.run_id 
,decode(vi_species_id,3,'Canine','Feline') species  
,'Heartworm' report_name
,pm.end_date   
,decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0)) rec_doses 
,sum(nvl(hw_doses,0)) doses 
,case when decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0)) > sum(nvl(hw_doses,0)) then 'Non_compliant'   
 else 'compliant' 
 end compliance
,case when decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0)) - sum(nvl(hw_doses,0)) < 0 then 0
  when decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0)) - sum(nvl(hw_doses,0)) > 
decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0)) then decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0))
  else decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0)) - sum(nvl(hw_doses,0)) end missed_doses   
,sum(hw_rev) rev   
from ppr_patient_master pm 
inner join process_log hl on hl.run_id = pm.run_id and flag = 'RUNID' 
inner join ppr_accounts a on a.location_number = hl.location_number and a.end_date = hl.end_date and a.hid = hl.h_id
group by pm.time_period,vi_species_id,patient_id,pm.run_id,pm.end_date,decode(vi_species_id,3,nvl(a.dose_rec_hw_dog,0),nvl(a.dose_rec_hw_cat,0))   
) b on a.run_id = b.run_id  
and a.time_period = b.time_period 
and a.report_name = b.report_name 
and a.species = b.species   
where a.run_id = gv_input_run_id
group by a.run_id  
,a.end_Date
,a.time_period 
,a.species 
,a.report_name 
,a.report_time_period;
 
build_process_log('SUCCESS','Completed inserting into report table T4 for run_id '||gv_input_run_id||' for HID '||gv_input_hid||'.','BUILD_REPORT_DATA');  
 
 
 
 
insert into ppr_t5 
select distinct run_id
,h_id
,location_number   
,end_Date
,report_name   
,species 
,case when family_name like 'Other%' then cast(family_name as nvarchar2(200)) else cast(upper(family_name) as nvarchar2(200))||unistr('\00AE') end product_name 
,row_order 
,case when sum(qty_pct) over(partition by run_id,report_name,species,time_period) > 1 and qty_rank = 1  
  then round(qty_pct,3)-(sum(qty_pct) over(partition by run_id,report_name,species,time_period)-1)  
  when sum(qty_pct) over(partition by run_id,report_name,species,time_period) < 1 and qty_rank = 1  
  then round(qty_pct,3)+(1-sum(qty_pct) over(partition by run_id,report_name,species,time_period))  
else sum(qty_pct) over(partition by run_id,species,report_name,family_name,time_period) end dose_pct
,time_period   
from (   
  --sum by new family name  
  select distinct run_id
  ,h_id  
  ,location_number 
  ,end_date
  ,report_name 
  ,species 
  ,family_name 
  ,time_period 
  ,sum(pct) over (partition by run_id,report_name,species,family_name,time_period) qty_pct
  ,min(row_order) over (partition by run_id,report_name,species,family_name,time_period) row_order
  ,min(qty_rank) over (partition by run_id,report_name,species,family_name,time_period) qty_rank  
  from ( 
  --second step gets the ranking while removing the 0 percentages. also convert the product name to other family  
  select run_id
  ,h_id
  ,location_number
  ,end_date
  ,report_name 
  ,species 
  ,time_period 
  ,case when dense_rank() over(partition by run_id,species,report_name,time_period order by decode(family_name,'Frontline',1,'Certifect',1,'Tritak',1,'NexGard',1,'Heartgard',1,2),qty desc,family_name ) >= 6 then 'Other '||report_name else family_name end family_name  
 
  --,round(Rev/total_rev,2) dose_pct  
  ,pct 
  ,case when dense_rank() over(partition by run_id,species,report_name,time_period order by decode(family_name,'Frontline',1,'Certifect',1,'Tritak',1,'NexGard',1,'Heartgard',1,2),qty desc,family_name ) > = 6 then 6  
  else dense_rank() over(partition by run_id,species,report_name,time_period order by decode(family_name,'Frontline',1,'Certifect',1,'Tritak',1,'NexGard',1,'Heartgard',1,2),qty desc,family_name ) end row_order 
  ,case when dense_rank() over(partition by run_id,species,report_name,time_period order by qty desc,family_name ) > = 6 then 6
  else dense_rank() over(partition by run_id,species,report_name,time_period order by qty desc,family_name ) end qty_rank
  from (   
--First step gets all the percentages for each individual products 
   select distinct s.run_id 
,s.h_id  
,s.location_number
,s.end_date 
,s.market report_name 
,s.species  
,family_name
,time_period
,sum(doses) over(partition by s.run_id,s.species,family_name,time_period) qty 
,sum(doses) over(partition by s.run_id,s.species,time_period) total_qty
,round(sum(doses) over(partition by s.run_id,s.species,family_name,time_period)/
   decode(sum(doses) over(partition by s.run_id,s.species,time_period),0,1,sum(doses) over(partition by s.run_id,s.species,time_period)),3) pct   
from 
(select * from ppr_species,(select * from process_log where flag = 'RUNID'  ),ppr_markets hl where market = 'Heartworm' ) s   
left join ppr_product_master pm on s.species = decode(pm.vi_Species_id,3,'Canine','Feline') and pm.run_id = s.run_id
left join ppr_accounts a on s.location_number = a.location_number and a.hid = s.h_id 
where --s.run_id = 13013 and
type in ('fh','h')
union
select distinct s.run_id
,s.h_id  
,s.location_number
,s.end_date 
,s.market report_name 
,s.species  
,family_name
,time_period
,sum(doses) over(partition by s.run_id,s.species,family_name,time_period) qty 
,sum(doses) over(partition by s.run_id,s.species,time_period) total_qty
,round(sum(doses) over(partition by s.run_id,s.species,family_name,time_period)/
  decode(sum(doses) over(partition by s.run_id,s.species,time_period),0,1,sum(doses) over(partition by s.run_id,s.species,time_period)),3) pct
from 
(select * from ppr_species,(select * from process_log where flag = 'RUNID'  ),ppr_markets hl where market = 'Flea/Tick' ) s   
left join ppr_product_master pm on s.species = decode(pm.vi_Species_id,3,'Canine','Feline') and pm.run_id = s.run_id
left join ppr_accounts a on s.location_number = a.location_number and a.hid = s.h_id 
where --s.run_id = 13013 and
type in ('fh','ft')   
 
  ) where pct > 0 
) --order by 5,6,row_order  
)
where run_id = gv_input_run_id  
--where 1=0
order by 1,5,6,8;  
 
 
build_process_log('SUCCESS','Completed inserting into report table T5 for run_id '||gv_input_run_id||' for HID '||gv_input_hid||'.','BUILD_REPORT_DATA');  
 
 
 
 
 
 
 
--delete from ppr_patient_master where run_id = gv_input_run_id;  
 
 
 
exception
when others then   
  rollback;
  build_process_log('ERROR','This HID '||gv_input_hid||' location_number '||gv_input_location_number||' failed:'||SQLERRM,'BUILD_T5');  
  raise; 
end; 
 
 
END cc;
/
