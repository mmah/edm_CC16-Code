create or replace PACKAGE cc AS

  gv_db_run_from varchar2(10);
  gv_input_hid number;
  gv_input_end_date varchar2(11);
  gv_input_start_date date;
  gv_input_run_id number;
  gv_input_location_number number;
  gv_null_value exception;
  gv_storage_error exception;
  gv_input_report_type varchar2(20);
  gv_input_account_number number;
  gv_input_update_flag number;
  gv_input_ccyear number := 2016;
  gv_row_count number;


  

  /* TODO enter package declarations (types, exceptions, methods etc) here */ 
  -- Step A run CHECK_XREF to check a HID, Customer ID combination in the Xref. Not necessary sicne the next step also calls it.
  function CHECK_XREF (input_hid in number, input_location_number in number) return number;
  -- Step 1 run init, this checks the HID, location_number, and END_DATE to ensure they are valid. Then it assignes a RUNID
  FUNCTION init (input_hid in number, input_location_number in number, input_report_type in varchar, input_end_date in varchar2 default null, input_update_flag in number default null) return number ;
  -- Step 2 run run_clinic. This runs the RUNID specified.
  function run_clinic (input_run_id in number) return number;
  procedure build_wellness;
  PROCEDURE build_consumer;
  PROCEDURE build_process_log (input_flag in varchar2, input_message in varchar2,input_process in varchar2 );
  PROCEDURE build_hid_log;
  procedure build_psr;
  procedure psr_insert_month_master (input_run_id number);
  procedure build_last_dose_clients;
  procedure build_last_dose;
  procedure build_NSAID_PURCHASE;
  procedure build_psr_views (input_run_id in number);
  procedure build_control  ( input_run_id in number default null);
  function  last_dose_start (input_hid in number ,input_update_flag in number, input_end_date in date) return date; 
  procedure ppr_build_pat_master (start_adjustment in number, end_adjustment in number, time_period_name in varchar2);
  procedure ppr_build_report_data(input_run_id in number default null);
  
END cc;