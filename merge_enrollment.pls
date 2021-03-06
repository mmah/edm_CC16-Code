create or replace procedure merge_enrollment as

begin


merge into enrollment a
using enrollment_temp b
on (a.location_number = b.location_number)
when matched then 
update set 
 a.ACCOUNT_NUMBER = b.ACCOUNT_NUMBER
, a.CLINIC_NAME = b.CLINIC_NAME
, a.CLINIC_ADDRESS_1 = b.CLINIC_ADDRESS_1
, a.CLINIC_ADDRESS_LINE_2 = b.CLINIC_ADDRESS_LINE_2
, a.CLINIC_CITY = b.CLINIC_CITY
, a.CLINIC_STATE = b.CLINIC_STATE
, a.CLINIC_POSTAL_CODE = b.CLINIC_POSTAL_CODE
, a.CLINIC_PHONE = b.CLINIC_PHONE
, a.CLINIC_CONTACT_FIRST_NAME = b.CLINIC_CONTACT_FIRST_NAME
, a.CLINIC_CONTACT_LAST_NAME = b.CLINIC_CONTACT_LAST_NAME
, a.CLINIC_CONTACT_PHONE = b.CLINIC_CONTACT_PHONE
, a.CLINIC_CONTACT_FAX = b.CLINIC_CONTACT_FAX
, a.CLINIC_CONTACT_EMAIL = b.CLINIC_CONTACT_EMAIL
, a.SOFTWARE = b.SOFTWARE
, a.DOSE_REC_HW_DOG = b.DOSE_REC_HW_DOG
, a.DOSE_REC_FT_DOG = b.DOSE_REC_FT_DOG
, a.DOSE_REC_HW_CAT = b.DOSE_REC_HW_CAT
, a.DOSE_REC_FT_CAT = b.DOSE_REC_FT_CAT
, a.SALES_REP_ID = b.SALES_REP_ID
, a.SALES_REP_NAME = b.SALES_REP_NAME
, a.ENROLLMENT_DATE = b.ENROLLMENT_DATE
, a.TERMS_AND_CONDITIONS_ACCEPTAN = b.TERMS_AND_CONDITIONS_ACCEPTAN
, a.CLINIC_STATUS = b.CLINIC_STATUS
, a.OFFERS = b.OFFERS
, a.LAST_REENROLLMENT_DATE = b.LAST_REENROLLMENT_DATE
, a.LAST_DOSE_OPT_IN_FLAG = b.LAST_DOSE_OPT_IN_FLAG
, a.TECH_CONTACT_FIRST_NAME = b.TECH_CONTACT_FIRST_NAME
, a.TECH_CONTACT_LAST_NAME = b.TECH_CONTACT_LAST_NAME
, a.TECH_CONTACT_PHONE_NUMBER = b.TECH_CONTACT_PHONE_NUMBER
, a.TECH_CONTACT_EMAIL = b.TECH_CONTACT_EMAIL
, a.CLINIC_CALL_STATUS = b.CLINIC_CALL_STATUS
, a.REFER_TO_REP_STATUS = b.REFER_TO_REP_STATUS
, a.CANCEL_STAT_SALES_REP_CTC = b.CANCEL_STAT_SALES_REP_CTC
, a.CANCEL_STAT_CLINIC_CTC = b.CANCEL_STAT_CLINIC_CTC
, a.CLINIC_STATUS_REPORT_LOADED = b.CLINIC_STATUS_REPORT_LOADED
, a.PROGRESS_REPORT_LOADED = b.PROGRESS_REPORT_LOADED
, a.CONSUMER_FILE_LOADED = b.CONSUMER_FILE_LOADED
, a.JAN_PURCH_HIST_FILE_LOADED = b.JAN_PURCH_HIST_FILE_LOADED
, a.FEB_PURCH_HIST_FILE_LOADED = b.FEB_PURCH_HIST_FILE_LOADED
, a.MAR_PURCH_HIST_FILE_LOADED = b.MAR_PURCH_HIST_FILE_LOADED
, a.APR_PURCH_HIST_FILE_LOADED = b.APR_PURCH_HIST_FILE_LOADED
, a.MAY_PURCH_HIST_FILE_LOADED = b.MAY_PURCH_HIST_FILE_LOADED
, a.JUN_PURCH_HIST_FILE_LOADED = b.JUN_PURCH_HIST_FILE_LOADED
, a.JUL_PURCH_HIST_FILE_LOADED = b.JUL_PURCH_HIST_FILE_LOADED
, a.AUG_PURCH_HIST_FILE_LOADED = b.AUG_PURCH_HIST_FILE_LOADED
, a.SEP_PURCH_HIST_FILE_LOADED = b.SEP_PURCH_HIST_FILE_LOADED
, a.OCT_PURCH_HIST_FILE_LOADED = b.OCT_PURCH_HIST_FILE_LOADED
, a.NOV_PURCH_HIST_FILE_LOADED = b.NOV_PURCH_HIST_FILE_LOADED
, a.DEC_PURCH_HIST_FILE_LOADED = b.DEC_PURCH_HIST_FILE_LOADED
, a.JAN_JUST_IN_TIME_MAILING_DATE = b.JAN_JUST_IN_TIME_MAILING_DATE
, a.FEB_JUST_IN_TIME_MAILING_DATE = b.FEB_JUST_IN_TIME_MAILING_DATE
, a.MAR_JUST_IN_TIME_MAILING_DATE = b.MAR_JUST_IN_TIME_MAILING_DATE
, a.APR_JUST_IN_TIME_MAILING_DATE = b.APR_JUST_IN_TIME_MAILING_DATE
, a.MAY_JUST_IN_TIME_MAILING_DATE = b.MAY_JUST_IN_TIME_MAILING_DATE
, a.JUN_JUST_IN_TIME_MAILING_DATE = b.JUN_JUST_IN_TIME_MAILING_DATE
, a.JUL_JUST_IN_TIME_MAILING_DATE = b.JUL_JUST_IN_TIME_MAILING_DATE
, a.AUG_JUST_IN_TIME_MAILING_DATE = b.AUG_JUST_IN_TIME_MAILING_DATE
, a.SEP_JUST_IN_TIME_MAILING_DATE = b.SEP_JUST_IN_TIME_MAILING_DATE
, a.OCT_JUST_IN_TIME_MAILING_DATE = b.OCT_JUST_IN_TIME_MAILING_DATE
, a.NOV_JUST_IN_TIME_MAILING_DATE = b.NOV_JUST_IN_TIME_MAILING_DATE
, a.DEC_JUST_IN_TIME_MAILING_DATE = b.DEC_JUST_IN_TIME_MAILING_DATE
, a.ONE_TIME_MAILING_DATE = b.ONE_TIME_MAILING_DATE
, a.ADDITIONAL_MAILINGS = b.ADDITIONAL_MAILINGS
, a.APPLET_INSTALLED = b.APPLET_INSTALLED
, a.CLINIC_PARTICIPATION_IN_CC = b.CLINIC_PARTICIPATION_IN_CC
, a.PREMIER_CINIC_OPT_EMAIL = b.PREMIER_CINIC_OPT_EMAIL
, a.PREMIER_CINIC_OPT_PREV = b.PREMIER_CINIC_OPT_PREV
, a.LAST_UPDATE = cooked.log_process_status.convert_current_time
, a.PREMIER_FLAG = b.PREMIER_FLAG
where a.ACCOUNT_NUMBER <> b.ACCOUNT_NUMBER
or a.CLINIC_NAME <> b.CLINIC_NAME
or a.CLINIC_ADDRESS_1 <> b.CLINIC_ADDRESS_1
or a.CLINIC_ADDRESS_LINE_2 <> b.CLINIC_ADDRESS_LINE_2
or a.CLINIC_CITY <> b.CLINIC_CITY
or a.CLINIC_STATE <> b.CLINIC_STATE
or a.CLINIC_POSTAL_CODE <> b.CLINIC_POSTAL_CODE
or a.CLINIC_PHONE <> b.CLINIC_PHONE
or a.CLINIC_CONTACT_FIRST_NAME <> b.CLINIC_CONTACT_FIRST_NAME
or a.CLINIC_CONTACT_LAST_NAME <> b.CLINIC_CONTACT_LAST_NAME
or a.CLINIC_CONTACT_PHONE <> b.CLINIC_CONTACT_PHONE
or a.CLINIC_CONTACT_FAX <> b.CLINIC_CONTACT_FAX
or a.CLINIC_CONTACT_EMAIL <> b.CLINIC_CONTACT_EMAIL
or a.SOFTWARE <> b.SOFTWARE
or a.DOSE_REC_HW_DOG <> b.DOSE_REC_HW_DOG
or a.DOSE_REC_FT_DOG <> b.DOSE_REC_FT_DOG
or a.DOSE_REC_HW_CAT <> b.DOSE_REC_HW_CAT
or a.DOSE_REC_FT_CAT <> b.DOSE_REC_FT_CAT
or a.SALES_REP_ID <> b.SALES_REP_ID
or a.SALES_REP_NAME <> b.SALES_REP_NAME
or a.ENROLLMENT_DATE <> b.ENROLLMENT_DATE
or a.TERMS_AND_CONDITIONS_ACCEPTAN <> b.TERMS_AND_CONDITIONS_ACCEPTAN
or a.CLINIC_STATUS <> b.CLINIC_STATUS
or a.OFFERS <> b.OFFERS
or a.LAST_REENROLLMENT_DATE <> b.LAST_REENROLLMENT_DATE
or a.LAST_DOSE_OPT_IN_FLAG <> b.LAST_DOSE_OPT_IN_FLAG
or a.TECH_CONTACT_FIRST_NAME <> b.TECH_CONTACT_FIRST_NAME
or a.TECH_CONTACT_LAST_NAME <> b.TECH_CONTACT_LAST_NAME
or a.TECH_CONTACT_PHONE_NUMBER <> b.TECH_CONTACT_PHONE_NUMBER
or a.TECH_CONTACT_EMAIL <> b.TECH_CONTACT_EMAIL
or a.CLINIC_CALL_STATUS <> b.CLINIC_CALL_STATUS
or a.REFER_TO_REP_STATUS <> b.REFER_TO_REP_STATUS
or a.CANCEL_STAT_SALES_REP_CTC <> b.CANCEL_STAT_SALES_REP_CTC
or a.CANCEL_STAT_CLINIC_CTC <> b.CANCEL_STAT_CLINIC_CTC
or a.CLINIC_STATUS_REPORT_LOADED <> b.CLINIC_STATUS_REPORT_LOADED
or a.PROGRESS_REPORT_LOADED <> b.PROGRESS_REPORT_LOADED
or a.CONSUMER_FILE_LOADED <> b.CONSUMER_FILE_LOADED
or a.JAN_PURCH_HIST_FILE_LOADED <> b.JAN_PURCH_HIST_FILE_LOADED
or a.FEB_PURCH_HIST_FILE_LOADED <> b.FEB_PURCH_HIST_FILE_LOADED
or a.MAR_PURCH_HIST_FILE_LOADED <> b.MAR_PURCH_HIST_FILE_LOADED
or a.APR_PURCH_HIST_FILE_LOADED <> b.APR_PURCH_HIST_FILE_LOADED
or a.MAY_PURCH_HIST_FILE_LOADED <> b.MAY_PURCH_HIST_FILE_LOADED
or a.JUN_PURCH_HIST_FILE_LOADED <> b.JUN_PURCH_HIST_FILE_LOADED
or a.JUL_PURCH_HIST_FILE_LOADED <> b.JUL_PURCH_HIST_FILE_LOADED
or a.AUG_PURCH_HIST_FILE_LOADED <> b.AUG_PURCH_HIST_FILE_LOADED
or a.SEP_PURCH_HIST_FILE_LOADED <> b.SEP_PURCH_HIST_FILE_LOADED
or a.OCT_PURCH_HIST_FILE_LOADED <> b.OCT_PURCH_HIST_FILE_LOADED
or a.NOV_PURCH_HIST_FILE_LOADED <> b.NOV_PURCH_HIST_FILE_LOADED
or a.DEC_PURCH_HIST_FILE_LOADED <> b.DEC_PURCH_HIST_FILE_LOADED
or a.JAN_JUST_IN_TIME_MAILING_DATE <> b.JAN_JUST_IN_TIME_MAILING_DATE
or a.FEB_JUST_IN_TIME_MAILING_DATE <> b.FEB_JUST_IN_TIME_MAILING_DATE
or a.MAR_JUST_IN_TIME_MAILING_DATE <> b.MAR_JUST_IN_TIME_MAILING_DATE
or a.APR_JUST_IN_TIME_MAILING_DATE <> b.APR_JUST_IN_TIME_MAILING_DATE
or a.MAY_JUST_IN_TIME_MAILING_DATE <> b.MAY_JUST_IN_TIME_MAILING_DATE
or a.JUN_JUST_IN_TIME_MAILING_DATE <> b.JUN_JUST_IN_TIME_MAILING_DATE
or a.JUL_JUST_IN_TIME_MAILING_DATE <> b.JUL_JUST_IN_TIME_MAILING_DATE
or a.AUG_JUST_IN_TIME_MAILING_DATE <> b.AUG_JUST_IN_TIME_MAILING_DATE
or a.SEP_JUST_IN_TIME_MAILING_DATE <> b.SEP_JUST_IN_TIME_MAILING_DATE
or a.OCT_JUST_IN_TIME_MAILING_DATE <> b.OCT_JUST_IN_TIME_MAILING_DATE
or a.NOV_JUST_IN_TIME_MAILING_DATE <> b.NOV_JUST_IN_TIME_MAILING_DATE
or a.DEC_JUST_IN_TIME_MAILING_DATE <> b.DEC_JUST_IN_TIME_MAILING_DATE
or a.ONE_TIME_MAILING_DATE <> b.ONE_TIME_MAILING_DATE
or a.ADDITIONAL_MAILINGS <> b.ADDITIONAL_MAILINGS
or a.APPLET_INSTALLED <> b.APPLET_INSTALLED
or a.CLINIC_PARTICIPATION_IN_CC <> b.CLINIC_PARTICIPATION_IN_CC
or a.PREMIER_CINIC_OPT_EMAIL <> b.PREMIER_CINIC_OPT_EMAIL
or a.PREMIER_CINIC_OPT_PREV <> b.PREMIER_CINIC_OPT_PREV
or a.PREMIER_FLAG <> b.PREMIER_FLAG
when not matched then 
insert (a.ACCOUNT_NUMBER,
a.LOCATION_NUMBER,
a.CLINIC_NAME,
a.CLINIC_ADDRESS_1,
a.CLINIC_ADDRESS_LINE_2,
a.CLINIC_CITY,
a.CLINIC_STATE,
a.CLINIC_POSTAL_CODE,
a.CLINIC_PHONE,
a.CLINIC_CONTACT_FIRST_NAME,
a.CLINIC_CONTACT_LAST_NAME,
a.CLINIC_CONTACT_PHONE,
a.CLINIC_CONTACT_FAX,
a.CLINIC_CONTACT_EMAIL,
a.SOFTWARE,
a.DOSE_REC_HW_DOG,
a.DOSE_REC_FT_DOG,
a.DOSE_REC_HW_CAT,
a.DOSE_REC_FT_CAT,
a.SALES_REP_ID,
a.SALES_REP_NAME,
a.ENROLLMENT_DATE,
a.TERMS_AND_CONDITIONS_ACCEPTAN,
a.CLINIC_STATUS,
a.OFFERS,
a.LAST_REENROLLMENT_DATE,
a.LAST_DOSE_OPT_IN_FLAG,
a.TECH_CONTACT_FIRST_NAME,
a.TECH_CONTACT_LAST_NAME,
a.TECH_CONTACT_PHONE_NUMBER,
a.TECH_CONTACT_EMAIL,
a.CLINIC_CALL_STATUS,
a.REFER_TO_REP_STATUS,
a.CANCEL_STAT_SALES_REP_CTC,
a.CANCEL_STAT_CLINIC_CTC,
a.CLINIC_STATUS_REPORT_LOADED,
a.PROGRESS_REPORT_LOADED,
a.CONSUMER_FILE_LOADED,
a.JAN_PURCH_HIST_FILE_LOADED,
a.FEB_PURCH_HIST_FILE_LOADED,
a.MAR_PURCH_HIST_FILE_LOADED,
a.APR_PURCH_HIST_FILE_LOADED,
a.MAY_PURCH_HIST_FILE_LOADED,
a.JUN_PURCH_HIST_FILE_LOADED,
a.JUL_PURCH_HIST_FILE_LOADED,
a.AUG_PURCH_HIST_FILE_LOADED,
a.SEP_PURCH_HIST_FILE_LOADED,
a.OCT_PURCH_HIST_FILE_LOADED,
a.NOV_PURCH_HIST_FILE_LOADED,
a.DEC_PURCH_HIST_FILE_LOADED,
a.JAN_JUST_IN_TIME_MAILING_DATE,
a.FEB_JUST_IN_TIME_MAILING_DATE,
a.MAR_JUST_IN_TIME_MAILING_DATE,
a.APR_JUST_IN_TIME_MAILING_DATE,
a.MAY_JUST_IN_TIME_MAILING_DATE,
a.JUN_JUST_IN_TIME_MAILING_DATE,
a.JUL_JUST_IN_TIME_MAILING_DATE,
a.AUG_JUST_IN_TIME_MAILING_DATE,
a.SEP_JUST_IN_TIME_MAILING_DATE,
a.OCT_JUST_IN_TIME_MAILING_DATE,
a.NOV_JUST_IN_TIME_MAILING_DATE,
a.DEC_JUST_IN_TIME_MAILING_DATE,
a.ONE_TIME_MAILING_DATE,
a.ADDITIONAL_MAILINGS,
a.APPLET_INSTALLED,
a.CLINIC_PARTICIPATION_IN_CC,
a.PREMIER_CINIC_OPT_EMAIL,
a.PREMIER_CINIC_OPT_PREV,
a.LAST_UPDATE,
a.PREMIER_FLAG)
values (b.ACCOUNT_NUMBER,
b.LOCATION_NUMBER,
b.CLINIC_NAME,
b.CLINIC_ADDRESS_1,
b.CLINIC_ADDRESS_LINE_2,
b.CLINIC_CITY,
b.CLINIC_STATE,
b.CLINIC_POSTAL_CODE,
b.CLINIC_PHONE,
b.CLINIC_CONTACT_FIRST_NAME,
b.CLINIC_CONTACT_LAST_NAME,
b.CLINIC_CONTACT_PHONE,
b.CLINIC_CONTACT_FAX,
b.CLINIC_CONTACT_EMAIL,
b.SOFTWARE,
b.DOSE_REC_HW_DOG,
b.DOSE_REC_FT_DOG,
b.DOSE_REC_HW_CAT,
b.DOSE_REC_FT_CAT,
b.SALES_REP_ID,
b.SALES_REP_NAME,
b.ENROLLMENT_DATE,
b.TERMS_AND_CONDITIONS_ACCEPTAN,
b.CLINIC_STATUS,
b.OFFERS,
b.LAST_REENROLLMENT_DATE,
b.LAST_DOSE_OPT_IN_FLAG,
b.TECH_CONTACT_FIRST_NAME,
b.TECH_CONTACT_LAST_NAME,
b.TECH_CONTACT_PHONE_NUMBER,
b.TECH_CONTACT_EMAIL,
b.CLINIC_CALL_STATUS,
b.REFER_TO_REP_STATUS,
b.CANCEL_STAT_SALES_REP_CTC,
b.CANCEL_STAT_CLINIC_CTC,
b.CLINIC_STATUS_REPORT_LOADED,
b.PROGRESS_REPORT_LOADED,
b.CONSUMER_FILE_LOADED,
b.JAN_PURCH_HIST_FILE_LOADED,
b.FEB_PURCH_HIST_FILE_LOADED,
b.MAR_PURCH_HIST_FILE_LOADED,
b.APR_PURCH_HIST_FILE_LOADED,
b.MAY_PURCH_HIST_FILE_LOADED,
b.JUN_PURCH_HIST_FILE_LOADED,
b.JUL_PURCH_HIST_FILE_LOADED,
b.AUG_PURCH_HIST_FILE_LOADED,
b.SEP_PURCH_HIST_FILE_LOADED,
b.OCT_PURCH_HIST_FILE_LOADED,
b.NOV_PURCH_HIST_FILE_LOADED,
b.DEC_PURCH_HIST_FILE_LOADED,
b.JAN_JUST_IN_TIME_MAILING_DATE,
b.FEB_JUST_IN_TIME_MAILING_DATE,
b.MAR_JUST_IN_TIME_MAILING_DATE,
b.APR_JUST_IN_TIME_MAILING_DATE,
b.MAY_JUST_IN_TIME_MAILING_DATE,
b.JUN_JUST_IN_TIME_MAILING_DATE,
b.JUL_JUST_IN_TIME_MAILING_DATE,
b.AUG_JUST_IN_TIME_MAILING_DATE,
b.SEP_JUST_IN_TIME_MAILING_DATE,
b.OCT_JUST_IN_TIME_MAILING_DATE,
b.NOV_JUST_IN_TIME_MAILING_DATE,
b.DEC_JUST_IN_TIME_MAILING_DATE,
b.ONE_TIME_MAILING_DATE,
b.ADDITIONAL_MAILINGS,
b.APPLET_INSTALLED,
b.CLINIC_PARTICIPATION_IN_CC,
b.PREMIER_CINIC_OPT_EMAIL,
b.PREMIER_CINIC_OPT_PREV,
cooked.log_process_status.convert_current_time,
b.PREMIER_FLAG)
;

end;