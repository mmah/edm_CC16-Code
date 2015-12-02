
create or replace procedure change_date_format as

begin
execute immediate 'alter session set NLS_DATE_FORMAT=''yyyy-mon-dd''';

end;
