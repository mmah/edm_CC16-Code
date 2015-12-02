create or replace package                   email_pkg as

--  mailhost            VARCHAR2(30) := 'mail.medimedia.com';
mailhost            VARCHAR2(30) := 'relay.vcaantech.com';
--  mailhost            VARCHAR2(30) := 'localhost';
    Mail_conn           utl_smtp.connection;
  crlf                varchar2(2) := chr(13)||chr(10);
  g_email_addr_list   varchar2(2000);

  Procedure sp_parse (p_input IN varchar2);
  Procedure header(name IN varchar2, value IN varchar2);
  Procedure PR_SEND_EMAIL (sender    IN VARCHAR2,
                           recipient IN VARCHAR2,
                           cc_recip  IN VARCHAR2,
                           subject   IN VARCHAR2,
                           message   IN VARCHAR2,
                           emailtxt  IN VARCHAR2,
                           p_project IN VARCHAR2);

end email_pkg;