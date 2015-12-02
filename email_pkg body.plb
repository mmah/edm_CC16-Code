create or replace package body                   email_pkg as

  Procedure Sp_Get_Project_Recipients(p_proj_name varchar2) AS
   
  begin
     SELECT --project,
            LTRIM(MAX(SYS_CONNECT_BY_PATH(email_addr,','))
            KEEP (DENSE_RANK LAST ORDER BY curr),',') 
       INTO g_email_addr_list
       FROM  (SELECT project, email_addr,
                     ROW_NUMBER() OVER (PARTITION BY project ORDER BY email_addr) AS curr,
                     ROW_NUMBER() OVER (PARTITION BY project ORDER BY email_addr) -1 AS prev
                FROM project_email_list
               WHERE active_ind = 'Y'
                 AND project = p_proj_name)
      GROUP BY project
    CONNECT BY prev = PRIOR curr AND project = PRIOR project
      START WITH curr = 1;

  end sp_get_project_recipients;
  -----------------------------------------------------
  PROCEDURE SP_PARSE(p_input varchar2) AS
  v_input varchar2(2000);
  v_start pls_integer;
  v_work_string varchar2(100);
  v_str1 varchar2(2000);
  v_str2 varchar2(2000);
  v_delimit char(1);

  BEGIN
    dbms_output.enable(1000000);
    v_input := trim(p_input);
    v_start := 1;

    --dbms_output.put_line('Starting String; '||v_input);

    -- figure out the delimiter - we only allow comma, pipe or space and only 1 kind at a time!
    IF (instr(trim(v_input),',') > 0) THEN
       v_delimit := ',';
    ELSIF (instr(trim(v_input),'|') > 0) THEN
       v_delimit := '|';
    ELSIF ((instr(trim(v_input),',') = 0)
       OR (instr(trim(v_input),'|') = 0))
      AND (instr(trim(v_input),' ') > 0) THEN
         v_delimit := ' ';
    END IF;

    --dbms_output.put_line('v_delimit = '||v_delimit);

    WHILE (length(v_input) > 0) LOOP
    IF (instr(v_input,v_delimit) > 0) THEN
       v_work_string := trim(substr(v_input,1,instr(v_input,v_delimit)-1));
      v_input := trim(substr(v_input,instr(v_input,v_delimit)+1, length(v_input)));

      v_str1 := substr(v_work_string,1,instr(v_work_string,',')-1);
      v_str2 := substr(v_work_string,instr(v_work_string,',')+1, length(v_work_string));
    ELSE
         v_work_string := substr(v_input,1,length(v_input));
      v_input := '';
      --now get the two strings
      v_str1 := substr(v_work_string,1,instr(v_work_string,',')-1);
      v_str2 := substr(v_work_string,instr(v_work_string,',')+1, length(v_work_string));
    END IF;

     dbms_output.put_line('Token = '||trim(v_str2));  --this is the individual parsed items.
     utl_smtp.rcpt(mail_conn, v_str2);
    END LOOP;
  END sp_parse;

----------------------------------------------------------------------------------------
  PROCEDURE  header(name varchar2, value varchar2) IS
  BEGIN
    utl_smtp.write_data(mail_conn, name||': '||value||utl_tcp.CRLF);
  END header;

----------------------------------------------------------------------------------------
PROCEDURE PR_SEND_EMAIL (sender    IN VARCHAR2,
                         recipient IN VARCHAR2,
                         cc_recip  IN VARCHAR2,
                         subject   IN VARCHAR2,
                         message   IN VARCHAR2,
                         emailtxt  IN VARCHAR2,
                         p_project IN VARCHAR2) AS

-- calling specification: ('name@xxx.com',
--                         'name@xxx.com',
--                         'name@xxx.com',
--                         'subject',
--                         'message',
--                         'text of email attachment',
--                         '');
-- The recipient and the cc_recip param may also be a delimited list of email addresses
-- which will be parsed by an in-line procedure.
-- The cc_recip param is optional.
-- The emailtxt param is optional.
--
-- Alternate - project based call:
-- calling specification: ('name@xxx.com',             ---sender 
--                         '',                         ---recipient
--                         '',                         ---cc_recipient
--                         'subject',                  ---subject
--                         'message',                  ---message
--                         'text of email attachment', ---message text for attachments only
--                         'ProjectName');             ---Project Name - only if sending to a project list

  --mailhost VARCHAR2(30) := 'mail.medimedia.com';
  --Mail_conn utl_smtp.connection;
  --crlf      varchar2(2) := chr(13)||chr(10);


BEGIN   --Main sp_send_email procedure here.

   if p_project is not null then
      sp_get_project_recipients(p_project);  --this creates to: list and puts it in gblvar: g_email_addr_list
   else
      g_email_addr_list := recipient;
   end if;
   
   mail_conn := utl_smtp.open_connection(mailhost, 25);
--   mail_conn := utl_smtp.open_connection(mailhost, 256);

   utl_smtp.helo(mail_conn, mailhost);
   utl_smtp.mail(mail_conn, sender);
--   sp_parse(recipient);
   sp_parse(g_email_addr_list);
   if (cc_recip is not null) then
        sp_parse(cc_recip);
   end if;
   
   utl_smtp.open_data(mail_conn);

   header('From','"'||sender||'" <'||sender||'>');
--   header('To','"'||recipient||'" <'||recipient||'>');
   header('To','"'||g_email_addr_list||'" <'||g_email_addr_list||'>');
   --header('To','"FC_IS_Support" <FC_IS_Support>');
   header('Subject', subject);
   if (cc_recip is not null) then
--         header('Cc','"'||cc_recip||'" <'||cc_recip||'>');
         header('Cc','"'||cc_recip||'"');
   end if;
   
--   header('Content-Type','text/plain');
   header('Content-Type','text/html');  --switch to html.

    if (emailtxt is not null) then
        utl_smtp.Write_Data(mail_conn, 'MIME-Version: 1.0'|| crlf ||
            'Content-Type: multipart/mixed;' || crlf ||
            ' boundary="-----SECBOUND"'|| crlf || crlf ||
            '-------SECBOUND' || CRLF ||
            'Content-Type: text/html;' || crlf ||
            'Content-Transfer_Encoding: 7bit' || crlf || crlf || 
            message ||
            crlf || crlf || 
            '-------SECBOUND' || crlf ||
           'Content-Type: text/plain;' || crlf ||
           ' name="Test.txt"' || crlf ||
           'Content-Transfer_Encoding: 8bit' || crlf ||
           'Content-Disposition: attachment;'|| crlf ||
           ' filename="test.txt"' || crlf || crlf ||
           emailtxt  || crlf ||
           --'Welcome to our attachment! Notice'|| crlf ||
           --'that we put a crlf in.' || crlf ||
           '-------SECBOUND--');
    else
--  original line below:
       utl_smtp.write_data(mail_conn, utl_tcp.crlf || message);
    end if;

   utl_smtp.close_data(mail_conn);
   utl_smtp.quit(mail_conn);

EXCEPTION
  WHEN utl_smtp.transient_error OR utl_smtp.permanent_error THEN
    utl_smtp.quit(mail_conn);
    raise_application_error(-20199,'Error sending mail: '||sqlerrm);
  WHEN Others THEN
    dbms_output.put_line(sqlerrm);
END pr_send_email;


end email_pkg;