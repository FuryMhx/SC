SELECT A.start_time as 时间, A.alarm_codes AS code, L.alarm_cn AS 内容, A.line_id as line_id, A.machine as machine, L.alarm_type as alarm_type
FROM tester_alarm A JOIN tester_alarm_list L 
ON A.alarm_codes = L.alarm_code 