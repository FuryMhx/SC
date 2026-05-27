SELECT 内容, count(*) as cnt FROM printer_almlist
WHERE 时间::DATE = '2026-05-13' 
  AND printer_id = 'SPT2.1_printer02'
  AND (内容 ILIKE '%通讯超时%' OR 内容 ILIKE '% cotimeout%')
GROUP BY 内容
ORDER BY cnt DESC