SELECT concat('TOP',rank_num,' �޴�') AS ����
,CONCAT(MAX(CASE WHEN week_day = 0 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 0 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 0 then '��)' ELSE '' END)) AS �� 
,CONCAT(MAX(CASE WHEN week_day = 1 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 1 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 1 then '��)' ELSE '' END)) AS ȭ 
,CONCAT(MAX(CASE WHEN week_day = 2 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 2 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 2 then '��)' ELSE '' END)) AS �� 
,CONCAT(MAX(CASE WHEN week_day = 3 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 3 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 3 then '��)' ELSE '' END)) AS �� 
,CONCAT(MAX(CASE WHEN week_day = 4 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 4 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 4 then '��)' ELSE '' END)) AS �� 
,CONCAT(MAX(CASE WHEN week_day = 5 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 5 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 5 then '��)' ELSE '' END)) AS �� 
,CONCAT(MAX(CASE WHEN week_day = 6 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 6 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 6 then '��)' ELSE '' END)) AS ��  
/*
SELECT concat('TOP',rank_num,' �޴�') AS ����
,MAX(CASE WHEN week_day = 0 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS �� 
,MAX(CASE WHEN week_day = 1 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS ȭ 
,MAX(CASE WHEN week_day = 2 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS �� 
,MAX(CASE WHEN week_day = 3 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS �� 
,MAX(CASE WHEN week_day = 4 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS �� 
,MAX(CASE WHEN week_day = 5 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS �� 
,MAX(CASE WHEN week_day = 6 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS ��
*/  
FROM 
(
	SELECT WEEKDAY(log_tktm) as week_day, CONVERT(menu_nm USING UTF8) AS menu_nm , COUNT(*)cnt
	,RANK() OVER(PARTITION BY WEEKDAY(log_tktm) ORDER BY COUNT(*) DESC,menu_nm) rank_num
	FROM menu_log
	WHERE menu_nm NOT IN ('login','logout')
	GROUP BY WEEKDAY(log_tktm), menu_nm
)T1
WHERE rank_num <= 10
GROUP BY rank_num
;