SELECT concat('TOP',rank_num,' 메뉴') AS 구분
,CONCAT(MAX(CASE WHEN week_day = 0 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 0 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 0 then '건)' ELSE '' END)) AS 월 
,CONCAT(MAX(CASE WHEN week_day = 1 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 1 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 1 then '건)' ELSE '' END)) AS 화 
,CONCAT(MAX(CASE WHEN week_day = 2 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 2 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 2 then '건)' ELSE '' END)) AS 수 
,CONCAT(MAX(CASE WHEN week_day = 3 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 3 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 3 then '건)' ELSE '' END)) AS 목 
,CONCAT(MAX(CASE WHEN week_day = 4 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 4 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 4 then '건)' ELSE '' END)) AS 금 
,CONCAT(MAX(CASE WHEN week_day = 5 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 5 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 5 then '건)' ELSE '' END)) AS 토 
,CONCAT(MAX(CASE WHEN week_day = 6 then menu_nm ELSE '-' END),MAX(CASE WHEN week_day = 6 then CONCAT('(', CAST(cnt AS CHAR)) ELSE '' END),MAX(CASE WHEN week_day = 6 then '건)' ELSE '' END)) AS 일  
/*
SELECT concat('TOP',rank_num,' 메뉴') AS 구분
,MAX(CASE WHEN week_day = 0 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS 월 
,MAX(CASE WHEN week_day = 1 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS 화 
,MAX(CASE WHEN week_day = 2 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS 수 
,MAX(CASE WHEN week_day = 3 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS 목 
,MAX(CASE WHEN week_day = 4 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS 금 
,MAX(CASE WHEN week_day = 5 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS 토 
,MAX(CASE WHEN week_day = 6 then CONCAT(menu_nm, '(', CAST(cnt AS CHAR), ')') ELSE '-' END) AS 일
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