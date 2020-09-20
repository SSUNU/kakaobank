SELECT T1.usr_no AS 사용자번호
,T2.sex AS 성별
,CASE WHEN RIGHT(birthday,4) <= RIGHT('20200626',4) THEN CAST(LEFT('20200626',4) AS UNSIGNED) - CAST(LEFT(birthday,4) AS UNSIGNED) + 1 ELSE CAST(LEFT('20200626',4) AS UNSIGNED ) - CAST(LEFT(birthday,4) AS UNSIGNED ) END AS 나이
,T2.last_loc AS 지역명
,T2.pre_loc AS 이전지역명
,T2.mcco AS 이동통신사명
,T2.join_dt AS 가입일
,T1.max_visit_menu AS 최빈메뉴
,T1.last_menu AS 최근메뉴
FROM 
(
	SELECT a.usr_no
	,max(max_visit_menu) AS max_visit_menu
	,max(IF(a.log_tktm = b.last_log_dt,a.menu_nm, NULL)) AS last_menu
	FROM menu_log a
	JOIN  (
				SELECT usr_no
				,max(case when num = 1 then menu_nm ELSE NULL END) max_visit_menu
				,max(case when menu_nm <> 'logout' then log_tktm ELSE NULL END) AS last_log_dt
				FROM  (
							SELECT usr_no,menu_nm
							,ROW_NUMBER() OVER(PARTITION BY usr_no ORDER BY COUNT(*) DESC) num
							,max(log_tktm) log_tktm
							FROM menu_log
							WHERE menu_nm NOT IN ('login','logout')
							GROUP BY usr_no,menu_nm
						)b
				GROUP BY usr_no
			) b ON a.usr_no = b.usr_no
	GROUP BY usr_no
)T1
JOIN
(
	SELECT usr_no
	,CASE WHEN SUBSTRING(last_redt,7,1) IN (1,3,5,7,9) THEN '남' ELSE '여' END AS sex
	,CASE WHEN LEFT(last_redt,6) <= right(DATE_FORMAT(SYSDATE(),'%Y%m%d'),6) THEN CONCAT('20',LEFT(last_redt,6)) ELSE CONCAT('19',LEFT(last_redt,6)) END AS birthday
	,last_loc 
	,(SELECT loc_nm FROM usr_info_chg_log WHERE usr_no = b.usr_no AND log_tktm = b.pre_last_loc_dt) AS pre_loc
	,IFNULL(last_mcco,'-') AS mcco
	,left(first_tktm,8) AS join_dt
	FROM 
	(
		SELECT a.usr_no
		,first_tktm
		,max(CASE WHEN log_tktm = last_rsdt_dt THEN rsdt_no ELSE NULL END) AS last_redt
		,max(CASE WHEN log_tktm < last_loc_dt AND loc_nm <> '' THEN log_tktm ELSE NULL END) AS pre_last_loc_dt
		,max(CASE WHEN log_tktm = last_loc_dt THEN loc_nm ELSE NULL END) AS last_loc
		,max(CASE WHEN log_tktm = last_mcco_dt THEN mcco_nm ELSE NULL END) AS last_mcco
		FROM usr_info_chg_log a
		JOIN (
					SELECT usr_no
					,MIN(log_tktm)first_tktm
					,MAX(CASE WHEN rsdt_no <> '' THEN LOG_TKTM ELSE NULL END) AS last_rsdt_dt
					,MAX(CASE WHEN loc_nm <> '' THEN LOG_TKTM ELSE NULL END) AS last_loc_dt
					,MAX(CASE WHEN mcco_nm <> '' THEN LOG_TKTM ELSE NULL END) AS last_mcco_dt
					FROM usr_info_chg_log
					GROUP BY usr_no
				)b ON a.usr_no = b.usr_no
		GROUP BY a.usr_no, first_tktm
	)b
)T2 ON T1.usr_no = T2.usr_no
ORDER BY 1
;