SELECT main.post_menu AS 메뉴명, pre_menu AS 이전메뉴명 ,cnt AS 접근건수
,TRUNCATE(cnt/totcnt,2) AS "비율(%)"
FROM 
(
	SELECT pre.menu_nm AS pre_menu, post.menu_nm AS post_menu, COUNT(*) AS cnt 
	FROM 
	(
		SELECT T1.usr_no, T2.in_id AS session_id , T1.menu_nm
		,ROW_NUMBER() over(PARTITION BY T1.usr_no, T2.in_id ORDER BY T1.log_tktm) num
		FROM menu_log T1
		JOIN 	
			(
				SELECT a.usr_no,in_id,out_id
				FROM (
							SELECT usr_no, log_id AS in_id,ROW_NUMBER()over(ORDER BY usr_no, log_id)login_num 
							FROM menu_log
							WHERE menu_nm = 'login'
						)a
				JOIN (
							SELECT usr_no, log_id AS out_id,ROW_NUMBER()over(ORDER BY usr_no, log_id)logout_num 
							FROM menu_log 
							WHERE menu_nm = 'logout'
						)b ON a.login_num = b.logout_num
			)T2 ON T1.usr_no = T2.usr_no AND T1.log_id BETWEEN T2.in_id AND T2.out_id
	)pre
	JOIN
	(
		SELECT T1.usr_no, T2.in_id AS session_id , T1.menu_nm
		,ROW_NUMBER() over(PARTITION BY T1.usr_no, T2.in_id ORDER BY T1.log_tktm) num
		FROM menu_log T1
		JOIN 	
			(
				SELECT a.usr_no,in_id,out_id
				FROM (
							SELECT usr_no, log_id AS in_id,ROW_NUMBER()over(ORDER BY usr_no, log_id)login_num 
							FROM menu_log 
							WHERE menu_nm = 'login'
						)a
				JOIN (
							SELECT usr_no, log_id AS out_id,ROW_NUMBER()over(ORDER BY usr_no, log_id)logout_num 
							FROM menu_log 
							WHERE menu_nm = 'logout'
						)b ON a.login_num = b.logout_num
			)T2 ON T1.usr_no = T2.usr_no AND T1.log_id BETWEEN T2.in_id AND T2.out_id
	)post ON pre.usr_no = post.usr_no AND pre.session_id = post.session_id AND pre.num+1 = post.num
	GROUP BY pre.menu_nm,post.menu_nm
)main
JOIN 
(
	SELECT post.menu_nm AS post_menu, COUNT(*) AS totcnt 
	FROM 
	(
		SELECT T1.usr_no, T2.in_id AS session_id , T1.menu_nm
		,ROW_NUMBER() over(PARTITION BY T1.usr_no, T2.in_id ORDER BY T1.log_tktm) num
		FROM menu_log T1
		JOIN 	
			(
				SELECT a.usr_no,in_id,out_id
				FROM (
							SELECT usr_no, log_id AS in_id,ROW_NUMBER()over(ORDER BY usr_no, log_id)login_num 
							FROM menu_log 
							WHERE menu_nm = 'login'
						)a
				JOIN (
							SELECT usr_no, log_id AS out_id,ROW_NUMBER()over(ORDER BY usr_no, log_id)logout_num 
							FROM menu_log 
							WHERE menu_nm = 'logout'
						)b ON a.login_num = b.logout_num
			)T2 ON T1.usr_no = T2.usr_no AND T1.log_id BETWEEN T2.in_id AND T2.out_id
	)pre
	JOIN
	(
		SELECT T1.usr_no, T2.in_id AS session_id , T1.menu_nm
		,ROW_NUMBER() over(PARTITION BY T1.usr_no, T2.in_id ORDER BY T1.log_tktm) num
		FROM menu_log T1
		JOIN 	
			(
				SELECT a.usr_no,in_id,out_id
				FROM (
							SELECT usr_no, log_id AS in_id,ROW_NUMBER()over(ORDER BY usr_no, log_id)login_num 
							FROM menu_log 
							WHERE menu_nm = 'login'
						)a
				JOIN (
							SELECT usr_no, log_id AS out_id,ROW_NUMBER()over(ORDER BY usr_no, log_id)logout_num 
							FROM menu_log 
							WHERE menu_nm = 'logout'
						)b ON a.login_num = b.logout_num
			)T2 ON T1.usr_no = T2.usr_no AND T1.log_id BETWEEN T2.in_id AND T2.out_id
	)post ON pre.usr_no = post.usr_no AND pre.session_id = post.session_id AND pre.num+1 = post.num
	GROUP BY post.menu_nm
)tot ON main.post_menu = tot.post_menu
ORDER BY main.post_menu, cnt DESC, pre_menu
;