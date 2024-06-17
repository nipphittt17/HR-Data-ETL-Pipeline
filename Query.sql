SELECT office, office_type, COUNT(employment_id) AS employment
FROM public."Employment"
GROUP BY office, office_type
ORDER BY employment DESC;

DROP VIEW IF EXISTS compensation_vs_salary;
CREATE VIEW compensation_vs_salary AS (
SELECT job_profile, 
	(SELECT job_title FROM public."JobProfile" WHERE job_profile = e.job_profile) AS job_title, 
	COUNT(employment_id) AS employments, 
	(SELECT compensation FROM public."JobProfile" WHERE job_profile = e.job_profile) AS compensation,
	MAX(salary) AS max_salary, MIN(salary) AS min_salary,
	ROUND(AVG(salary)::numeric, 2) AS avg_salary
FROM public."Employment" e
GROUP BY job_profile
ORDER BY employments DESC
);

SELECT * FROM compensation_vs_salary;
SELECT job_title, employments
FROM compensation_vs_salary
WHERE avg_salary > compensation;

SELECT job_title, employments
FROM compensation_vs_salary
WHERE avg_salary < compensation;

-- (CURRENT_DATE-Start_date)/365 