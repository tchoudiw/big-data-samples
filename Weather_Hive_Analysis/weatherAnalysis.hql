--Display current Weather
SELECT a.country, a.location.region, a.location.name, a.location.localtime, a.`current`.temp_c, a.`current`.temp_f, a.`current`.condition, a.`current`.wind_mph, a.`current`.wind_kph, a.`current`.wind_degree, a.`current`.wind_dir, a.`current`.pressure_mb, a.`current`.pressure_in, a.`current`.precip_mm, a.`current`.precip_in, a.`current`.humidity, a.`current`.cloud, a.`current`.feelslike_c, a.`current`.feelslike_f
FROM expl.weatherdata a
INNER JOIN 
(SELECT country, MAX(location.localtime) AS localtime
FROM expl.weatherdata 
GROUP BY country) b
ON b.country = a.country
AND b.localtime = a.location.localtime;

-- Display Weather trend

select country, 1.0*sum((x-xbar)*(y-ybar))/sum((x-xbar)*(x-xbar)) as Beta
from
(
    select country,
        avg(`current`.temp_c) over(partition by country) as ybar,
        `current`.temp_c as y,
        avg(datediff(to_date(location.localtime),to_date(current_timestamp))) over(partition by country) as xbar,
        datediff(to_date(location.localtime),to_date(current_timestamp)) as x
    from expl.weatherdata
    where hour(location.localtime) between 06 and 22	
) as Calcs
group by country;



