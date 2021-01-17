WITH "wind_data_speed"("datetime", "wind_speed", "CityId") AS (
    SELECT w."datetime", w."wind_speed", w."CityId"
    FROM "wind_data" w JOIN "city_attributes" c USING("CityId")
    WHERE c."City"='Jerusalem'
    ) ,
"tms"("dt_datetime") AS (
    SELECT generate_series(MIN("datetime"), MAX("datetime"), INTERVAL '1 days')::DATE AS "dt_datetime"
    FROM "wind_data_speed"
    ),
"cta"("dt_datetime", "ct") AS (
    SELECT "datetime"::DATE AS "dt_datetime", AVG("wind_speed") AS "ct"
    FROM "wind_data_speed"
    GROUP BY 1
    ),
"wact" AS (
    SELECT *, AVG("ct") OVER(ORDER BY "dt_datetime" ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS "avg5days"
    FROM "tms" LEFT JOIN "cta" t USING ("dt_datetime")
    ),
"lines" AS (
    SELECT "datetime"::DATE AS "dt_datetime", "datetime" , "wind_speed"
    FROM  "wind_data_speed"
)
SELECT t."datetime", t."wind_speed"
FROM "wact" JOIN "lines" t USING("dt_datetime")
WHERE t."wind_speed" > "wact"."avg5days"
ORDER BY t."datetime";
