-- Consulta 1: Total de órdenes por día
SELECT 
  date_trunc('day', CAST(datetime AS timestamp)) AS day,  -- Se extrae el día de cada registro
  SUM(num_orders) AS total_orders                         -- Se suman las órdenes por día
FROM raw
GROUP BY 1                                                -- Agrupa por el día
ORDER BY 1;                                               -- Orden cronológico



-- Consulta 2: Promedio de órdenes por hora del día
SELECT 
  hour(CAST(datetime AS timestamp)) AS hour_of_day,       -- Extrae la hora (0 a 23)
  AVG(num_orders) AS avg_orders                           -- Calcula el promedio de órdenes en cada hora
FROM raw
GROUP BY 1
ORDER BY 1;



-- Consulta 3: Total de órdenes por mes
SELECT 
  date_trunc('month', CAST(datetime AS timestamp)) AS month,  -- Extrae año y mes
  SUM(num_orders) AS total_orders                             -- Suma total mensual
FROM raw
GROUP BY 1
ORDER BY 1;



-- Consulta 4: Promedio de órdenes por día de la semana
SELECT 
  day_of_week(CAST(datetime AS timestamp)) AS day_of_week,   -- Devuelve 1 (domingo) a 7 (sábado)
  AVG(num_orders) AS avg_orders                               -- Calcula promedio por día de semana
FROM raw
GROUP BY 1
ORDER BY 1;



-- Consulta 5: Promedio de órdenes por hora y día de la semana
SELECT 
  day_of_week(CAST(datetime AS timestamp)) AS day_of_week,   -- Día de la semana
  hour(CAST(datetime AS timestamp)) AS hour_of_day,          -- Hora del día
  AVG(num_orders) AS avg_orders                              -- Promedio de órdenes
FROM raw
GROUP BY 1, 2
ORDER BY 1, 2;



-- Consulta 6: Diferencia de órdenes entre días consecutivos
WITH daily_orders AS (
  SELECT 
    date_trunc('day', CAST(datetime AS timestamp)) AS day,
    SUM(num_orders) AS total_orders
  FROM raw
  GROUP BY 1
)
SELECT 
  day,
  total_orders,
  total_orders - LAG(total_orders) OVER (ORDER BY day) AS diff_with_prev_day -- Cambio frente al día anterior
FROM daily_orders;



-- Consulta 7: Picos de actividad (más del 150% del promedio)
WITH daily_orders AS (
  SELECT 
    date_trunc('day', CAST(datetime AS timestamp)) AS day,
    SUM(num_orders) AS total_orders
  FROM raw
  GROUP BY 1
), stats AS (
  SELECT AVG(total_orders) AS avg_total FROM daily_orders
)
SELECT 
  d.day, d.total_orders
FROM daily_orders d
JOIN stats s ON 1=1
WHERE d.total_orders > 1.5 * s.avg_total   -- Filtra días con valores atípicos altos
ORDER BY d.total_orders DESC;



-- Consulta 8: Últimas 24 horas registradas
WITH last_day AS (
  SELECT MAX(CAST(datetime AS timestamp)) AS max_time FROM raw
)
SELECT 
  datetime,
  num_orders
FROM raw
WHERE CAST(datetime AS timestamp) > (
  SELECT max_time - INTERVAL '1' DAY FROM last_day
)
ORDER BY CAST(datetime AS timestamp);



-- Consulta 9: Top 10 horas con mayor número de órdenes
SELECT 
  datetime,
  num_orders,
  RANK() OVER (ORDER BY num_orders DESC) AS rank   -- Ranking por número de órdenes
FROM raw
ORDER BY num_orders DESC
LIMIT 10;



-- Consulta 10: Media móvil de 3 pasos anteriores
SELECT 
  datetime,
  num_orders,
  AVG(num_orders) OVER (
    ORDER BY CAST(datetime AS timestamp)
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING     -- Promedio de los últimos 3 registros anteriores
  ) AS rolling_avg_prev_3
FROM raw
ORDER BY CAST(datetime AS timestamp);

