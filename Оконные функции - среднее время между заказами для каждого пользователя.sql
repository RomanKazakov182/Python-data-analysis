SELECT user_id
     , ROUND(AVG(EXTRACT(epoch FROM time_diff)) / 60 / 60)::INTEGER AS hours_between_orders
FROM
(
SELECT user_id
     , order_id
     , time
     , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY time) AS order_number
     , LAG(time, 1) OVER(PARTITION BY user_id ORDER BY time) AS time_lag
     , time - LAG(time, 1) OVER(PARTITION BY user_id ORDER BY time) AS time_diff
FROM user_actions
WHERE action = 'create_order'
    AND order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')
ORDER BY user_id, order_id
) AS t
WHERE order_number > 1
GROUP BY user_id
LIMIT 1000