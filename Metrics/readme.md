### 内部指标统计



-- 执行sql查询     滚动窗口 10秒    计算10秒窗口内用户点击次数
--         Table sqlQuery = tableEnv.sqlQuery("SELECT TUMBLE_END(proctime, INTERVAL '10' SECOND) as processtime,"
--         		+ "userId,count(*) as pvcount "
--         		+ "FROM Users "
--         		+ "GROUP BY TUMBLE(proctime, INTERVAL '10' SECOND), userId");




--  CREATE TABLE metric (
--     identity VARCHAR,
--     metric   VARCHAR,
--     dt      VARCHAR,
--     key      VARCHAR,
--     value    Double
-- ) WITH (
--     type='mysql',
--     url ='jdbc:mysql://master:3306/flink-test?characterEncoding=utf-8',
--     userName = 'hive',
--     password = '123456',
--     tableName = 'metric',
--     --schema = 'dtstack',
--     cache = 'LRU',
--     asyncPoolSize ='3'
-- );
