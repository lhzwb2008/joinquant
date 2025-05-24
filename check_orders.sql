-- 查看今天的所有订单
SELECT * FROM `order`.`joinquant_stock` 
WHERE DATE(tradetime) = CURDATE()
ORDER BY insertdate DESC;

-- 查看未执行的订单
SELECT * FROM `order`.`joinquant_stock` 
WHERE if_deal = 0 AND DATE(tradetime) = CURDATE();

-- 查看已执行的订单
SELECT * FROM `order`.`joinquant_stock` 
WHERE if_deal = 1 AND DATE(tradetime) = CURDATE();

-- 统计今天的订单情况
SELECT 
    ordertype as '订单类型',
    COUNT(*) as '订单数量',
    SUM(CASE WHEN if_deal = 0 THEN 1 ELSE 0 END) as '未执行',
    SUM(CASE WHEN if_deal = 1 THEN 1 ELSE 0 END) as '已执行'
FROM `order`.`joinquant_stock`
WHERE DATE(tradetime) = CURDATE()
GROUP BY ordertype; 