SELECT COUNT(*)FROM(SELECT DISTINCT Seller_ID from ITEMS WHERE Seller_ID IN (SELECT UserID FROM Users WHERE Rating > 1000));
