SELECT COUNT(*) FROM (SELECT COUNT (category) FROM ItemCategories WHERE itemid IN (SELECT BiddedItemID FROM Bids WHERE amount > 100 GROUP BY BiddedItemID) GROUP BY category);
