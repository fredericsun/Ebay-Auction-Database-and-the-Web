ELECT COUNT(DISTINCT Seller_ID) FROM Items INNER JOIN (SELECT DISTINCT BidderID FROM Bids) ON Items.Seller_ID = BidderID;
