SELECT Count(*) FROM (SELECT COUNT(category) numOfCategoriesOfItem FROM ItemCategories GROUP BY itemid) WHERE numOfCategoriesOfItem = 4;
