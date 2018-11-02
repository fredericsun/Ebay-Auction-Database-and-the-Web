drop table if exists Items;
drop table if exists Categories;
drop table if exists ItemCategories;
drop table if exists Bids;
drop table if exists Users;

CREATE TABLE Items (
    ItemID         TINYTEXT     NOT NULL,
    Name           TEXT         NOT NULL,
    Seller_ID      TINYTEXT     NOT NULL,
    Buy_Price      FLOAT(16,2),
    Currently      FLOAT(16,2),
    First_Bid      FLOAT(16,2)  NOT NULL,
    Number_of_Bids INT,
    Started        DATETIME     NOT NULL,
    Ends           DATETIME     NOT NULL,
    Description    LONGTEXT,
    PRIMARY KEY (ItemID),
    FOREIGN KEY (Seller_ID) REFERENCES Users(UserID)
);

CREATE TABLE Categories (
    Category TINYTEXT NOT NULL,
    PRIMARY KEY (Category)
);

CREATE TABLE ItemCategories (
    itemid   TINYTEXT NOT NULL,
    category TINYTEXT NOT NULL,
    PRIMARY KEY (itemid, category),
    FOREIGN KEY (itemid)   REFERENCES Items(ItemID),
    FOREIGN KEY (category) REFERENCES Categories(Category)
);

CREATE TABLE Bids (
    BiddedItemID TINYTEXT    NOT NULL,
    BidderID     TINYTEXT    NOT NULL,
    Time         DATETIME    NOT NULL,
    Amount       FLOAT(16,2) NOT NULL,
    PRIMARY KEY (BiddedItemID, BidderID, Time),
    FOREIGN KEY (BiddedItemID) REFERENCES Items(ItemID),
    FOREIGN KEY (BidderID)     REFERENCES Users(UserID)
);

CREATE TABLE Users (
    UserID   TINYTEXT NOT NULL,
    Rating   INT      NOT NULL,
    Location TEXT,
    Country  TINYTEXT,
    PRIMARY KEY (UserID)
);
