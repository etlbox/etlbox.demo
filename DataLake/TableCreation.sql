create table OrderDemo
(
    Id INT PRIMARY KEY IDENTITY(1,1),
    Number VARCHAR(10) NOT NULL,
    Details VARCHAR(200) NULL,
    Date DATETIME NOT NULL
)

INSERT INTO OrderDemo (Number, Details, Date) VALUES ('AB1000','1 umbrella', '2020-01-01')
INSERT INTO OrderDemo (Number, Details, Date) VALUES ('AB1011','1 shirt', '2020-01-01')
INSERT INTO OrderDemo (Number, Details, Date) VALUES ('AB1012','2 bags', '2020-01-01')
INSERT INTO OrderDemo (Number, Details, Date) VALUES ('AB1100','2 umbrellas', '2020-01-02')
INSERT INTO OrderDemo (Number, Details, Date) VALUES ('AB1101','3 clocks', '2020-01-02')

--SELECT Id, Number, Details, Date FROM Orders WHERE Id > -1 ORDER BY Date
--SELECT * FROM Orders
--SELECT Id, Number, Details, Date FROM Orders WHERE Date >= '1980-01-01'