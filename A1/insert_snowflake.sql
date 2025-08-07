-- Insert into DimDate
INSERT INTO DimDate (DateKey, Date, Day, MonthNumber, MonthName, Quarter, Year) VALUES
(1, '2024-01-01', 1, 1, 'January', 1, 2024),
(2, '2024-02-01', 1, 2, 'February', 1, 2024),
(3, '2024-03-01', 1, 3, 'March', 1, 2024),
(4, '2024-04-01', 1, 4, 'April', 2, 2024),
(5, '2024-05-01', 1, 5, 'May', 2, 2024),
(6, '2024-06-01', 1, 6, 'June', 2, 2024),
(7, '2024-07-01', 1, 7, 'July', 3, 2024),
(8, '2024-08-01', 1, 8, 'August', 3, 2024),
(9, '2024-09-01', 1, 9, 'September', 3, 2024),
(10, '2024-10-01', 1, 10, 'October', 4, 2024);

-- Insert into DimCity
INSERT INTO DimCity (CityKey, CityId, CityName) VALUES
(1, 'C001', 'New York'),
(2, 'C002', 'Los Angeles'),
(3, 'C003', 'Chicago'),
(4, 'C004', 'Houston'),
(5, 'C005', 'Phoenix'),
(6, 'C006', 'Philadelphia'),
(7, 'C007', 'San Antonio'),
(8, 'C008', 'San Diego'),
(9, 'C009', 'Dallas'),
(10, 'C010', 'San Jose');

-- Insert into DimCustomer
INSERT INTO DimCustomer (CustomerKey, CustomerId, CustomerName, CityKey) VALUES
(1, 'CU001', 'Alice Smith', 1),
(2, 'CU002', 'Bob Johnson', 2),
(3, 'CU003', 'Charlie Lee', 3),
(4, 'CU004', 'David Kim', 4),
(5, 'CU005', 'Eva Chen', 5),
(6, 'CU006', 'Frank Wright', 6),
(7, 'CU007', 'Grace Hall', 7),
(8, 'CU008', 'Hannah Lewis', 8),
(9, 'CU009', 'Ian Scott', 9),
(10, 'CU010', 'Julie Young', 10);

-- Insert into DimLine
INSERT INTO DimLine (LineKey, LineId, LineName) VALUES
(1, 'L001', 'Fiction'),
(2, 'L002', 'Non-Fiction'),
(3, 'L003', 'Science'),
(4, 'L004', 'History'),
(5, 'L005', 'Children'),
(6, 'L006', 'Romance'),
(7, 'L007', 'Mystery'),
(8, 'L008', 'Fantasy'),
(9, 'L009', 'Biography'),
(10, 'L010', 'Self-help');

-- Insert into DimAuthor
INSERT INTO DimAuthor (AuthorKey, AuthorId, AuthorName) VALUES
(1, 'A001', 'John Doe'),
(2, 'A002', 'Jane Smith'),
(3, 'A003', 'Mike Johnson'),
(4, 'A004', 'Linda Davis'),
(5, 'A005', 'Robert Brown'),
(6, 'A006', 'Emily Wilson'),
(7, 'A007', 'Michael Taylor'),
(8, 'A008', 'Jessica Moore'),
(9, 'A009', 'Thomas Anderson'),
(10, 'A010', 'Sarah Martinez');

-- Insert into DimPublisher
INSERT INTO DimPublisher (PublisherKey, PublisherId, PublisherName) VALUES
(1, 'P001', 'Penguin'),
(2, 'P002', 'HarperCollins'),
(3, 'P003', 'Simon & Schuster'),
(4, 'P004', 'Macmillan'),
(5, 'P005', 'Hachette'),
(6, 'P006', 'Random House'),
(7, 'P007', 'Scholastic'),
(8, 'P008', 'Pearson'),
(9, 'P009', 'Oxford Press'),
(10, 'P010', 'Cambridge Press');

-- Insert into DimProduct
INSERT INTO DimProduct (ProductKey, LineKey, AuthorKey, PublisherKey, ProductId, Title, SaleCost, SelePrice) VALUES
(1, 1, 1, 1, 'B001', 'Mystery Novel', 7.50, 15.00),
(2, 2, 2, 2, 'B002', 'History Book', 8.00, 20.00),
(3, 3, 3, 3, 'B003', 'Science Guide', 9.00, 25.00),
(4, 4, 4, 4, 'B004', 'Kids Story', 6.00, 12.00),
(5, 5, 5, 5, 'B005', 'Romantic Novel', 7.00, 14.00),
(6, 6, 6, 6, 'B006', 'Biography', 10.00, 22.00),
(7, 7, 7, 7, 'B007', 'Self-help Book', 8.50, 18.00),
(8, 8, 8, 8, 'B008', 'Fiction Bestseller', 12.00, 30.00),
(9, 9, 9, 9, 'B009', 'Fantasy Adventure', 11.00, 28.00),
(10, 10, 10, 10, 'B010', 'Educational Text', 15.00, 35.00);

-- Insert into FactBookSales
INSERT INTO FactBookSales (DateKey, CustomerKey, ProductKey, TransRef, SalePrice, SaleCost, Quantity, SalesAmount) VALUES
(1, 1, 1, 'TXN001', 15.00, 7.50, 1, 15.00),
(2, 2, 2, 'TXN002', 20.00, 8.00, 2, 40.00),
(3, 3, 3, 'TXN003', 25.00, 9.00, 1, 25.00),
(4, 4, 4, 'TXN004', 12.00, 6.00, 3, 36.00),
(5, 5, 5, 'TXN005', 14.00, 7.00, 1, 14.00),
(6, 6, 6, 'TXN006', 22.00, 10.00, 1, 22.00),
(7, 7, 7, 'TXN007', 18.00, 8.50, 2, 36.00),
(8, 8, 8, 'TXN008', 30.00, 12.00, 1, 30.00),
(9, 9, 9, 'TXN009', 28.00, 11.00, 1, 28.00),
(10, 10, 10, 'TXN010', 35.00, 15.00, 1, 35.00);
