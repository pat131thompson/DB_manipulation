-- CREATE SCHEMA --

CREATE SCHEMA assignment

-- SET SEARCH PATH --

SET SEARCH_PATH TO assignment, PUBLIC

-- DDL TABLE book --

CREATE TABLE book (
	bno INTEGER PRIMARY KEY CHECK (bno BETWEEN 100000 AND 999999),
	title VARCHAR (255) NOT NULL CHECK (title <> ''),
	author VARCHAR (70) NOT NULL CHECK (author <> ''),
	category VARCHAR (10) NOT NULL CHECK (category IN ('Science', 'Leisure', 'Arts', 'Lifestyle')),
	price DECIMAL (6,2) NOT NULL,
	sales INTEGER DEFAULT 0
)
-- DDL TABLE customer --

CREATE TABLE customer (
	cno INTEGER PRIMARY KEY CHECK (cno BETWEEN 100000 AND 999999),
	name VARCHAR (70) NOT NULL CHECK (name <> ''),
	address VARCHAR (255) NOT NULL CHECK (address <> ''),
	balance DECIMAL (6,2) DEFAULT 0
)
-- DDL TABLE bookOrder --

CREATE TABLE bookOrder (
	cno INTEGER CHECK (cno BETWEEN 100000 AND 999999) REFERENCES customer(cno),
	bno INTEGER not null CHECK (bno BETWEEN 100000 AND 999999) REFERENCES book (bno),
	-- orderTime TIMESTAMP (6) DEFAULT CURRENT_TIMESTAMP,
	orderTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	qty INTEGER DEFAULT 0
)

-- TRIGGER FUNCTION TO UPDATE BOOK SALES --

CREATE FUNCTION update_book_sales()
	RETURNS TRIGGER
	LANGUAGE PLPGSQL
AS
$$
	BEGIN
		UPDATE book
		SET sales = sales + NEW.qty
		WHERE bno = NEW.bno;
		RETURN NEW;
	END;
	$$

-- FOLLOWED BY --

CREATE TRIGGER update_book_sales
	AFTER INSERT
	ON bookOrder
	FOR EACH ROW
	EXECUTE PROCEDURE update_book_sales()

-- TRIGGER FUNCTION TO UPDATE CUSTOMER BALANCE --

CREATE FUNCTION update_customer_balance()
	RETURNS TRIGGER
	LANGUAGE PLPGSQL
AS
$$
	BEGIN
		UPDATE customer
		SET balance = balance - NEW.qty * price FROM book
		WHERE bno = NEW.bno
		AND cno = NEW.cno; 							
		RETURN NEW;
	END;
	$$
	
-- FOLLOWED BY --

CREATE TRIGGER update_customer_balance
	AFTER INSERT
	ON bookOrder
	FOR EACH ROW
	EXECUTE PROCEDURE update_customer_balance();
	
	
	
	
	