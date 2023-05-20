import psycopg2
import pandas as pd


def getConn():
    pwFile = open("pw.txt", "r")
    pw = pwFile.read()
    pwFile.close()
    connStr = "host='cmpstudb-01.cmp.uea.ac.uk' \
               dbname= 'jqu22jmu' user='jqu22jmu' password = " + pw
    conn = psycopg2.connect(connStr)
    return conn


def clearOutput():
    with open("output.txt", "w") as clearfile:
        clearfile.write('')


def writeOutput(output):
    with open("output.txt", "a") as myfile:
        myfile.write(output)


try:
    conn = None
    conn = getConn()
    conn.autocommit = True
    cur = conn.cursor()

    f = open("input.txt", "r")
    clearOutput()
    for x in f:
        print(x)

        '''------------------------
           A-- INSERT A NEW BOOK --
           ------------------------'''

        if x[0] == 'A':
            raw = x.split("#", 1)
            raw[1] = raw[1].strip()
            data = raw[1].split("#")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "INSERT INTO book (bno, title, author, category, price) VALUES ('{}','{}','{}','{}','{}');".format(
                    data[0], data[1], data[2], data[3], data[4])
                writeOutput("TASK " + x[0] + "\n")
                cur.execute(sql)
            except Exception as e:
                writeOutput(str(e) + "\n")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "SELECT * FROM book WHERE bno = 234567"
                table_df = pd.read_sql_query(sql, conn)
                table_str = table_df.to_string()
                writeOutput(table_str + "\n")
            except Exception as e:
                print(e)

            '''-----------------------
               C--INSERT A CUSTOMER --
               -----------------------'''

        if x[0] == 'C':
            raw = x.split("#", 1)
            raw[1] = raw[1].strip()
            data = raw[1].split("#")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "INSERT INTO customer (cno, name, address) VALUES ('{}','{}','{}');".format(data[0], data[1], data[2],)
                writeOutput("TASK " + x[0] + "\n")
                cur.execute(sql)
            except Exception as e:
                writeOutput(str(e) + "\n")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "SELECT * FROM customer WHERE cno = 789212"
                table_df = pd.read_sql_query(sql, conn)
                table_str = table_df.to_string()
                writeOutput(table_str + "\n")
            except Exception as e:
                print(e)

            '''---------------------------------------------------------------------------------------------------------
            E--Place an order for a customer for a specified number of copies of a book.
             --Check if the book and customer is already in the system.
             --If the book entry or customer information is not available then create required entries and then perform 
             the operation.
            ---------------------------------------------------------------------------------------------------------'''

        if x[0] == 'E':
            raw = x.split("#", 1)
            raw[1] = raw[1].strip()
            data = raw[1].split("#")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "INSERT INTO bookOrder(cno, bno, qty) VALUES ('{}','{}','{}') ;".format(data[0], data[1], data[2])
                writeOutput("TASK " + x[0] + "\n")
                cur.execute(sql)
            except Exception as e:
                writeOutput(str(e) + "\n")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "SELECT * FROM bookOrder WHERE bno = 234567"
                table_df = pd.read_sql_query(sql, conn)
                table_str = table_df.to_string()
                writeOutput(table_str + "\n")
            except Exception as e:
                print(e)

            '''-------------------------------------------------------------------------------------------
           F -- Record a payment by a customer. The payment is subtracted from the customer's balance.
           -------------------------------------------------------------------------------------------'''

        if x[0] == 'F':
            raw = x.split("#", 1)
            raw[1] = raw[1].strip()
            data = raw[1].split("#")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "UPDATE customer SET balance = balance + ('{}') WHERE cno = ('{}') ;".format(data[1], data[0])
                writeOutput("TASK " + x[0] + "\n")
                cur.execute(sql)
            except Exception as e:
                writeOutput(str(e) + "\n")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "SELECT * FROM customer WHERE cno = 789212"
                table_df = pd.read_sql_query(sql, conn)
                table_str = table_df.to_string()
                writeOutput(table_str + "\n")
            except Exception as e:
                print(e)

            '''---------------------------------------------------------------------------------------------------------
           G--Find details of customers who have current orders for a book with a given text fragment in the book title.  
            --For example, find customers with orders for books with 'Python' in the title.
            --This transaction produces a report with lines showing the full title of a book ordered, the customer name 
            ---and the customer address relevant to the order. 
            ---The report is to be sorted by title and then by customer name
            ---------------------------------------------------------------------------------------------------------'''

        if x[0] == 'G':
            raw = x.split("#", 1)
            raw[1] = raw[1].strip()
            data = raw[1].split("#")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "SELECT book.title, customer.name, customer.address FROM customer INNER JOIN bookOrder ON " \
                      "customer.cno = bookOrder.cno INNER JOIN book ON book.bno = bookOrder.bno WHERE title LIKE ('%{}"\
                      "%') ORDER BY title, name;".format(data[0])
                writeOutput("TASK " + x[0] + "\n")
                cur.execute(sql)
                table_df = pd.read_sql_query(sql, conn)
                table_str = table_df.to_string()
                writeOutput(table_str + "\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

            '''---------------------------------------------------------------------------------------------------
            H--Find details of books ordered by a specified customer. 
            --The report will show the name of the customer followed by, for each book, the book number, title, 
            --and author, sorted by book number.
            ---------------------------------------------------------------------------------------------------'''

        if x[0] == 'H':
            raw = x.split("#", 1)
            raw[1] = raw[1].strip()
            data = raw[1].split("#")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "SELECT customer.name, book.bno, book.title, book.author FROM customer INNER JOIN bookOrder ON " \
                      "customer.cno = bookOrder.cno INNER JOIN book ON book.bno = bookOrder.bno WHERE customer.cno = " \
                      "('{}') ORDER BY bno;".format(data[0])
                writeOutput("TASK " + x[0] + "\n")
                cur.execute(sql)
                table_df = pd.read_sql_query(sql, conn)
                table_str = table_df.to_string()
                writeOutput(table_str + "\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

            '''---------------------------------------------------------------------------------------------------
            I--Produce a book report by category. 
            --This report shows, for each category, the number of books sold and the total value of these sales. 
            \--The total value calculation assumes that the currently held price is used and any earlier changes 
            --in the price of a book since it was inserted into the database are ignored. 
            ---------------------------------------------------------------------------------------------------'''

        if x[0] == 'I':
            raw = x.split("#", 1)
            raw[1] = raw[1].strip()
            data = raw[1].split("#")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "SELECT category, SUM(sales) as sum_sales_by_category, SUM(price) as " \
                      "value_sales_by_category from book GROUP BY category; "
                writeOutput("TASK " + x[0] + "\n")
                cur.execute(sql)
                table_df = pd.read_sql_query(sql, conn)
                table_str = table_df.to_string()
                writeOutput(table_str + "\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

            '''----------------------------------------------------------------------------
            J--Produce a customer report. 
            --This report shows, for each customer, the customer number, 
            --customer name, a count of the number of copies of books on order (if any). 
            --This report is to be in customer number order. 
            ----------------------------------------------------------------------------'''

        if x[0] == 'J':
            raw = x.split("#", 1)
            raw[1] = raw[1].strip()
            data = raw[1].split("#")
            try:
                cur.execute("SET SEARCH_PATH TO assignment, public;")
                sql = "SELECT customer.name, customer.cno, COUNT(book.sales) as " \
                      "count_sales FROM customer INNER JOIN bookOrder ON customer.cno = bookOrder.cno INNER JOIN book " \
                      "ON book.bno = bookOrder.bno GROUP BY customer.name, customer.cno ORDER BY customer.cno "
                writeOutput("TASK " + x[0] + "\n")
                cur.execute(sql)
                table_df = pd.read_sql_query(sql, conn)
                table_str = table_df.to_string()
                writeOutput(table_str + "\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

        elif x[0] == 'X':
            print("Exit {}".format(x[0]))
            writeOutput("\n\nExit program!")
except Exception as e:
    print(e)
