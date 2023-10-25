
from flask import Flask, render_template, Response, logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pdfkit
import os

app = Flask(__name__)

# Connect to MySQL Database with connection pooling
DATABASE_URL = "mysql+mysqlconnector://root:XXXXXXXXXXXXXXXXX@localhost/Sales_Project"
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)

# A utility function to run queries and fetch data
def fetch_data(query):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            data = result.fetchall()
    except SQLAlchemyError as e:
        app.logger.error("MySQL error: %s", e)
        data = []
    return data

@app.route("/")
def index():
    # Top 10 Query 1 top ten products and their marketing budget and the profit
    query1 = """SELECT `product_type`, SUM(`marketing`) as `totalmarketing`, SUM(`profit`) as `totalprofit`
                           FROM `business_data`
                            GROUP BY `product_type`
                             ORDER BY `totalmarketing` DESC
                              LIMIT 10;"""  
    data1 = fetch_data(query1)
    # Market - total profit and marketing expenses
    query2 = """SELECT market, SUM(profit) as totalprofit, SUM(marketing) as totalmarketing
                           FROM business_data
                            GROUP BY market;"""
    data2 = fetch_data(query2)
    # Correlation Between Total Expenses and Profit in Each Market Size
    query3 = """SELECT `market_size`,(COUNT(*) * SUM(`profit` * `total_expenses`) - SUM(`profit`) * SUM(`total_expenses`)) / 
                           (SQRT(COUNT(*) * SUM(`profit` * `profit`) - SUM(`profit`) * SUM(`profit`)) * SQRT(COUNT(*) * SUM(`total_expenses` * `total_expenses`) - SUM(`total_expenses`) * SUM(`total_expenses`)))
                            AS Correlation
                             FROM `business_data`
                              GROUP BY `market_size` HAVING COUNT(*) * SUM(`profit` * `profit`) - SUM(`profit`) * SUM(`profit`) > 0 AND 
                               COUNT(*) * SUM(`total_expenses` * `total_expenses`) - SUM(`total_expenses`) * SUM(`total_expenses`) > 0;
                                 """  
    data3 = fetch_data(query3)
    # Product Types That Have Seen Sales Growth Over Time and how much
    query4 = """SELECT `product_type`, (MAX(sales) - MIN(sales)) AS SalesGrowth
                            FROM (SELECT `product_type`, Date, SUM(sales) as sales
                             FROM business_data
                             GROUP BY `product_type`, Date)
                              as SubQuery7
                               GROUP BY `product_type` HAVING COUNT(sales) > 1 AND MAX(sales) > MIN(sales);
                                """
    data4 = fetch_data(query4)
    # States Where Actual COGS Are Different From 
        # Budget COGS and show by how much is the difference
    query5 = """SELECT state, ABS(actual_cogs - budget_cogs) AS difference
                           FROM (SELECT state, SUM(cogs) as actual_cogs, SUM(budget_cogs) as budget_cogs
                            FROM business_data
                             GROUP BY state) 
                              as subquery6
                               WHERE actual_cogs != budget_cogs;"""  
    data5 = fetch_data(query5)
    # Average Inventory for Each Product Type in Each State
    query6 = """SELECT state, product_type, ROUND(AVG(inventory), 2) as avg_inventory
                           FROM business_data
                            GROUP BY state, product_type;"""
    data6 = fetch_data(query6)
    # States with Highest Sales
    query7 = """SELECT state, MAX(sales) as MaxSales
                           FROM (SELECT state, market, SUM(sales) as sales
                            FROM business_data
                             GROUP BY state, market) as sub_query_3
                              GROUP BY state
                               ORDER BY MaxSales DESC
                                LIMIT 5;"""  
    data7 = fetch_data(query7)
    # Highest selling product in each market and what is the profit and Total Expenses
    query8 = """SELECT market, MAX(product) as product,  
                           MAX(sales) as HighestSales,
                            SUM(profit) as total_profit,  
                             SUM(total_expenses) as total_expenses_sum  
                              FROM ( SELECT market, product,
                                SUM(sales) as sales,
                                 SUM(profit) as profit,
                                  SUM(total_expenses) as total_expenses
                                   FROM business_data
                                    GROUP BY market, product) AS sub_query
                                     GROUP BY market;"""
    data8 = fetch_data(query8)
    # Most Profitable Markets in Each State       
    query9 = """SELECT state, MAX(profit) as MaxProfit
                           FROM ( SELECT state, market, 
                            SUM(profit) as profit
                             FROM business_data
                              GROUP BY state, market) AS sub_query1
                               GROUP BY state;"""
    data9 = fetch_data(query9)
    # Least Profitable Markets in Each State
    query10 = """SELECT `state`, MIN(`profit`) as `MinProfit`
                        FROM (
                         SELECT `state`, `market`, SUM(`profit`) as `profit`
                          FROM `business_data`
                           GROUP BY `state`, `market`) AS sub_query1
                            GROUP BY `state`
                             ORDER BY `MinProfit` ASC;"""
    data10 = fetch_data(query10)
    
    
    return render_template("index.html", data1=data1, data2=data2, data3=data3, data4=data4, data5=data5, data6=data6, data7=data7, data8=data8, data9=data9, data10=data10)


if __name__ == "__main__":
    app.run(debug=True)
