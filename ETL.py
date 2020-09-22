# -- APAN 5310: SQL & RELATIONAL DATABASES

#   -------------------------------------------------------------------------
#   --                                                                     --
#   --                            HONOR CODE                               --
#   --                                                                     --
#   --  I affirm that I will not plagiarize, use unauthorized materials,   --
#   --  or give or receive illegitimate help on assignments, papers, or    --
#   --  examinations. I will also uphold equity and honesty in the         --
#   --  evaluation of my work and the work of others. I do so to sustain   --
#   --  a community built around this Code of Honor.                       --
#   --                                                                     --
#   -------------------------------------------------------------------------


#     You are responsible for submitting your own, original work. We are
#     obligated to report incidents of academic dishonesty as per the
#     Student Conduct and Community Standards.


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------


# -- HOMEWORK ASSIGNMENT 6


#   NOTES:
#
#     - Type your code between the START and END tags. Code should cover all
#       questions. Do not alter this template file in any way other than
#       adding your answers. Do not delete the START/END tags. The file you
#       submit will be validated before grading and will not be graded if it
#       fails validation due to any alteration of the commented sections.
#
#     - Our course is using PostgreSQL. We grade your assignments in PostgreSQL.
#       You risk losing points if you prepare your SQL queries for a different
#       database system (MySQL, MS SQL Server, Oracle, etc).
#
#     - Make sure you test each one of your answers. If a query returns an error
#       it will earn no points.
#
#     - In your CREATE TABLE statements you must provide data types AND
#       primary/foreign keys (if applicable).
#
#     - You may expand your answers in as many lines as you find appropriate
#       between the START/END tags.
#


# -----------------------------------------------------------------------------


#
#  NOTE: Provide the script that covers all questions between the START/END tags
#        at the end of the file. Separate START/END tags for each question.
#        Add comments to explain each step.
#
#
#  QUESTION 1 (5 points)
#  ---------------------
#  For this assignment we will use a fictional dataset of customer transactions
#  of a pharmacy. The dataset provides information on customer purchases of
#  one or two drugs at different quantities. Download the dataset from the
#  assignment page.
#
#  You will notice that there can be multiple transactions per customer, each
#  one recorded on a separate row.
#
#  Design an appropriate 3NF relational schema. Then, create a new database
#  called "abc_pharmacy" in pgAdmin. Finally, provide the Python code that
#  connects to the new database and creates all necessary tables as per the 3NF
#  relational schema you designed (Note: you should create more than one table).
#
#  NOTE: You should use pgAdmin ONLY to create the database. All other actions
#        must be performed in your Python code. No points if the database
#        tables are created in pgAdmin and not with Python code.

# -- START PYTHON CODE --
import pandas as pd
from sqlalchemy import create_engine;
df = pd.read_csv('APAN5310_HW6_DATA.csv');
df.info();
df.head();
conn_url = 'postgresql://postgres:password@localhost/abc_pharmacy';
engine = create_engine(conn_url);
connection = engine.connect();
stmt = """
create table customer
        (customer_id  integer,
         first_name   varchar(50) NOT NULL,
         last_name    varchar(50) NOT NULL,
         email        varchar(255) NOT NULL,
         cell_phone   char(12) NOT NULL,
         primary key(customer_id)

 );

create table customer_homephone
         (customer_id  integer,
         home_phone    char(12),
         primary key(customer_id),
         foreign key(customer_id) references customer (customer_id)
 );

create table drug
        (drug_id    integer,
         drug_name  text NOT NULL,
         drug_company text NOT NULL,
         primary key(drug_id)
 );

create table customer_order
        (order_id        integer unique,
         customer_id     integer,
         purchase_timestamp timestamp NOT NULL,
         primary key(order_id),
         foreign key(customer_id) references customer (customer_id)
         );

create table product_order
        (order_id    integer,
         drug_id     integer,
         quantity    integer NOT NULL,
         price       money NOT NULL,
         primary key(order_id,drug_id),
         foreign key(drug_id) references drug (drug_id),
         foreign key(order_id) references customer_order (order_id)
 );

"""
connection.execute(stmt);

# -- END PYTHON CODE --

# -----------------------------------------------------------------------------
#
#  QUESTION 2 (15 points)
#  ---------------------
#  Provide the Python code that populates the database with the data from the
#  provided "APAN5310_HW6_DATA.csv" file. You can download the dataset
#  from the assignment page. It is anticipated that you will perform several steps
#  of data processing in Python in order to extract, transform and load all data from
#  the file to the database tables. Manual transformations in a spreadsheet, or
#  similar, are not acceptable, all work must be done in Python. Make sure your code
#  has no errors, no partial credit for code that returns errors. When grading,
#  we will run your script and test your work with sample SQL scripts on the
#  database that will be created and populated.
#

# -- START PYTHON CODE --

df[['cell_phone','home_phone']] = df.cell_and_home_phones.str.split(";",expand=True,);
df.insert(0, 'order_id', range(1, 1 + len(df)));
df.insert(0, 'order_id', range(1, 1 + len(df)));
slice_df1 = df[['order_id','first_name','last_name', 'email','cell_and_home_phones','drug_company_1','drug_name_1','quantity_1','price_1','purchase_timestamp','cell_phone','home_phone','Name']];
slice_df2 = df[['order_id','first_name','last_name', 'email','cell_and_home_phones','drug_company_2','drug_name_2','quantity_2','price_2','purchase_timestamp','cell_phone','home_phone','Name']];
slice_df1.rename(columns={'drug_name_1': 'drug_name', 'drug_company_1': 'drug_company','quantity_1':'quantity','price_1':'price'}, inplace=True);
slice_df2.rename(columns={'drug_name_2': 'drug_name', 'drug_company_2': 'drug_company','quantity_2':'quantity','price_2':'price'}, inplace=True);
crazydf = pd.concat([slice_df1, slice_df2]);
temp_customerdf = crazydf.loc[:,['Name']].drop_duplicates();
temp_customerdf.insert(0, 'customer_id', range(1, 1 + len(temp_customerdf)));
customer_id_list = [temp_customerdf.customer_id[temp_customerdf.Name == i].values[0] for i in crazydf.Name];
crazydf.insert(1, 'customer_id', customer_id_list);

customer_df = crazydf[[ 'customer_id','first_name', 'last_name', 'email', 'cell_phone']].drop_duplicates();
customer_df.to_sql(name='customer', con=engine, if_exists='append', index=False);

homephone_df = crazydf.loc[:, ['customer_id','home_phone']].drop_duplicates();
homephone_df = homephone_df[homephone_df.home_phone != ''];
homephone_df.to_sql(name='customer_homephone', con=engine, if_exists='append', index=False);

customerorder_df = crazydf.loc[:, ['order_id','customer_id','purchase_timestamp']].drop_duplicates();
customerorder_df.to_sql(name='customer_order', con=engine, if_exists='append', index=False);

drug_df = crazydf.loc[:, ['drug_name','drug_company']].drop_duplicates();
drug_df = drug_df.dropna(subset=['drug_name']);
drug_df['drugnamecompany'] = drug_df['drug_name'].str.cat(drug_df['drug_company'],sep=" ");

temp_drug_df = pd.DataFrame(drug_df.drugnamecompany.unique(), columns=['drugnamecompany']);
temp_drug_df.insert(0, 'drug_id', range(1, 1 + len(temp_drug_df)));

drug_id_list = [temp_drug_df.drug_id[temp_drug_df.drugnamecompany == i].values[0] for i in drug_df.drugnamecompany];
drug_df.insert(0, 'drug_id', drug_id_list);
drug_df = drug_df[['drug_id','drug_name','drug_company']];
drug_df.to_sql(name='drug', con=engine, if_exists='append', index=False);

crazydf = crazydf.dropna(subset=['drugnamecompany']);
drug_id_list = [temp_drug_df.drug_id[temp_drug_df.drugnamecompany == i].values[0] for i in crazydf.drugnamecompany];
crazydf.insert(6, 'drug_id', drug_id_list);

order_df = crazydf[['order_id','drug_id','quantity','price']].drop_duplicates();
order_df.to_sql(name='product_order', con=engine, if_exists='append', index=False);


# -- END PYTHON CODE --

# -----------------------------------------------------------------------------
#
#  QUESTION 3 (2 points)
#  ---------------------
#  Write the Python code that queries the "abc_pharmacy" database and returns
#  the customer name(s) and total purchase cost of the top 3 most expensive
#  transactions.
#
#  Type the actual result as part of your answer below, as a comment.
#

# -- START PYTHON CODE --
stmt = """
select first_name, last_name, total_cost
from
   (select c.customer_id, tc.total_cost,dense_rank() over (order by total_cost DESC) as t_rank
    from customer_order c,
      (select order_id, sum(price) as total_cost
       from product_order
       group by order_id) as tc
   where tc.order_id = c.order_id) as tr, customer c
where tr.customer_id = c.customer_id
and t_rank <= 3
order by total_cost DESC;
"""
results = connection.execute(stmt).fetchall();
column_names = results[0].keys();
t_df = pd.DataFrame(results, columns=column_names);
t_df
# -- END PYTHON CODE --

# -----------------------------------------------------------------------------
