from __future__ import annotations
import streamlit as st
import os
import mysql.connector
import pandas as pd
import plotly.express as px
import io
import re
from pandas.api import types as pdt
import math
from mysql.connector import errorcode, IntegrityError
import numpy as np


# MySQL Connection
def connect_db():
    """Function to connect to MySQL database"""
    password = os.getenv("MYSQL_PASSWORD")
    conn_object = mysql.connector.connect(
                                            host="localhost",  
                                            user="root",       
                                            password=password, 
                                            database="northwind"  
                                        )
    return conn_object

################# FOR CRUD OPERATIONS & VALIDATIONS ################# 

# Show tables in the database
def get_tables():
    """Function to show tables in the database"""
    conn = connect_db()
    query = f'''SELECT table_name 
                FROM information_schema.tables
                WHERE table_schema = 'northwind' 
                AND table_type = 'BASE TABLE';'''
    df = pd.read_sql(query, conn)
    tables_list = df['TABLE_NAME'].tolist()
    conn.close()
    # st.write("### List of Tables in Northwind Database")
    return tables_list

# Fetch columns of a table
def fetch_table_column_details(table_name):
    """Function to fetch columns of a table"""
    conn = connect_db()
    query = f"SHOW COLUMNS FROM {table_name};"
    df = pd.read_sql(query, conn)
    conn.close()
    # st.dataframe(df)
    return df

def csv_instructions(selected_table):
    general_instructions = '''\n1. Follow the same column order as listed in the above table. \n2. Do not miss any column. \n3. Do not include extra columns that are not part of the table. \n4. Ensure data types in each column adhere to the specified data types. \n5. Ensure correct data in non null columns \n'''
    general_instructions_customers = '''\n1. Follow the same column order as listed in the above table. \n2. Do not miss any column. \n3. Do not include extra columns that are not part of the table. \n4. Ensure data types in each column adhere to the specified data types. \n5. Ensure correct data in non null columns \n 6. Ensure to use unique Customer IDs \n 7. Do not leave any value blank (add NA if unknown)'''
    general_instructions_employees = '''\n1. Follow the same column order as listed in the above table. \n2. Do not miss any column. \n3. Do not include extra columns that are not part of the table. \n4. Ensure data types in each column adhere to the specified data types. \n5. Ensure correct data in non null columns \n 6. Ensure to data type of date columns in csv as Custom formatted to YYYY-MM-DD \n 7. Do not add any existing employee again'''

    if selected_table == "Categories":
        return general_instructions
    elif selected_table == "Customers":
        return general_instructions_customers
    elif selected_table == "Employees":
        return general_instructions_employees
    else:
        return general_instructions
    
    
def check_for_primary_key_uniqueness(selected_table, uploaded_df):
    conn = connect_db()
    cur = conn.cursor()    

    # Step 1: Fetch table column details
    selected_table_df = fetch_table_column_details(selected_table)
    
    # Step 2: Identify the primary key column
    primary_key_col = selected_table_df.loc[selected_table_df['Key'] == 'PRI', 'Field'].values
    primary_key_col = primary_key_col[0]
    
    # Step 3: Fetch existing primary key values from the selected table
    existing_df = pd.read_sql(f"SELECT {primary_key_col} FROM {selected_table}", conn)
    conn.close()

    # Step 4: Take only that column from uploaded_df    
    uploaded_keys = uploaded_df[primary_key_col]

    # Step 5: Check for duplicates
    duplicates = uploaded_keys[uploaded_keys.isin(existing_df[primary_key_col])]

    if not duplicates.empty:
        return 0
    else:
        return 1


def validate_df_against_schema(df_uploaded: pd.DataFrame, table_column_details: pd.DataFrame):
    """Function to validate uploaded DataFrame against table schema"""
    
    # 1. Check for required columns
    expected_cols = table_column_details["Field"].tolist()
    uploaded_cols = df_uploaded.columns.tolist()
    # st.write(expected_cols)
    # st.write(uploaded_cols) 

    # Compare sets (exact, case-sensitive)
    missing_cols = [c for c in expected_cols if c not in uploaded_cols]
    # st.write(missing_cols)
    if missing_cols:
        st.error(f"Missing required columns:") 
        st.write(missing_cols)
    else:
        required_cols_msg = 1
        
    # 2. Check for NOT NULL constraints
    not_null_columns = table_column_details.loc[table_column_details["Null"].str.upper() == "NO", "Field"].tolist()
    # Identify columns that have NaN or empty string where they shouldn’t
    columns_with_nulls = []
    for col in not_null_columns:
        if df_uploaded[col].isnull().sum()>0 or df_uploaded[col].isna().sum() or df_uploaded[col].eq("").sum()>0:
            columns_with_nulls.append(col)
    if columns_with_nulls:
        st.error("The below columns have null values while they are not supposed to have null values:")
        st.write(columns_with_nulls)
    else:
        not_null_msg = 1
    
    return required_cols_msg, not_null_msg


##########################################################################################################################################################


def remove_existing_rows(df, table_name):
    """Return only rows whose key_cols combination does not exist in the table."""
    conn = connect_db()
    cur = conn.cursor()    
    key_cols = df.columns.tolist()
    # st.write(f"### Key Columns: {key_cols}")    
    keys = ", ".join(f"`{c}`" for c in key_cols)
    # st.write(f"### Key Columns: {keys}")
    query = f"SELECT {keys} FROM `{table_name}`"
    existing = pd.read_sql(query, conn)
    # st.write("### Existing Rows in Database")
    # st.dataframe(existing)
    # st.write(existing.dtypes)
    # st.write("### Uploaded DataFrame Rows")
    # # st.dataframe(df)
    # st.write(df.dtypes)
    conn.close()

    for col in key_cols:
        if existing[col].dtype == 'datetime64[ns]':
            df[col] = pd.to_datetime(df[col], errors='coerce')
            existing[col] = pd.to_datetime(existing[col], errors='coerce')
        else: 
           df[col] = df[col].astype(existing[col].dtype)
    
    
    # new_df = pd.concat([existing, df], ignore_index=True)
    # st.write("### Combined DataFrame for Duplicates Check")
    # st.dataframe(new_df)
    # duplicated_rows = new_df.duplicated().sum()
    # st.write(f"Number of Duplicated Rows based on all columns: {duplicated_rows}")
    # st.write("Datatypes converted")
    # st.write(existing.dtypes)
    try:
        # new_df = df[~df.apply(tuple, 1).isin(existing.apply(tuple, 1))]  
        new_df = pd.merge(existing, df, how='outer', indicator=True).query('_merge == "right_only"').drop('_merge',axis=1)

        # st.write("### New Rows after removing existing rows")
        # st.dataframe(new_df)
        return new_df 
    #     # Return in the same column order as original df
    #     new_df = new_df[df.columns]
    #     st.dataframe(new_df)
    except Exception as e:
        st.error(f"Error during concat-based filtering to find existing rows: {e}")
        return None  # If error, return original df


############################################################################################################################################

def insert_data_into_db(selected_table,table_column_details, df_uploaded):
    """Function to insert data into database"""
    conn = connect_db()
    cursor = conn.cursor()

    # 1. Drop any auto_increment column from the DataFrame and also the duplicated records    
    # Extract all columns and auto_increment ones
    # st.write(f"Connection Setup to Table: {selected_table}")
    all_cols = table_column_details["Field"].tolist()
    # st.write(f"All Columns in Table: {all_cols}")
    autoinc_cols = []
    for col_name in all_cols:
        if table_column_details.loc[table_column_details["Field"]==col_name, "Extra"].values[0].lower()=="auto_increment":
            autoinc_cols.append(col_name)
    # st.write(f"Auto-increment Columns to Exclude: {autoinc_cols}")
    df = df_uploaded.copy()
    for col in autoinc_cols:
        if col in df.columns:
            df = df.drop(columns=[col])
    # st.write("### DataFrame after dropping auto-increment columns")
    #st.dataframe(df,hide_index=True,use_container_width=True)

    # Drop any duplicated records:
    new_df = remove_existing_rows(df, selected_table)
    # st.write("Data to be created")
    # st.dataframe(new_df,hide_index=True,use_container_width=True)
    
    # Write the new_df to the selected table manually
    def _quote_ident_mysql(name: str) -> str:
        # Quote identifiers like table/column names for MySQL
        return "`" + str(name).replace("`", "``") + "`"

    try:
        # -- A. clean headers so we don't end up with 'nan' as a column name
        # trim header strings
        new_df.columns = [str(c).strip() for c in new_df.columns]
        # drop any blank/NaN headers
        bad_headers = [c for c in new_df.columns if pd.isna(c) or str(c).strip() == ""]
        if bad_headers:
            st.warning(f"Dropping columns with missing headers: {bad_headers}")
            new_df = new_df.drop(columns=bad_headers)

        # -- B. only keep columns that actually exist in the table
        valid_cols = table_column_details["Field"].astype(str).tolist()
        cols = [c for c in new_df.columns if c in valid_cols]
        if not cols:
            st.error("None of the DataFrame columns match the table columns.")
            return 0

        # -- C. build INSERT with quoted identifiers
        col_list = ", ".join(_quote_ident_mysql(c) for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        insert_query = f"INSERT INTO {_quote_ident_mysql(selected_table)} ({col_list}) VALUES ({placeholders})"

        # st.write(f"Using columns: {cols}")
        # st.write(insert_query)

        # -- D. convert NaN/NaT to None in values (so DB receives NULL, not 'nan')
        def row_tuple(row):
            vals = []
            for c in cols:
                v = row[c]
                if (isinstance(v, (float, np.floating)) and np.isnan(v)) or pd.isna(v):
                    vals.append(None)
                else:
                    vals.append(v)
            return tuple(vals)

        data = [row_tuple(r) for _, r in new_df.iterrows()]
        # st.write(data[:5])  # preview a few rows

        # -- E. execute
        if not data:
            st.info("No rows to insert.")
            return 2

        cursor.executemany(insert_query, data)
        conn.commit()
        # st.success(f"Inserted {len(data)} new rows into {selected_table}.")
        return 1

    except IntegrityError as ie:
        conn.rollback()
        st.error(f"Integrity error: {ie}")
        return 0
    except mysql.connector.Error as db_err:
        conn.rollback()
        st.error(f"MySQL error: {db_err}")
        return 0
    finally:
        cursor.close()
        conn.close()          
        
        
        
        
        
        
# # Build constraint string for a column
# def build_constraint(row):
#     parts = [row["Type"]]  # e.g., 'int', 'varchar(15)', 'text'
#     parts.append("NOT NULL" if str(row["Null"]).upper() == "NO" else "NULL")
#     if str(row["Key"]).upper() == "PRI":
#         parts.append("PRIMARY KEY")
#         parts.append("UNIQUE")  # PK implies unique
#     elif str(row["Key"]).upper() == "UNI":
#         parts.append("UNIQUE")
#     if "auto_increment" in str(row.get("Extra", "")).lower():
#         parts.append("AUTO_INCREMENT")
#     return " ".join(parts)



# # Helper functions for validation
# def _extract_varchar_len(constraint_text: str) -> int | None:
#     match = re.search(r"VARCHAR\s*\(\s*(\d+)\s*\)", constraint_text, flags=re.I)
#     return int(match.group(1)) if match else None

# # Helper for validation - Type inference from SQL types
# def _expected_type(constraint_text: str) -> str | None:
#     # Very lightweight type sniffing from common SQL types
#     text = constraint_text.upper()
#     if any(t in text for t in ["INT", "BIGINT", "SMALLINT"]): return "int"
#     if any(t in text for t in ["DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL"]): return "float"
#     if "BOOL" in text: return "bool"
#     if "DATE" in text and "TIME" not in text: return "date"
#     if "TIMESTAMP" in text or "DATETIME" in text or "TIME" in text: return "datetime"
#     if any(t in text for t in ["CHAR", "TEXT", "VARCHAR"]): return "str"
#     return None

# # Helper for validation - check non-blank values
# def _nonblank(s: pd.Series) -> pd.Series:
#     # treat empty strings as blank, not just NaN
#     return s[~(s.isna() | s.astype(str).str.strip().eq(""))]

# # Helper for validation - check if series matches expected type
# def _series_matches_type(s: pd.Series, expected: str) -> bool:
#     try:
#         nb = _nonblank(s)

#         if expected == "int":
#             coerced = pd.to_numeric(nb, errors="coerce")
#             # Must all be numbers and all be integers (no decimals)
#             if coerced.isna().any():
#                 return False
#             return (coerced % 1 == 0).all()

#         if expected == "float":
#             coerced = pd.to_numeric(nb, errors="coerce")
#             return not coerced.isna().any()

#         if expected == "bool":
#             ok = nb.astype(str).str.lower().isin(
#                 ["true","false","1","0","yes","no","t","f","y","n"]
#             )
#             return ok.all()

#         if expected == "date":
#             coerced = pd.to_datetime(nb, errors="coerce", format="mixed").dt.date
#             return not coerced.isna().any()

#         if expected == "datetime":
#             coerced = pd.to_datetime(nb, errors="coerce", format="mixed")
#             return not coerced.isna().any()

#         if expected == "str":
#             return True
#     except Exception:
#         return False

#     return True




# # Validate uploaded DataFrame against schema
# def validate_df_against_schema(df: pd.DataFrame, column_constraint_df: pd.DataFrame) -> dict:
#     """
#     Returns a dict with:
#       - ok: bool
#       - errors: list[str]
#       - details: pd.DataFrame (per-column results)
#     """
#     required_cols = list(column_constraint_df["Column Header"])
#     constraints_map = dict(zip(column_constraint_df["Column Header"], column_constraint_df["Data Type"]))

#     errors = []
#     detail_rows = []

#     # Column presence checks
#     uploaded_cols = list(df.columns)
#     missing = [c for c in required_cols if c not in df.columns]
#     extras = [c for c in df.columns if c not in required_cols]

#     if missing:
#         errors.append(f"Missing required columns: {missing}")
#     if extras:
#         # Not always an error—if your table allows extras, downgrade this to a warning
#         errors.append(f"Unexpected columns present: {extras}")

#     # Per-column constraint checks
#     for col in required_cols:
#         cons = str(constraints_map.get(col, ""))
#         if col not in df.columns:
#             detail_rows.append({"Column": col, "Status": "Missing", "Notes": "Column not found in file"})
#             continue

#         s = df[col]
#         notes = []

#         # NOT NULL
#         if re.search(r"\bNOT\s+NULL\b", cons, flags=re.I):
#             null_count = s.isna() | (s.astype(str).str.strip().eq(""))
#             if null_count.sum() > 0:
#                 notes.append(f"NOT NULL violated in {int(null_count.sum())} rows")

#         # UNIQUE
#         if re.search(r"\bUNIQUE\b", cons, flags=re.I):
#             dup_count = s.duplicated(keep=False).sum()
#             if dup_count > 0:
#                 notes.append(f"UNIQUE violated in {int(dup_count)} rows")

#         # TYPE check (best-effort)
#         expected = _expected_type(cons)
#         if expected:
#             if not _series_matches_type(s, expected):
#                 notes.append(f"Type mismatch (expected {expected})")

#         # VARCHAR length
#         maxlen = _extract_varchar_len(cons)
#         if maxlen is not None:
#             too_long = s.dropna().astype(str).str.len() > maxlen
#             if too_long.any():
#                 notes.append(f"Length > {maxlen} in {int(too_long.sum())} rows")

#         # Record row result
#         detail_rows.append({
#                             "Column": col,
#                             "Status": "OK" if not notes else "ERROR",
#                             "Notes": "; ".join(notes) if notes else ""
#                         })

#     details_df = pd.DataFrame(detail_rows)
#     if (details_df["Status"] == "ERROR").any():
#         errors.append("One or more columns violate constraints.")

#     return {"ok": len(errors) == 0, "errors": errors, "details": details_df}


# def remove_existing_rows(df, table_name, key_cols):
#     """Return only rows whose key_cols combination does not exist in the table."""
#     conn = connect_db()
#     cur = conn.cursor()
#     keys = ", ".join(f"`{c}`" for c in key_cols)
#     cur.execute(f"SELECT {keys} FROM `{table_name}`;")
#     existing = pd.DataFrame(cur.fetchall(), columns=key_cols)
#     conn.close()

#     try:
#         # Tag sources and concatenate
#         df_tagged = df.copy()
#         df_tagged["_src"] = "df"

#         existing_tagged = existing.copy()
#         existing_tagged["_src"] = "existing"

#         combined = pd.concat([df_tagged, existing_tagged], ignore_index=True, sort=False)

#         # For each key combo, mark if it exists in the DB.
#         # NOTE: groupby ignores groups with NaN in any key, yielding NaN -> fill with False.
#         has_existing = (
#             combined.groupby(key_cols)["_src"]
#             .transform(lambda s: (s == "existing").any())
#             .fillna(False)                    # <- critical to avoid ~ on float (NaN)
#             .astype(bool)
#         )

#         # Keep only original df rows whose key combo does NOT exist in DB
#         new_df = combined[(combined["_src"] == "df") & (~has_existing)].drop(columns="_src")

#         # Return in the same column order as original df
#         new_df = new_df[df.columns]
#         st.dataframe(new_df)
#     except Exception as e:
#         st.error(f"Error during concat-based filtering to find existing rows: {e}")
#         return df  # If error, return original df

#     return new_df


# # Function to insert data into database
# def inser_datainto_db(table_name, df_uploaded):
#     """
#     Insert rows from a DataFrame into a MySQL table,
#     automatically ignoring AUTO_INCREMENT primary key columns
#     so MySQL will generate them.
#     """

#     # --- 1. Inspect table metadata to find auto_increment column(s)
#     conn = connect_db()
#     cur = conn.cursor(dictionary=True)
#     cur.execute(f"SHOW COLUMNS FROM `{table_name}`;")
#     # st.write(f"### Connection Setup to Table: {table_name}")
#     meta = cur.fetchall()
#     cur.close()
#     conn.close()

#     # Extract all columns and auto_increment ones
#     all_cols = [c["Field"] for c in meta]
#     autoinc_cols = [c["Field"] for c in meta if "auto_increment" in str(c["Extra"]).lower()]
    
#     # st.write(f"All Columns in Table: {all_cols}")
#     # st.write(f"Auto-increment Columns to Exclude: {autoinc_cols}")
#     # --- 2. Drop any auto_increment column from the DataFrame and also the duplicated records
    
#     df = df_uploaded.copy()
#     for col in autoinc_cols:
#         if col in df.columns:
#             df = df.drop(columns=[col])
#     # st.write("### DataFrame after dropping auto-increment columns")
#     # st.dataframe(df)

#     # Keep only valid DB columns
#     df = df[[c for c in df.columns if c in all_cols]]
#     # st.write("### DataFrame after aligning with table columns")
#     # st.dataframe(df)
#     # st.write(df.shape[1])
#     if df.empty or df.shape[1] == 0:
#         return 0, [{"level": "error", "message": "No insertable columns after alignment."}]
#         # st.write("No insertable columns after alignment.")
    
#     # Drop any duplicated records:
#     df = remove_existing_rows(df, table_name, df.columns.tolist())
#     # st.write("### Data to be created")
#     # st.dataframe(df)
#     # --- 3. Convert NaN/blank → None (so SQL NULLs are used)
#     df = df.applymap(lambda x: None if (pd.isna(x) or (isinstance(x, str) and x.strip() == "")) else x)
#     df.drop_duplicates(inplace=True)
#     st.write("### Data to be created")
#     st.dataframe(df)
#     # --- 4. Prepare INSERT statement dynamically
#     cols = list(df.columns)
#     col_list = ", ".join(f"`{c}`" for c in cols)
#     placeholders = ", ".join(["%s"] * len(cols))
#     # insert_sql = f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders})"
#     insert_sql = f"INSERT IGNORE INTO `{table_name}` ({col_list}) VALUES ({placeholders})"



#     # --- 5. Execute in one transaction (fast executemany)
#     conn = connect_db()
#     cur = conn.cursor()
#     inserted = 0
#     errors = []

#     try:
#         conn.start_transaction()
#         rows = [tuple(row[c] for c in cols) for _, row in df.iterrows()]
#         cur.executemany(insert_sql, rows)
#         inserted = cur.rowcount
#         conn.commit()
#         st.success("✅ Data inserted successfully into the database.")
#     except IntegrityError as ie:
#         conn.rollback()
#         errors.append({"level": "error", "message": f"Integrity error: {ie}"})
#         st.error(f"Integrity error: {ie}")
#     except mysql.connector.Error as db_err:
#         conn.rollback()
#         errors.append({"level": "error", "message": f"MySQL error: {db_err}"})
#         st.error(f"MySQL error: {db_err}")
#     finally:
#         cur.close()
#         conn.close()

#     return None


# ################# FOR ANALYTICS DASHBOARD #################
# # Display Customer Data
# def display_customers():
#     """Fetch and display customer data in Streamlit"""
#     conn = connect_db()
#     query = "SELECT * FROM Customers;"
#     df = pd.read_sql(query, conn)
#     #Close the connection
#     conn.close()
#     st.write("### Customer Data")
#     st.dataframe(df)

# # # Create New Customer (CRUD)
# # def create_customer():
# #     """Form to add a new customer to the database"""
# #     st.subheader("Create New Customer")
# #     customer_id = st.text_input("Customer ID")
# #     company_name = st.text_input("Company Name")
# #     contact_name = st.text_input("Contact Name")
# #     contact_title = st.text_input("Contact Title")
# #     address = st.text_area("Address")
# #     city = st.text_input("City")
# #     region = st.text_input("Region")
# #     postal_code = st.text_input("Postal Code")
# #     country = st.text_input("Country")
# #     phone = st.text_input("Phone")
# #     fax = st.text_input("Fax")
    
# #     if st.button("Add Customer"):
# #         conn = connect_db()
# #         cursor = conn.cursor()
# #         query = """
# #         INSERT INTO Customers 
# #         (CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax) 
# #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
# #         """
# #         cursor.execute(query, (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax))
# #         conn.commit()
# #         cursor.close()
# #         conn.close()
# #         st.success(f"Customer {company_name} added successfully!")

# # # Analytics Dashboard (Example: Sales by Category)
# # def analytics_dashboard():
# #     """Display basic sales data using a bar chart"""
# #     st.title("Analytics Dashboard")
    
# #     conn = connect_db()
# #     cursor = conn.cursor()
# #     cursor.execute("""
# #         SELECT CategoryName, SUM(UnitPrice * Quantity * (1 - Discount)) AS TotalSales
# #         FROM OrderDetails od
# #         JOIN Products p ON od.ProductID = p.ProductID
# #         JOIN Categories c ON p.CategoryID = c.CategoryID
# #         GROUP BY CategoryName;
# #     """)  # Query to calculate sales by category
# #     data = cursor.fetchall()
# #     cursor.close()
# #     conn.close()

# #     # Create DataFrame for Plotly
# #     df = pd.DataFrame(data, columns=["Category", "Total Sales"])
    
# #     # Plotting the data using Plotly
# #     fig = px.bar(df, x="Category", y="Total Sales", title="Total Sales by Product Category")
# #     st.plotly_chart(fig)


