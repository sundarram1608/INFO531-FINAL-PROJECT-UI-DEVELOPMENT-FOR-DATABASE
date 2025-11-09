import streamlit as st

st.set_page_config(
                    page_title="Northwind DBMS",
                    # page_icon="",
                    layout="wide",
                    initial_sidebar_state="collapsed"
                )
st.title(":blue[Northwind DBMS Web Application]")
# st.header("Manage your Northwind database with ease using this interface.")
# st.divider()
st.markdown("""
                #### :green[This application allows you to perform various operations on the Northwind database:]
                - ##### CRUD operations on Categories, Customers, Employees, Order Details, Orders, Products, Shippers and Suppliers
                - ##### Generate & Export reports and analytics""")
st.divider()
st.markdown("""
                #### :green[Quick Start]
                - ##### :orange[Navigate] through the sidebar by clicking on :gray-badge[>>] at the top left of the screen to access different modules
                - ##### :orange[CRUD operations] - Select Data Management CRUD
                - ##### :orange[Analytics] - Select Analytics Dashboard""")
