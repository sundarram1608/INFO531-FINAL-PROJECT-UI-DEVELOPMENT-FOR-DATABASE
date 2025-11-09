import streamlit as st
from helpers import *


def create_tabs():
    # #Create tabs for CRUD operations
    tab1, tab2, tab3, tab4 = st.tabs(["Create", "Read", "Update", "Delete"])

    with tab1:
        st.info(":green[Create New Records in a Table by Uploading CSV File]")
        # #Get tables to select from
        tables_list = get_tables()
        # all_tables = get_tables()
        # master_tables = ["Categories","Employees","Customers","Suppliers","Shippers"]
        # tables_list = [table for table in all_tables if table in master_tables]
        selected_table = st.selectbox(":blue[Select Table to Create New Record]", tables_list)

        bulk_upload = ["Categories", "Customers", "Employees", "Shippers", "Suppliers"]
        if selected_table in bulk_upload:    
            # #Fetch columns of the selected table
            for table_name in bulk_upload:
                if selected_table == table_name:
                    table_column_details = fetch_table_column_details(table_name)
                    # column_constraint_df = pd.DataFrame(list(zip(columns, constraints, column_key, column_autoincr)), columns=['Column Header', 'Data Type', 'Key Details', 'Auto Increment'])

            # #Provide Instructions for CSV upload
            with st.popover("Data Upload Instructions"):
                st.warning(f"Ensure the {selected_table} data is in CSV format with the below listed details.")
                st.dataframe(table_column_details,hide_index=True,use_container_width=True)
                st.info(f'{csv_instructions(selected_table)}')
            
            # #File uploader for CSV    
            uploaded_file = st.file_uploader(":blue[Upload CSV File]", type=["csv"])
            
            # #Process & verify the uploaded file
            if uploaded_file is not None:
                # #Read CSV safely
                try:
                    df_uploaded = pd.read_csv(uploaded_file)
                except Exception as e:
                    st.error(f"❌ Having problem with the file uploaded : {e}")
                else:
                    # #Check for empty csv
                    if df_uploaded.empty:
                        st.error("❌ The uploaded CSV file has no records. Please provide at least one row of data to insert or update.")
                    else:
                        # #Proceed to validation only if data exists
                        # result = validate_df_against_schema(df_uploaded, table_column_details)
                        required_cols_msg, not_null_msg = validate_df_against_schema(df_uploaded, table_column_details)
                        if required_cols_msg == 1 and not_null_msg == 1:
                            if selected_table == "Customers":
                                primary_key_check_value = check_for_primary_key_uniqueness(selected_table,df_uploaded)
                                if primary_key_check_value == 1:
                                    st.success("All required columns are present and there are no null values in NOT NULL columns.")
                                    st.info(f"Below data would be created in {selected_table} table in the database, if there are no data type & duplication discrepancies")
                                    st.dataframe(df_uploaded, hide_index=True, use_container_width=True)
                                    insert_button = st.button("Insert Records into Database")
                                else:
                                    st.error(f"Recheck Primary key allocation")

                            else:
                                st.success("All required columns are present and there are no null values in NOT NULL columns.")
                                st.info(f"Below data would be created in {selected_table} table in the database, if there are no data type & duplication discrepancies")
                                st.dataframe(df_uploaded, hide_index=True, use_container_width=True)
                                
                                insert_button = st.button("Insert Records into Database")
                            
                            try:
                                if insert_button:
                                    insert_value = insert_data_into_db(selected_table,table_column_details, df_uploaded)
                                    if insert_value == 1:
                                        st.success(f"Successfully inserted data in to database")
                                    elif insert_value == 2:
                                        st.info(f"No data created as there was no new data in the uploaded csv")
                                    else:
                                        st.error(f"Error inserting data into database.")
                        
                            except Exception as e:
                                pass
                                # st.error(f"Error inserting data into database: {e}")
        elif selected_table == "Orders":
            pass
        elif selected_table == "OrderDetails":
            pass
        else:
            pass
                            
    with tab2:
        pass
    
    with tab3:
        pass
    
    with tab4:
        pass