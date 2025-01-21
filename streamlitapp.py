# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Smart Tagging")

st.write(
    """
    """
)

# Get the current credentials
session = get_active_session()

db_name = st.text_input("Enter Database Name")
schema_name = st.text_input("Enter Schema Name")
table_name = st.text_input("Enter Table Name")

if(db_name!='' and schema_name!='' and table_name!=''):

    created_dataframe = session.sql(f"select * from {db_name}.{schema_name}.{table_name}")

    st.write("Available Fields:")
    
    for i in created_dataframe.columns:
        st.write(i)

    # Execute the query and convert it into a Pandas dataframe
    queried_data = created_dataframe.to_pandas()

    column_name = st.text_input("Enter Column Name")

    if(column_name!=''):
        column_sample_df = session.sql(f"select distinct {column_name} from {db_name}.{schema_name}.{table_name}")
        column_data = column_sample_df.to_pandas()

        column_data = column_sample_df.to_pandas()

        st.write("Data Sample")
        st.write(
            column_data.head(5)
        )

        df = session.sql(f"""
        with cte as (
            SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER({column_name},
                'What are the different word categories can I use for this text ?') as cortex_result
            FROM {db_name}.{schema_name}.{table_name} 
        ) 
        select cortex_result[0]['answer'] as tag, cortex_result[0]['score'] as score from cte order by cortex_result[0]['score']  desc limit 5
""")
        suggested_tags = df.select('tag').collect()
        custom_tags = st.text_input("Select keywords or categories suggested by AI or add your own in the text box below separated by commas(,)")
        
        i =0
        user_selection_from_tags = []
        for item in suggested_tags: 
            selected = st.checkbox(label=item['TAG'], key=f"item_{i}")
            i = i+1
            # Do something with selected items (if needed) 
            if selected:
                user_selection_from_tags.append(item['TAG'].replace('"', '') )

        
        
        st.write("Selected tags:")
        total_tags = custom_tags.split(",")
        total_tags.extend(user_selection_from_tags)
        for i in total_tags:
            st.write(i)


    classify_data = None
        
    def calculate_result():
        if(len(total_tags) >=2):
            st.subheader("Smart Tagging")
            st.write("Classified data:")
            classify_df = session.sql(f"select {column_name}, SNOWFLAKE.CORTEX.CLASSIFY_TEXT ({column_name}, {total_tags})['label'] as TAG from {db_name}.{schema_name}.{table_name} limit 25")
            classify_data = classify_df.to_pandas()
            
            st.write(classify_data)
            st.write("Total tags applied:")
            classify_group_df = session.sql(f"select SNOWFLAKE.CORTEX.CLASSIFY_TEXT ({column_name}, {total_tags})['label'] as TAG, count(tag) AS COUNT from {db_name}.{schema_name}.{table_name} group by 1 ")
            classify_group_data = classify_group_df.to_pandas()
            st.write(classify_group_data)
            
            # Create a simple bar chart
            # # See docs.streamlit.io for more types of charts
            st.subheader("Smart Analysis")
            st.bar_chart(data=classify_group_data, x="TAG", y="COUNT")
            
    
    if st.button("Generate"):
        calculate_result() 
    if st.button("Save"):
        st.write(f"Tags saved into a new table called along with current data from the {table_name} ")
        # Join the DataFrames
        #joined_df = classify_data.join(queried_data, on=["R_REASON_DESC"], join_type="inner")  
        #session.write_pandas(joined_df, "TEST.PUBLIC.result_table", auto_create_table=True)
    