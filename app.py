import streamlit as st
import pandas as pd
import pyodbc

st.set_page_config(page_title="IP Rule Family Analyzer", layout="wide", initial_sidebar_state="expanded")

def get_db_connection(username, password):
    return pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=tcp:lumenip.database.windows.net,1433;"
        f"Database=lumenip-IPRulesEngineQA1;Uid={username}@lumenip;Pwd={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )

def get_dashboard_metrics(filtered_df):
    return pd.DataFrame([{
        'Jurisdictions': ', '.join(sorted(set([j.strip() for j in filtered_df['Jurisdictions'].str.split(',').explode().str.strip()]))),
        'MatterTypes': ', '.join(sorted(set([m.strip() for m in filtered_df['MatterType'].str.split(',').explode().str.strip()]))),
        'Actions': len(filtered_df[filtered_df['RuleType'] == 'Action']),
        'Tasks': len(filtered_df[filtered_df['RuleType'] == 'Task'])
    }])

def get_jurisdictions(conn):
    query = """
    SELECT DISTINCT Name,
           CASE Name 
               WHEN 'United States' THEN 1
               WHEN 'European Patent Office' THEN 2
               WHEN 'WIPO' THEN 3
               ELSE 4 
           END as SortOrder
    FROM tblCountryMaster 
    WHERE (isDirtyFlag = 0 OR isDirtyFlag IS NULL)
    ORDER BY 2, 1
    """
    return pd.read_sql(query, conn)

def get_filtered_options(conn, jurisdiction=None, matter_type=None):
    conditions = []
    if jurisdiction and jurisdiction != 'All':
        conditions.append(f"c.Name = '{jurisdiction}'")
    if matter_type:
        conditions.append(f"m.MaterType = '{matter_type}'")
    
    where_clause = f"WHERE rd.Active = 1 {' AND ' + ' AND '.join(conditions) if conditions else ''}"
    
    query = f"""
    SELECT DISTINCT
        rd.ID,
        rd.Activity,
        CONCAT('[', rd.ID, '] ', rd.Activity) as DisplayName,
        o.Label AS Outcome
    FROM tblRuleDefination rd
    CROSS APPLY dbo.SplitStrings(rd.MatterType, ',') mt
    CROSS APPLY dbo.SplitStrings(rd.Jurisdiction, ',') j
    JOIN tblMatterTypeMaster m ON RTRIM(LTRIM(mt.Item)) = CAST(m.ID AS VARCHAR)
    JOIN tblCountryMaster c ON RTRIM(LTRIM(j.Item)) = CAST(c.ID AS VARCHAR)
    LEFT JOIN tblOutcomes o ON rd.ID = o.[Rule]
    {where_clause}
    """
    return pd.read_sql(query, conn)

def get_family_references(df, rule_id=None, rule_name=None, outcome=None):
    if rule_id:
        return df[df['ChainPath'].apply(lambda x: str(rule_id) in x.split('->'))]['FamilyReference'].unique()
    elif rule_name:
        return df[df['RuleName'] == rule_name]['FamilyReference'].unique()
    elif outcome:
        return df[df['Outcome'].str.contains(outcome, case=False, na=False)]['FamilyReference'].unique()
    return []

def main():
    st.markdown("""
        <style>
            .stMetric .metric-label { font-size: 0.8rem !important; }
            .stMetric .metric-value { font-size: 1.8rem !important; }
            .logo-text { 
                font-family: 'Arial Black', sans-serif; 
                font-size: 2.5rem; 
                font-weight: 900;
                text-align: center;
                padding: 1rem;
                color: #FFFFFF;
            }
            .spacer { margin-bottom: 2rem; }
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown('<div class="logo-text">RightHub</div>', unsafe_allow_html=True)
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

    if username and password:
        try:
            conn = get_db_connection(username, password)
            
            with st.sidebar:
                st.header("Search Filters")
                
                jurisdictions = get_jurisdictions(conn)
                jurisdiction = st.selectbox("Jurisdiction", 
                    options=['All'] + list(jurisdictions['Name']))
                
                matter_types = pd.read_sql("""
                    SELECT DISTINCT MaterType
                    FROM tblMatterTypeMaster 
                    WHERE (isDirtyFlag = 0 OR isDirtyFlag IS NULL)
                    ORDER BY MaterType
                """, conn)
                
                matter_type = st.selectbox("Matter Type", 
                    options=[''] + list(matter_types['MaterType']))
                
                filtered_options = get_filtered_options(conn, jurisdiction, matter_type)
                
                rule = st.selectbox("Search Rules",
                    options=[''] + list(sorted(filtered_options['DisplayName'].unique())))
                
                # Split rule into ID and name if selected
                rule_id = rule.split(']')[0][1:] if rule else None
                rule_name = rule.split(']')[1].strip() if rule else None
                
                outcomes = st.selectbox("Search Outcomes",
                    options=[''] + list(sorted(filtered_options['Outcome'].dropna().unique())))
                
                search_clicked = st.button("Search", type="primary")

            if search_clicked:
                results_df = pd.read_sql("EXEC dbo.RuleHierarchyReport", conn)
                
                mask = pd.Series(True, index=results_df.index)
                
                if any([rule_id, outcomes]):
                    family_refs = get_family_references(
                        results_df,
                        rule_id=rule_id,
                        outcome=outcomes
                    )
                    mask &= results_df['FamilyReference'].isin(family_refs)
                
                if jurisdiction and jurisdiction != 'All':
                    mask &= results_df['Jurisdictions'].str.contains(jurisdiction, case=False, na=False)
                if matter_type:
                    mask &= results_df['MatterType'].str.contains(matter_type, case=False, na=False)
                
                filtered_df = results_df[mask]
                
                if filtered_df.empty:
                    st.write("No results found.")
                else:
                    def truncate_text(text, max_length=30):
                        return text[:max_length] + '...' if len(text) > max_length else text
                    
                    dashboard_df = get_dashboard_metrics(filtered_df)
                    
                    cols = st.columns(4)
                    cols[0].metric("Jurisdictions", truncate_text(dashboard_df['Jurisdictions'].iloc[0]))
                    cols[1].metric("Matter Types", truncate_text(dashboard_df['MatterTypes'].iloc[0]))
                    cols[2].metric("Actions", int(dashboard_df['Actions'].iloc[0]))
                    cols[3].metric("Tasks", int(dashboard_df['Tasks'].iloc[0]))

                    st.markdown('<div class="spacer"></div>', unsafe_allow_html=True)
                    
                    st.dataframe(
                        filtered_df[[
                            'FamilyReference', 'RuleID', 'ChainPath', 'RuleType', 'RuleName',
                            'MatterType', 'Jurisdictions', 'TriggeredBy', 'TriggerCondition',
                            'Output Type', 'Outcome', 'DueDate', 'FinalDueDate'
                        ]],
                        hide_index=True,
                        use_container_width=True,
                        height=600
                    )
                    
                    csv = filtered_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Export to Excel",
                        data=csv,
                        file_name="rule_families.csv",
                        mime="text/csv"
                    )

        except Exception as e:
            st.error(f"Error: {str(e)}")
        finally:
            conn.close()
    else:
        st.info("Please enter your database credentials to begin.")

if __name__ == "__main__":
    main()