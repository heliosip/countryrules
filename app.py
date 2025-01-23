import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import re

# Initialize session state
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
    st.session_state['username'] = None
    st.session_state['password'] = None
    st.session_state['database'] = None

st.set_page_config(page_title="IP Rule Family Analyzer", layout="wide", initial_sidebar_state="expanded")

def get_db_connection(username, password, database):
    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server=tcp:lumenip.database.windows.net,1433;"
        f"Database={database};"
        f"Uid={username}@lumenip;"
        f"Pwd={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

def calculate_date(trigger_date, formula):
    if not formula or not isinstance(formula, str):
        return None
    
    # Standardize formula text
    formula = formula.strip().lower()
    
    # Extract number and unit from formula
    pattern = r'add (\d+) (\w+)'
    match = re.search(pattern, formula)
    
    if not match:
        return None
        
    number = int(match.group(1))
    unit = match.group(2).rstrip('s')  # Remove potential plural
    
    if unit == 'month':
        # Handle month addition considering end of month cases
        new_month = trigger_date.month + number
        new_year = trigger_date.year + (new_month - 1) // 12
        new_month = ((new_month - 1) % 12) + 1
        
        # Try to maintain the same day, but adjust for end of month cases
        max_day = (trigger_date.replace(year=new_year, month=new_month + 1, day=1) - timedelta(days=1)).day
        new_day = min(trigger_date.day, max_day)
        
        return trigger_date.replace(year=new_year, month=new_month, day=new_day)
    elif unit == 'day':
        return trigger_date + timedelta(days=number)
    elif unit == 'week':
        return trigger_date + timedelta(weeks=number)
    
    return None

def get_calculated_rule_data(conn, rule_id=None, trigger_date=None):
    results_df = pd.read_sql("EXEC dbo.RuleHierarchyReport", conn)
    
    if rule_id:
        family_refs = get_family_references(results_df, rule_id=rule_id)
        results_df = results_df[results_df['FamilyReference'].isin(family_refs)]
    
    if trigger_date:
        # Convert trigger_date to datetime if it's not already
        if isinstance(trigger_date, str):
            trigger_date = pd.to_datetime(trigger_date).date()
            
        # Apply date calculations and store results in new columns
        results_df['Calculated_Due_Date'] = results_df['DueDate'].apply(
            lambda x: calculate_date(trigger_date, x) if pd.notnull(x) else None)
        results_df['Calculated_Final_Due_Date'] = results_df['FinalDueDate'].apply(
            lambda x: calculate_date(trigger_date, x) if pd.notnull(x) else None)
    
    return results_df

def get_dashboard_metrics_triggers(filtered_df):
    return pd.DataFrame([{
        'Jurisdictions': ', '.join(sorted(set([j.strip() for j in filtered_df['Jurisdictions'].str.split(',').explode().str.strip()]))),
        'MatterTypes': ', '.join(sorted(set([m.strip() for m in filtered_df['MatterType'].str.split(',').explode().str.strip()]))),
        'Actions': len(filtered_df[filtered_df['RuleType'] == 'Action']),
        'Tasks': len(filtered_df[filtered_df['RuleType'] == 'Task'])
    }])

def get_dashboard_metrics_release(filtered_df, from_date, to_date):
    return pd.DataFrame([{
        'Jurisdictions': ', '.join(sorted(set([j.strip() for j in filtered_df['Country'].str.split(',').explode().str.strip()]))),
        'From': from_date.strftime('%Y-%m-%d'),
        'To': to_date.strftime('%Y-%m-%d'),
        'Actions': len(filtered_df[filtered_df['Rule Type'] == 'Action']),
        'Tasks': len(filtered_df[filtered_df['Rule Type'] == 'Task'])
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
def get_release_notes_data(conn, jurisdiction=None, matter_type=None, from_date=None, to_date=None):
    conditions = []
    if jurisdiction and jurisdiction != 'All':
        conditions.append(f"c.Name = '{jurisdiction}'")
    if matter_type:
        conditions.append(f"m.MaterType = '{matter_type}'")
    if from_date:
        conditions.append(f"rd.ModifiedOn >= '{from_date}'")
    if to_date:
        conditions.append(f"rd.ModifiedOn <= '{to_date}'")
    
    where_clause = f"WHERE rd.Active = 1 {' AND ' + ' AND '.join(conditions) if conditions else ''}"
    
    query = f"""
    SELECT DISTINCT
        rd.ID as 'QA Rule ID',
        rd.ProdId as 'Rule ID',
        rd.Activity as 'Rule Name',
        rt.RuleType as 'Rule Type',
        STUFF((
            SELECT DISTINCT ', ' + m2.MaterType
            FROM dbo.SplitStrings(rd.MatterType, ',') mt2
            JOIN tblMatterTypeMaster m2 ON RTRIM(LTRIM(mt2.Item)) = CAST(m2.ID AS VARCHAR)
            FOR XML PATH(''), TYPE).value('.', 'varchar(max)'), 1, 2, '') as 'Matter Type',
        STUFF((
            SELECT DISTINCT ', ' + c2.Name
            FROM dbo.SplitStrings(rd.Jurisdiction, ',') j2
            JOIN tblCountryMaster c2 ON RTRIM(LTRIM(j2.Item)) = CAST(c2.ID AS VARCHAR)
            FOR XML PATH(''), TYPE).value('.', 'varchar(max)'), 1, 2, '') as Country,
        rd.versionType as 'Version Type',
        rd.CalcCode as 'Calc Code',
        rd.versionNotes as 'Version Notes',
        rd.releaseVersion as 'Release Version',
        rd.Reference as Reference,
        rd.ModifiedOn as 'Modified On'
    FROM tblRuleDefination rd
    CROSS APPLY dbo.SplitStrings(rd.MatterType, ',') mt
    CROSS APPLY dbo.SplitStrings(rd.Jurisdiction, ',') j
    JOIN tblMatterTypeMaster m ON RTRIM(LTRIM(mt.Item)) = CAST(m.ID AS VARCHAR)
    JOIN tblCountryMaster c ON RTRIM(LTRIM(j.Item)) = CAST(c.ID AS VARCHAR)
    LEFT JOIN tblRuleTypeMaster rt ON rd.RuleType = rt.ID
    {where_clause}
    ORDER BY rd.ModifiedOn DESC
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

def get_calculated_rule_data(conn, rule_id=None, trigger_date=None):
    results_df = pd.read_sql("EXEC dbo.RuleHierarchyReport", conn)
    
    if rule_id:
        family_refs = get_family_references(results_df, rule_id=rule_id)
        results_df = results_df[results_df['FamilyReference'].isin(family_refs)]
    
    if trigger_date:
        results_df['Calculated_Due_Date'] = results_df['DueDate'].apply(
            lambda x: calculate_date(trigger_date, x))
        results_df['Calculated_Final_Due_Date'] = results_df['FinalDueDate'].apply(
            lambda x: calculate_date(trigger_date, x))
    
    return results_df

def main():
    css = '''
        <style>
            .stMetric .metric-label { font-size: 12px !important; }
            .stMetric .metric-value { font-size: 24px !important; }
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
    '''
    st.markdown(css, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown('<div class="logo-text">RightHub</div>', unsafe_allow_html=True)
        
        if not st.session_state['authenticated']:
            st.header("Login")
            username = st.text_input("Username", key="username_input")
            password = st.text_input("Password", type="password", key="password_input")
            database = st.selectbox("Database", 
                options=["lumenip-IPRulesEngineQA1", "IPRulesEngine"],
                key="database_select")
            
            if st.button("Login", key="login_button"):
                try:
                    conn = get_db_connection(username, password, database)
                    test_df = pd.read_sql("SELECT 1", conn)
                    conn.close()
                    st.session_state['authenticated'] = True
                    st.session_state['username'] = username
                    st.session_state['password'] = password
                    st.session_state['database'] = database
                    st.rerun()
                except Exception as e:
                    st.error(f"Login failed: {str(e)}")
                    st.session_state['authenticated'] = False
        
        if st.session_state['authenticated']:
            st.header("Reports")
            report_type = st.selectbox(
                "Select Report",
                options=["What Triggers What", "Release Notes", "Calculate Rule"],
                key="report_select"
            )
            
            st.header("Search Filters")
            
            try:
                conn = get_db_connection(
                    st.session_state['username'],
                    st.session_state['password'],
                    st.session_state['database']
                )
                
                jurisdictions = get_jurisdictions(conn)
                jurisdiction = st.selectbox(
                    "Jurisdiction", 
                    options=['All'] + list(jurisdictions['Name']),
                    key="jurisdiction_select"
                )
                
                matter_types = pd.read_sql("""
                    SELECT DISTINCT MaterType
                    FROM tblMatterTypeMaster 
                    WHERE (isDirtyFlag = 0 OR isDirtyFlag IS NULL)
                    ORDER BY MaterType
                """, conn)
                
                matter_type = st.selectbox(
                    "Matter Type", 
                    options=[''] + list(matter_types['MaterType']),
                    key="matter_type_select"
                )
                
                if report_type in ["What Triggers What", "Calculate Rule"]:
                    filtered_options = get_filtered_options(conn, jurisdiction, matter_type)
                    
                    rule = st.selectbox(
                        "Search Rules",
                        options=[''] + list(sorted(filtered_options['DisplayName'].unique())),
                        key="rule_select"
                    )
                    
                    if report_type == "What Triggers What":
                        outcomes = st.selectbox(
                            "Search Outcomes",
                            options=[''] + list(sorted(filtered_options['Outcome'].dropna().unique())),
                            key="outcome_select"
                        )
                    else:  # Calculate Rule
                        trigger_date = st.date_input(
                            "Trigger Date",
                            value=datetime.now().date(),
                            key="trigger_date"
                        )
                else:  # Release Notes
                    from_date = st.date_input("From Date", key="from_date")
                    to_date = st.date_input("To Date", key="to_date")
                
                search_clicked = st.button("Search", type="primary", key="search_button")
            except Exception as e:
                st.error(f"Error in filters: {str(e)}")
                search_clicked = False
            finally:
                if 'conn' in locals() and conn:
                    conn.close()

    # Main content area for results
    if st.session_state['authenticated'] and search_clicked:
        try:
            conn = get_db_connection(
                st.session_state['username'],
                st.session_state['password'],
                st.session_state['database']
            )
            
            if report_type == "What Triggers What":
                results_df = pd.read_sql("EXEC dbo.RuleHierarchyReport", conn)
                
                rule_id = rule.split(']')[0][1:] if rule else None
                
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
                    
                    dashboard_df = get_dashboard_metrics_triggers(filtered_df)
                    
                    cols = st.columns([3, 1, 1, 1, 1])
                    cols[0].metric("Jurisdictions", truncate_text(dashboard_df['Jurisdictions'].iloc[0], max_length=50))
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
            
            elif report_type == "Calculate Rule":
                rule_id = rule.split(']')[0][1:] if rule else None
                filtered_df = get_calculated_rule_data(conn, rule_id, trigger_date)
                
                if jurisdiction and jurisdiction != 'All':
                    filtered_df = filtered_df[filtered_df['Jurisdictions'].str.contains(
                        jurisdiction, case=False, na=False)]
                if matter_type:
                    filtered_df = filtered_df[filtered_df['MatterType'].str.contains(
                        matter_type, case=False, na=False)]
                
                if filtered_df.empty:
                    st.write("No results found.")
                else:
                    def truncate_text(text, max_length=30):
                        return text[:max_length] + '...' if len(text) > max_length else text
                    
                    dashboard_df = get_dashboard_metrics_triggers(filtered_df)
                    
                    cols = st.columns([3, 1, 1, 1, 1])
                    cols[0].metric("Jurisdictions", 
                        truncate_text(dashboard_df['Jurisdictions'].iloc[0], max_length=50))
                    cols[1].metric("Matter Types", 
                        truncate_text(dashboard_df['MatterTypes'].iloc[0]))
                    cols[2].metric("Actions", int(dashboard_df['Actions'].iloc[0]))
                    cols[3].metric("Tasks", int(dashboard_df['Tasks'].iloc[0]))

                    st.markdown('<div class="spacer"></div>', unsafe_allow_html=True)
                    
# Create display dataframe with base date column
                    display_df = filtered_df[[
                        'FamilyReference', 'RuleID', 'RuleType', 'RuleName',
                        'Output Type', 'Outcome'
                    ]].copy()
                    
                    # Add Base Date before calculated dates
                    display_df['Base Date'] = trigger_date.strftime('%Y-%m-%d')
                    display_df['Calculated_Due_Date'] = filtered_df['Calculated_Due_Date']
                    display_df['Calculated_Final_Due_Date'] = filtered_df['Calculated_Final_Due_Date']
                    
                    # Format dates for display
                    for date_col in ['Calculated_Due_Date', 'Calculated_Final_Due_Date']:
                        display_df[date_col] = pd.to_datetime(
                            display_df[date_col]).dt.strftime('%Y-%m-%d')
                        
                    st.dataframe(
                        display_df,
                        hide_index=True,
                        use_container_width=True,
                        height=600
                    )
            
            else:  # Release Notes
                filtered_df = get_release_notes_data(
                    conn, 
                    jurisdiction=jurisdiction if jurisdiction != 'All' else None,
                    matter_type=matter_type if matter_type else None,
                    from_date=from_date,
                    to_date=to_date
                )
                
                if filtered_df.empty:
                    st.write("No results found.")
                else:
                    def truncate_text(text, max_length=30):
                        return text[:max_length] + '...' if len(text) > max_length else text
                    
                    dashboard_df = get_dashboard_metrics_release(filtered_df, from_date, to_date)
                    
                    cols = st.columns([3, 1, 1, 1, 1])
                    cols[0].metric("Jurisdictions", truncate_text(dashboard_df['Jurisdictions'].iloc[0], max_length=50))
                    cols[1].metric("From", dashboard_df['From'].iloc[0])
                    cols[2].metric("To", dashboard_df['To'].iloc[0])
                    cols[3].metric("Actions", int(dashboard_df['Actions'].iloc[0]))
                    cols[4].metric("Tasks", int(dashboard_df['Tasks'].iloc[0]))

                    st.markdown('<div class="spacer"></div>', unsafe_allow_html=True)
                    
                    st.dataframe(
                        filtered_df[[
                            'QA Rule ID', 'Rule ID', 'Rule Name', 'Rule Type', 'Matter Type',
                            'Country', 'Version Type', 'Calc Code', 'Version Notes',
                            'Release Version', 'Reference', 'Modified On'
                        ]],
                        hide_index=True,
                        use_container_width=True,
                        height=600
                    )
            
            # Common export functionality
            csv = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Export to Excel",
                data=csv,
                file_name=f"{report_type.lower().replace(' ', '_')}.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"Error loading results: {str(e)}")
        finally:
            if 'conn' in locals() and conn:
                conn.close()

if __name__ == "__main__":
    main()
