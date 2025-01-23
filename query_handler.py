def get_rule_families(conn, filters=None):
    query = """
    WITH RuleAttributes AS (
        SELECT DISTINCT
            rd.ID AS RuleID,
            j.Item AS JurisdictionID,
            m.Item AS MatterTypeID
        FROM tblRuleDefination rd
        CROSS APPLY dbo.SplitStrings(rd.Jurisdiction, ',') j
        CROSS APPLY dbo.SplitStrings(rd.MatterType, ',') m
        WHERE rd.Active = 1
    ),
    ValidConnections AS (
        SELECT DISTINCT 
            o.[Rule] AS ParentRuleID,
            c.[Rule] AS ChildRuleID
        FROM tblOutcomes o
        JOIN tblConditions c ON c.Value = o.Label
        WHERE EXISTS (
            SELECT 1
            FROM RuleAttributes ra1
            JOIN RuleAttributes ra2 ON 
                ra1.JurisdictionID = ra2.JurisdictionID AND
                ra1.MatterTypeID = ra2.MatterTypeID
            WHERE ra1.RuleID = o.[Rule]
            AND ra2.RuleID = c.[Rule]
        )
    ),
    RuleFamilies AS (
        SELECT 
            rd.ID AS RuleID,
            1 AS Level,
            NULL AS ParentRuleID,
            rd.ID AS RootRuleID,
            'RF-' + RIGHT('00000' + CAST(ROW_NUMBER() OVER (ORDER BY rd.ID) AS VARCHAR(5)), 5) AS FamilyReference,
            CAST(CAST(rd.ID AS VARCHAR(10)) AS VARCHAR(900)) AS ChainPath
        FROM tblRuleDefination rd
        WHERE rd.Active = 1
        AND NOT EXISTS (
            SELECT 1 FROM ValidConnections vc WHERE vc.ChildRuleID = rd.ID
        )

        UNION ALL

        SELECT 
            vc.ChildRuleID,
            rf.Level + 1,
            rf.RuleID,
            rf.RootRuleID,
            rf.FamilyReference,
            CAST(rf.ChainPath + ' -> ' + CAST(vc.ChildRuleID AS VARCHAR(10)) AS VARCHAR(900))
        FROM RuleFamilies rf
        JOIN ValidConnections vc ON rf.RuleID = vc.ParentRuleID
        WHERE rf.Level < 5
    )
    SELECT 
        rf.FamilyReference,
        rd.ID AS RuleID,
        rf.ChainPath,
        rd.Activity AS RuleName,
        rf.Level,
        STUFF((
            SELECT DISTINCT ', ' + cm.Name
            FROM RuleAttributes ra
            JOIN tblCountryMaster cm ON cm.ID = ra.JurisdictionID
            WHERE ra.RuleID = rd.ID
            FOR XML PATH(''), TYPE
        ).value('.', 'varchar(max)'), 1, 2, '') AS Jurisdictions,
        STUFF((
            SELECT DISTINCT ', ' + 
                CASE ra.MatterTypeID
                    WHEN '1' THEN 'Patent'
                    WHEN '2' THEN 'Trademark'
                    WHEN '3' THEN 'Design'
                    WHEN '4' THEN 'Utility Model'
                    WHEN '5' THEN 'Domain Name'
                    WHEN '6' THEN 'Unitary Patent'
                    ELSE ra.MatterTypeID
                END
            FROM RuleAttributes ra
            WHERE ra.RuleID = rd.ID
            FOR XML PATH(''), TYPE
        ).value('.', 'varchar(max)'), 1, 2, '') AS MatterType
    FROM RuleFamilies rf
    JOIN tblRuleDefination rd ON rd.ID = rf.RuleID
    """
    
    if filters:
        where_clauses = []
        if filters.get('matter_type'):
            where_clauses.append(f"MatterType LIKE '%{filters['matter_type']}%'")
        if filters.get('jurisdiction'):
            where_clauses.append(f"Jurisdictions LIKE '%{filters['jurisdiction']}%'")
            
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
            
    query += " ORDER BY rf.FamilyReference, rf.Level"
    
    return pd.read_sql(query, conn)