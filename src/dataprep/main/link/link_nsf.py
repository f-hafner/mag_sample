
# build query for nsf and 



q = """
        SELECT a.GrantID, a.Position, a.Award_AwardEffectiveDate, substr(a.Award_AwardEffectiveDate, 7, 4) as Year
            , b.*, c.*
        FROM NSF_MAIN as a 
        INNER JOIN (
            SELECT GrantID, Position, Name 
            FROM NSF_Institution
        ) b 
        USING (GrantID, Position)
        INNER JOIN (
            SELECT GrantID, Position, Firstname, LastName, PIFullName
            FROM NSF_Investigator
            WHERE RoleCode = 'Principal Investigator'
        ) c
        USING (GrantID, Position)
        WHERE AWARD_TranType = "Grant" AND AWARD_Agency = 'NSF' 
        limit 10
        """


# unclear: where do I see the award type?
# note: there can be different instutions! --> need to join be grantid, not grant and position! 