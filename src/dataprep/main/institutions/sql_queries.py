
query_mag = """SELECT AffiliationId
                        , NormalizedName AS name
                        , PublicationCount
                        , Latitude as lat
                        , Longitude as lon
                    FROM affiliations
                    INNER JOIN affiliation_outcomes using(AffiliationId)
                    WHERE Iso3166Code = 'US' 
                        -- ## ignore these cases; university systems have low # of pubs except suny system
                        AND name NOT LIKE '%health system'
                        AND (name NOT LIKE '% system' OR name = 'state university of new york system')
                        AND PublicationCount > 61 -- ## approximately median; drops a lot of noise
                        AND name NOT LIKE '% extension'
                        -- ## these are restritions to avoid false positives and increase precision in training. 
                            --# oftentimes, universities have medical center/univ hospital separate. 
                            --# locations are similar but they (often) do not have a correspondence to cng
                            --# in other cases, the entities match but the end of the name differs (columbia university; "%main campus"). 
                        AND (name LIKE "%university%" -- ## avoid false positives
                                OR name LIKE "%college%" 
                                OR name LIKE "%institute%" 
                                OR name LIKE "%school of mines%"
                                OR name LIKE "%virginia tech%") 
                        AND name NOT LIKE "%university hospital%" -- ## unclear: cng is about teaching institutions, but mag is about research ones. hospitals are not in cng but may be relevant for research outcomes?
                        AND name NOT LIKE "%medical center%"
                        AND name NOT LIKE "%hospitals and clinics%"
                        AND name NOT LIKE "%law center%"
                        AND name NOT LIkE "%college of law%"
                        AND name NOT LIkE "%college of veterinary medicine%"
                """

query_cng = """SELECT unitid
            , normalizedname AS name
            , latitude AS lat
            , longitude AS lon
            , basic2021
            , stabbr
            , city
            FROM cng_institutions"""


query_pq = """SELECT university_id
                , normalizedname AS name
                , location
                , normalizedname AS city
                , us_stabbr as stabbr
                FROM pq_unis
                WHERE location like "United States%"
                    AND location NOT like "%Puerto Rico%" 
                    AND normalizedname NOT like "%university%and%university%" -- ## drop name strings with multiple institutions               
                    AND normalizedname NOT like "%university%with%university%" -- ## drop name strings with multiple institutions     
                    AND normalizedname NOT like "%global campus"   
                """
