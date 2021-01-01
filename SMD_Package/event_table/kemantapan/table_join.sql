SELECT table1.LINKID, table1.FROM_STA, table1.TO_STA, table1.SEGMENT_LENGTH, table1.IRI as IRI, rni.SURF_TYPE
FROM
    (
        SELECT LINKID, FROM_STA, TO_STA, AVG(IRI) as IRI, max(SEGMENT_LENGTH) as SEGMENT_LENGTH
        FROM roughness_1_2020
        GROUP BY LINKID, FROM_STA, TO_STA
    ) table1
    LEFT OUTER JOIN
    (
        SELECT LINKID, FROM_STA, TO_STA, STATS_MODE(SURF_TYPE) AS SURF_TYPE
        FROM rni_2020
        GROUP BY LINKID, FROM_STA, TO_STA
    ) rni
    on table1.LINKID=rni.LINKID and table1.FROM_STA*100/:to_km_factor = rni.FROM_STA
WHERE table1.LINKID IN ('01001') -- for testing.
ORDER by table1.LINKID,table1.FROM_STA,table1.TO_STA