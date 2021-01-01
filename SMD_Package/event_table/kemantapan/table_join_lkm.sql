SELECT table1.LINKID, table1.FROM_STA, table1.TO_STA, table1.SEGMENT_LENGTH, table1.IRI as IRI, rni.SURF_TYPE
FROM
    (
        SELECT LINKID, FROM_STA, TO_STA, IRI, SEGMENT_LENGTH, LANE_CODE
        FROM roughness_1_2020
    ) table1
    LEFT OUTER JOIN
    (
        SELECT LINKID, FROM_STA, TO_STA, SURF_TYPE AS SURF_TYPE, LANE_CODE
        FROM rni_2020
    ) rni
    on table1.LINKID=rni.LINKID and table1.FROM_STA/:to_km_factor = rni.FROM_STA and table1.LANE_CODE = rni.LANE_CODE
WHERE table1.LINKID IN ('01001') -- for testing.
ORDER by table1.LINKID,table1.FROM_STA,table1.TO_STA