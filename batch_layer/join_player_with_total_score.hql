-- concise player information
DROP TABLE IF EXISTS zhoua_player_summary;

CREATE TABLE zhoua_player_summary AS
SELECT
    pl.ID AS Player_ID,
    pl.Name,
    pl.Number,
    pl.Country,
    pl.Position,
    pl.Age,
    pts.Total_Points_Until_2023_2024
FROM
    zhoua_nba_players_orc pl
LEFT JOIN
    zhoua_total_points_orc pts
ON
    pl.Name = pts.Player_Name;


SELECT *
FROM zhoua_player_summary;
