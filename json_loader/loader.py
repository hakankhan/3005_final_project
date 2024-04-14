import psycopg
import json

competitionSeasonsStr = [
    ["La Liga", "2020/2021"],
    ["La Liga", "2019/2020"],
    ["La Liga", "2018/2019"],
    ["Premier League", "2003/2004"],
]
competitionSeasonsID = []


from datetime import timedelta

def convertTime(timeStr : str):
    mins, secs = map(int, timeStr.split(':'))
    return str(timedelta(minutes=mins, seconds=secs))

conn = psycopg.connect(dbname="project_database", user='postgres', password='1234', host='localhost', port='5432')
cur = conn.cursor()

conn.autocommit = True

with open("data/competitions.json", "r", encoding="utf-8") as compsJSON:
    comps = json.load(compsJSON)

for comp in comps:
    for competitionSeason in competitionSeasonsStr:
        if (
            comp["competition_name"] == competitionSeason[0]
            and comp["season_name"] == competitionSeason[1]
        ):
            competitionSeasonsID.append([comp["competition_id"], comp["season_id"]])
            query = '''
                    INSERT INTO competition (competition_id, season_id, competition_name, season_name, gender, youth, international)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                    '''
            values = (comp["competition_id"],comp["season_id"],comp["competition_name"],comp["season_name"],comp["competition_gender"],comp["competition_youth"],comp["competition_international"])
            cur.execute(query,values)

print("number of competitive seasons", len(competitionSeasonsID))


for cSID in competitionSeasonsID:
    with open( f"data/matches/{cSID[0]}/{cSID[1]}.json", "r", encoding="utf-8") as compJSON:
        comps = json.load(compJSON)
    print("number of matches", len(comps))
    for comp in comps:
        query = '''
                INSERT INTO match (match_id, match_date, kick_off, competition_id, season_id, home_team_id, home_team_name, away_team_id,away_team_name,home_score,away_score,stadium_id,stadium_name,stadium_country_name,referee_id, referee_name, referee_country_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                '''
        values = [comp["match_id"],comp["match_date"],comp["kick_off"],comp["competition"]["competition_id"],
                    comp["season"]["season_id"],comp["home_team"]["home_team_id"],comp["home_team"]["home_team_name"],
                    comp["away_team"]["away_team_id"],comp["away_team"]["away_team_name"],comp["home_score"],comp["away_score"]]
        if "stadium" in comp:
            values += comp["stadium"]["id"],comp["stadium"]["name"],comp["stadium"]["country"]["name"]
        else: 
            values += None,None,None
        
        if "referee" in comp:
            values += comp["referee"]["id"],comp["referee"]["name"],comp["referee"]["country"]["name"]
        else:
            values += None,None,None
        cur.execute(query,values)
        
        query = """
        INSERT INTO team (team_id,match_id,team_name,gender,manager_id,manager_name,manager_nickname,manager_country_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
        """
        
        hometeam = comp["home_team"]
        values = [hometeam["home_team_id"],comp["match_id"],hometeam["home_team_name"],hometeam["home_team_gender"]]
        
        if "managers" in hometeam:
            manager = hometeam["managers"][0]
            values += manager["id"],manager["name"],manager["nickname"],manager["country"]["name"]
        else:
            values += None,None,None,None
        
        cur.execute(query,values)
        
        awayteam = comp["away_team"]
        values = [awayteam["away_team_id"],comp["match_id"],awayteam["away_team_name"],awayteam["away_team_gender"]]
        
        if "managers" in awayteam:
            manager = awayteam["managers"][0]
            values += manager["id"],manager["name"],manager["nickname"],manager["country"]["name"]
        else:
            values += None,None,None,None
        
        cur.execute(query,values)
        
        

        with open(f"data/lineups/{comp["match_id"]}.json", "r", encoding="utf-8") as lineupJSON:
            lineup = json.load(lineupJSON)
            
            for team in lineup:
                for player in team["lineup"]:
                    query = """
                    INSERT INTO player (player_id, match_id, team_id, name, nickname, jersey_number, country_id,country_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                    """
                    values = [player["player_id"],comp["match_id"],team["team_id"],player["player_name"],player["player_nickname"],player["jersey_number"],player["country"]["id"],player["country"]["name"]]
                    
                    cur.execute(query,values)
                    
                    for pos in player["positions"]:
                        query = """
                        INSERT INTO position (match_id, player_id, position_name, from_time, to_time, start_period, end_period, start_reason, end_reason)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                        """ 
                        
                        values = [comp["match_id"],player["player_id"],pos["position_id"],convertTime(pos["from"])]
                        
                        if pos["to"]:
                            values += convertTime(pos["to"]),
                        else:
                            values += None,
                        
                        values += pos["from_period"],pos["to_period"],pos["start_reason"],pos["end_reason"]
                                                
                        cur.execute(query,values)
                    
                    for card in player["cards"]:
                        query = """
                        INSERT INTO card (player_id, match_id, time, type, reason, period)
                        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                        """

                        values = [player["player_id"],comp["match_id"],convertTime(card["time"]),card["card_type"],card["reason"],card["period"]]
                        cur.execute(query,values)
                    
                    
        with open(f"data/events/{comp["match_id"]}.json", "r", encoding="utf-8") as eventsJSON:
            events = json.load(eventsJSON)
            for event in events:
                e_id = None
                e_index = None
                e_period = None
                e_timestamp = None
                e_minute = None
                e_second = None
                e_type_id = None
                e_type_name = None
                e_possession = None
                e_possession_team_id = None
                e_possession_team_name = None
                e_play_pattern_id = None
                e_play_pattern_name = None
                e_team_id = None
                e_team_name = None
                e_duration = None
                e_tactics_formation = None
                e_tactics_lineup = None
                e_related_events = None
                e_half_start_late_video_start = None
                e_player_id = None
                e_player_name = None
                e_position_id = None
                e_position_name = None
                e_location = None
                e_pass_recipient_id = None
                e_pass_recipient_name = None
                e_pass_length = None
                e_pass_angle = None
                e_pass_height_id = None
                e_pass_height_name = None
                e_pass_end_location = None
                e_pass_body_part_id = None
                e_pass_body_part_name = None
                e_pass_type_id = None
                e_pass_type_name = None
                e_under_pressure = None
                e_pass_outcome_id = None
                e_pass_outcome_name = None
                e_pass_aerial_won = None
                e_pass_switch = None
                e_pass_technique_id = None
                e_pass_technique_name = None
                e_pass_through_ball = None
                e_off_camera = None
                e_pass_deflected = None
                e_pass_cross = None
                e_pass_outswinging = None
                e_pass_assisted_shot_id = None
                e_pass_shot_assist = None
                e_pass_no_touch = None
                e_pass_cut_back = None
                e_pass_inswinging = None
                e_counterpress = None
                e_pass_straight = None
                e_pass_goal_assist = None
                e_pass_miscommunication = None
                e_out = None
                e_ball_receipt_outcome_id = None
                e_ball_receipt_outcome_name = None
                e_carry_end_location = None
                e_ball_recovery_offensive = None
                e_ball_recovery_recovery_failure = None
                e_dribble_outcome_id = None
                e_dribble_outcome_name = None
                e_dribble_overrun = None
                e_dribble_nutmeg = None
                e_dribble_no_touch = None
                e_block_offensive = None
                e_block_deflection = None
                e_block_save_block = None
                e_miscontrol_aerial_won = None
                e_foul_committed_advantage = None
                e_foul_committed_card_id = None
                e_foul_committed_card_name = None
                e_foul_committed_offensive = None
                e_foul_committed_type_id = None
                e_foul_committed_type_name = None
                e_foul_committed_penalty = None
                e_foul_won_advantage = None
                e_foul_won_defensive = None
                e_foul_won_penalty = None
                e_duel_type_id = None
                e_duel_type_name = None
                e_duel_outcome_id = None
                e_duel_outcome_name = None
                e_clearance_body_part_id = None
                e_clearance_body_part_name = None
                e_clearance_left_foot = None
                e_clearance_head = None
                e_clearance_right_foot = None
                e_clearance_aerial_won = None
                e_clearance_other = None
                e_interception_outcome_id = None
                e_interception_outcome_name = None
                e_shot_one_on_one = None
                e_shot_statsbomb_xg = None
                e_shot_end_location = None
                e_shot_key_pass_id = None
                e_shot_type_id = None
                e_shot_type_name = None
                e_shot_outcome_id = None
                e_shot_outcome_name = None
                e_shot_technique_id = None
                e_shot_technique_name = None
                e_shot_body_part_id = None
                e_shot_body_part_name = None
                e_shot_freeze_frame = None
                e_shot_first_time = None
                e_shot_open_goal = None
                e_shot_aerial_won = None
                e_shot_deflected = None
                e_shot_saved_off_target = None
                e_shot_saved_to_post = None
                e_shot_redirect = None
                e_shot_follows_dribble = None
                e_goalkeeper_end_location = None
                e_goalkeeper_position_id = None
                e_goalkeeper_position_name = None
                e_goalkeeper_type_id = None
                e_goalkeeper_type_name = None
                e_goalkeeper_body_part_id = None
                e_goalkeeper_body_part_name = None
                e_goalkeeper_outcome_id = None
                e_goalkeeper_outcome_name = None
                e_goalkeeper_technique_id = None
                e_goalkeeper_technique_name = None
                e_goalkeeper_punched_out = None
                e_goalkeeper_shot_saved_off_target = None
                e_goalkeeper_shot_saved_to_post = None
                e_goalkeeper_success_in_play = None
                e_goalkeeper_lost_in_play = None
                e_goalkeeper_lost_out = None
                e_injury_stoppage_in_chain = None
                e_bad_behaviour_card_id = None
                e_bad_behaviour_card_name = None
                e_substitution_outcome_id = None
                e_substitution_outcome_name = None
                e_substitution_replacement_id = None
                e_substitution_replacement_name = None
                e_50_50_outcome_id = None
                e_50_50_outcome_name = None
                
                e_id = event.get("id", None)

                e_index = event.get("index", None)

                e_period = event.get("period", None)

                e_timestamp = event.get("timestamp", None)

                e_minute = event.get("minute", None)

                e_second = event.get("second", None)

                if "type" in event:
                    e_type_id = event["type"].get("id", None)
                    e_type_name = event["type"].get("name", None)

                e_possession = event.get("possession", None)

                if "possession_team" in event:
                    e_possession_team_id = event["possession_team"].get("id", None)
                    e_possession_team_name = event["possession_team"].get("name", None)

                if "play_pattern" in event:
                    e_play_pattern_id = event["play_pattern"].get("id", None)
                    e_play_pattern_name = event["play_pattern"].get("name", None)

                if "team" in event:
                    e_team_id = event["team"].get("id", None)
                    e_team_name = event["team"].get("name", None)

                e_duration = event.get("duration", None)

                if "tactics" in event:
                    e_tactics_formation = event["tactics"].get("formation", None)
                    e_tactics_lineup = event["tactics"].get("lineup", None)

                e_related_events = event.get("related_events", None)

                if "half_start" in event:
                    e_half_start_late_video_start = event["half_start"].get("late_video_start", None)

                if "player" in event:
                    e_player_id = event["player"].get("id", None)
                    e_player_name = event["player"].get("name", None)

                if "position" in event:
                    e_position_id = event["position"].get("id", None)
                    e_position_name = event["position"].get("name", None)

                e_location = event.get("location", None)

                if "pass" in event:
                    if "recipient" in event["pass"]:
                        e_pass_recipient_id = event["pass"]["recipient"].get("id", None)
                        e_pass_recipient_name = event["pass"]["recipient"].get("name", None)
                    e_pass_length = event["pass"].get("length", None)
                    e_pass_angle = event["pass"].get("angle", None)
                    if "height" in event["pass"]:
                        e_pass_height_id = event["pass"]["height"].get("id", None)
                        e_pass_height_name = event["pass"]["height"].get("name", None)
                    e_pass_end_location = event["pass"].get("end_location", None)
                    if "body_part" in event["pass"]:
                        e_pass_body_part_id = event["pass"]["body_part"].get("id", None)
                        e_pass_body_part_name = event["pass"]["body_part"].get("name", None)
                    if "type" in event["pass"]:
                        e_pass_type_id = event["pass"]["type"].get("id", None)
                        e_pass_type_name = event["pass"]["type"].get("name", None)
                    e_under_pressure = event.get("under_pressure", None)
                    if "outcome" in event["pass"]:
                        e_pass_outcome_id = event["pass"]["outcome"].get("id", None)
                        e_pass_outcome_name = event["pass"]["outcome"].get("name", None)
                    e_pass_aerial_won = event["pass"].get("aerial_won", None)
                    e_pass_switch = event["pass"].get("switch", None)
                    if "technique" in event["pass"]:
                        e_pass_technique_id = event["pass"]["technique"].get("id", None)
                        e_pass_technique_name = event["pass"]["technique"].get("name", None)
                    e_pass_through_ball = event["pass"].get("through_ball", None)
                    e_off_camera = event.get("off_camera", None)
                    e_pass_deflected = event["pass"].get("deflected", None)
                    e_pass_cross = event["pass"].get("cross", None)
                    e_pass_outswinging = event["pass"].get("outswinging", None)
                    e_pass_assisted_shot_id = event["pass"].get("assisted_shot_id", None)
                    e_pass_shot_assist = event["pass"].get("shot_assist", None)
                    e_pass_no_touch = event["pass"].get("no_touch", None)
                    e_pass_cut_back = event["pass"].get("cut_back", None)
                    e_pass_inswinging = event["pass"].get("inswinging", None)
                    e_counterpress = event.get("counterpress", None)
                    e_pass_straight = event["pass"].get("straight", None)
                    e_pass_goal_assist = event["pass"].get("goal_assist", None)
                    e_pass_miscommunication = event["pass"].get("miscommunication", None)
                    e_out = event.get("out", None)
                if "ball_receipt" in event and "outcome" in event["ball_receipt"]:
                    e_ball_receipt_outcome_id = event["ball_receipt"]["outcome"].get("id", None)
                    e_ball_receipt_outcome_name = event["ball_receipt"]["outcome"].get("name", None)
                if "carry" in event:
                    e_carry_end_location = event["carry"].get("end_location", None)
                if "ball_recovery" in event:
                    e_ball_recovery_offensive = event["ball_recovery"].get("offensive", None)
                    e_ball_recovery_recovery_failure = event["ball_recovery"].get("recovery_failure", None)
                if "dribble" in event:
                    e_dribble_outcome_id = event["dribble"]["outcome"].get("id", None)
                    e_dribble_outcome_name = event["dribble"]["outcome"].get("name", None)
                    e_dribble_overrun = event["dribble"].get("overrun", None)
                    e_dribble_nutmeg = event["dribble"].get("nutmeg", None)
                    e_dribble_no_touch = event["dribble"].get("no_touch", None)
                if "block" in event:
                    e_block_offensive = event["block"].get("offensive", None)
                    e_block_deflection = event["block"].get("deflection", None)
                    e_block_save_block = event["block"].get("save_block", None)
                if "miscontrol" in event:
                    e_miscontrol_aerial_won = event["miscontrol"].get("aerial_won", None)
                if "foul_committed" in event:
                    e_foul_committed_advantage = event["foul_committed"].get("advantage", None)
                    if "card" in event["foul_committed"]:
                        e_foul_committed_card_id = event["foul_committed"]["card"].get("id", None)
                        e_foul_committed_card_name = event["foul_committed"]["card"].get("name", None)
                    e_foul_committed_offensive = event["foul_committed"].get("offensive", None)
                    if "type" in event["foul_committed"]:
                        e_foul_committed_type_id = event["foul_committed"]["type"].get("id", None)
                        e_foul_committed_type_name = event["foul_committed"]["type"].get("name", None)
                    e_foul_committed_penalty = event["foul_committed"].get("penalty", None)
                if "foul_won" in event:
                    e_foul_won_advantage = event["foul_won"].get("advantage", None)
                    e_foul_won_defensive = event["foul_won"].get("defensive", None)
                    e_foul_won_penalty = event["foul_won"].get("penalty", None)
                if "duel" in event:
                    if "type" in event["duel"]:
                        e_duel_type_id = event["duel"]["type"].get("id", None)
                        e_duel_type_name = event["duel"]["type"].get("name", None)
                    if "outcome" in event["duel"]:
                        e_duel_outcome_id = event["duel"]["outcome"].get("id", None)
                        e_duel_outcome_name = event["duel"]["outcome"].get("name", None)
                if "clearance" in event:
                    if "body_part" in event["clearance"]:
                        e_clearance_body_part_id = event["clearance"]["body_part"].get("id", None)
                        e_clearance_body_part_name = event["clearance"]["body_part"].get("name", None)
                    e_clearance_left_foot = event["clearance"].get("left_foot", None)
                    e_clearance_head = event["clearance"].get("head", None)
                    e_clearance_right_foot = event["clearance"].get("right_foot", None)
                    e_clearance_aerial_won = event["clearance"].get("aerial_won", None)
                    e_clearance_other = event["clearance"].get("other", None)
                if "interception" in event and "outcome" in event["interception"]:
                    e_interception_outcome_id = event["interception"]["outcome"].get("id", None)
                    e_interception_outcome_name = event["interception"]["outcome"].get("name", None)
                if "shot" in event:
                    e_shot_one_on_one = event["shot"].get("one_on_one", None)
                    e_shot_statsbomb_xg = event["shot"].get("statsbomb_xg", None)
                    e_shot_end_location = event["shot"].get("end_location", None)
                    e_shot_key_pass_id = event["shot"].get("key_pass_id", None)
                    if "type" in event["shot"]:
                        e_shot_type_id = event["shot"]["type"].get("id", None)
                        e_shot_type_name = event["shot"]["type"].get("name", None)
                    if "outcome" in event["shot"]:
                        e_shot_outcome_id = event["shot"]["outcome"].get("id", None)
                        e_shot_outcome_name = event["shot"]["outcome"].get("name", None)
                    if "technique" in event["shot"]:
                        e_shot_technique_id = event["shot"]["technique"].get("id", None)
                        e_shot_technique_name = event["shot"]["technique"].get("name", None)
                    if "body_part" in event["shot"]:
                        e_shot_body_part_id = event["shot"]["body_part"].get("id", None)
                        e_shot_body_part_name = event["shot"]["body_part"].get("name", None)
                    e_shot_freeze_frame = event["shot"].get("freeze_frame", None)
                    e_shot_first_time = event["shot"].get("first_time", None)
                    e_shot_open_goal = event["shot"].get("open_goal", None)
                    e_shot_aerial_won = event["shot"].get("aerial_won", None)
                    e_shot_deflected = event["shot"].get("deflected", None)
                    e_shot_saved_off_target = event["shot"].get("saved_off_target", None)
                    e_shot_saved_to_post = event["shot"].get("saved_to_post", None)
                    e_shot_redirect = event["shot"].get("redirect", None)
                    e_shot_follows_dribble = event["shot"].get("follows_dribble", None)
                if "goalkeeper" in event:
                    e_goalkeeper_end_location = event["goalkeeper"].get("end_location", None)
                    if "position" in event["goalkeeper"]:
                        e_goalkeeper_position_id = event["goalkeeper"]["position"].get("id", None)
                        e_goalkeeper_position_name = event["goalkeeper"]["position"].get("name", None)
                    if "type" in event["goalkeeper"]:
                        e_goalkeeper_type_id = event["goalkeeper"]["type"].get("id", None)
                        e_goalkeeper_type_name = event["goalkeeper"]["type"].get("name", None)
                    if "body_part" in event["goalkeeper"]:
                        e_goalkeeper_body_part_id = event["goalkeeper"]["body_part"].get("id", None)
                        e_goalkeeper_body_part_name = event["goalkeeper"]["body_part"].get("name", None)
                    if "outcome" in event["goalkeeper"]:
                        e_goalkeeper_outcome_id = event["goalkeeper"]["outcome"].get("id", None)
                        e_goalkeeper_outcome_name = event["goalkeeper"]["outcome"].get("name", None)
                    if "technique" in event["goalkeeper"]:
                        e_goalkeeper_technique_id = event["goalkeeper"]["technique"].get("id", None)
                        e_goalkeeper_technique_name = event["goalkeeper"]["technique"].get("name", None)
                    e_goalkeeper_punched_out = event["goalkeeper"].get("punched_out", None)
                    e_goalkeeper_shot_saved_off_target = event["goalkeeper"].get("shot_saved_off_target", None)
                    e_goalkeeper_shot_saved_to_post = event["goalkeeper"].get("shot_saved_to_post", None)
                    e_goalkeeper_success_in_play = event["goalkeeper"].get("success_in_play", None)
                    e_goalkeeper_lost_in_play = event["goalkeeper"].get("lost_in_play", None)
                    e_goalkeeper_lost_out = event["goalkeeper"].get("lost_out", None)
                e_injury_stoppage_in_chain = event.get("injury_stoppage", {}).get("in_chain", None)
                if "bad_behaviour" in event and "card" in event["bad_behaviour"]:
                    e_bad_behaviour_card_id = event["bad_behaviour"]["card"].get("id", None)
                    e_bad_behaviour_card_name = event["bad_behaviour"]["card"].get("name", None)
                if "substitution" in event:
                    if "outcome" in event["substitution"]:
                        e_substitution_outcome_id = event["substitution"]["outcome"].get("id", None)
                        e_substitution_outcome_name = event["substitution"]["outcome"].get("name", None)
                    if "replacement" in event["substitution"]:
                        e_substitution_replacement_id = event["substitution"]["replacement"].get("id", None)
                        e_substitution_replacement_name = event["substitution"]["replacement"].get("name", None)
                if "50_50" in event and "outcome" in event["50_50"]:
                    e_50_50_outcome_id = event["50_50"]["outcome"].get("id", None)
                    e_50_50_outcome_name = event["50_50"]["outcome"].get("name", None)


                values = (e_id, comp["match_id"], e_index, e_period, e_timestamp, 
                e_minute, e_second, e_type_id, e_type_name, e_possession, e_possession_team_id, 
                e_possession_team_name, e_team_id,e_team_id,e_play_pattern_name)
                
                values2 = values + (e_player_id, e_player_name, e_position_id, e_position_name, e_duration)
                                
                match event["type"]["name"]:
                    case "Starting XI":
                        query = """
                            INSERT INTO starting_xi(event_id, match_id, index, period, timestamp, minute, 
                            second, type_id, type_name, possession, possession_team_id, possession_team_name, team_id, team_name, play_pattern_name,
                            duration, formation)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                        """
                        
                        values += (e_duration, e_tactics_formation)
                        
                        cur.execute(query,values)
                    case "Half Start":
                        query = """
                            INSERT INTO half_start(event_id, match_id, index, period, timestamp, minute, 
                            second, type_id, type_name, possession, possession_team_id, possession_team_name, team_id, team_name, play_pattern_name,
                            duration)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                        """
                        
                        values += (e_duration,)
                        
                        cur.execute(query,values)
                        
                    case "Pass":
                        query = """
                            INSERT INTO pass(event_id, match_id, index, period, timestamp, minute, second, type_id, 
                            type_name, possession, possession_team_id, possession_team_name, team_id, 
                            team_name ,play_pattern_name, player_id, player_name, position_id, 
                            position_name, duration, recipient_id, recipient_name, length, angle, height_id, height_name, 
                            end_location_x, end_location_y, body_part_id, body_part_name, pass_type_id, pass_type_name, 
                            under_pressure, outcome_id, outcome_name, aerial_won, switch, technique_id, technique_name, 
                            through_ball, deflected, is_cross, outswinging, assisted_shot_event_id, shot_assist, cut_back, 
                            inswinging, counterpress, straight, goal_assist, miscommunication, out, location_x, location_y)
                            
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_pass_recipient_id, e_pass_recipient_name,e_pass_length,e_pass_angle,e_pass_height_id,e_pass_height_name,
                                    e_pass_end_location[0],e_pass_end_location[1],e_pass_body_part_id,e_pass_body_part_name,e_pass_type_id,e_pass_type_name,
                                    e_under_pressure,e_pass_outcome_id,e_pass_outcome_name,e_pass_aerial_won,e_pass_switch,e_pass_technique_id,e_pass_technique_name,
                                    e_pass_through_ball,e_pass_deflected,e_pass_cross,e_pass_outswinging,e_pass_assisted_shot_id,e_pass_shot_assist,e_pass_cut_back,
                                    e_pass_inswinging,e_counterpress,e_pass_straight,e_pass_goal_assist, e_pass_miscommunication ,e_out, e_location[0],e_location[1])
                        
                        cur.execute(query,values)
                        
                    case "Ball Receipt*":
                        query = """
                            INSERT INTO ball_receipt(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            under_pressure, outcome_id, outcome_name)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_under_pressure,e_ball_receipt_outcome_id,e_ball_receipt_outcome_name)
                        
                        cur.execute(query,values)
                    case "Carry":
                        query = """
                            INSERT INTO carry(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            under_pressure, end_location_x, end_location_y)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        if e_carry_end_location:                     
                            values = values2 + (e_location[0],e_location[1],e_under_pressure,e_carry_end_location[0],e_carry_end_location[1])
                        else:
                            values = values2 + (e_location[0],e_location[1],e_under_pressure,None,None)
                            
                        
                        cur.execute(query,values)
                    
                    case "Pressure":
                        query = """
                            INSERT INTO pressure(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            under_pressure, counterpress)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        values = values2 + (e_location[0],e_location[1],e_under_pressure,e_counterpress)
                        cur.execute(query,values)
                        
                    case "Ball Recovery":
                        query = """
                            INSERT INTO ball_recovery(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            under_pressure, out, failure, offensive)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_under_pressure,e_out,e_ball_recovery_recovery_failure,e_ball_recovery_offensive)
                        cur.execute(query,values)
                    case "Dribbled Past":
                        query = """
                            INSERT INTO dribbled_past(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            counterpress)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_counterpress)
                        cur.execute(query,values)
                        
                        
                    case "Dribble":
                        query = """
                            INSERT INTO dribble(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            success, overrun, nutmeg, out, under_pressure, no_touch)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1], e_dribble_outcome_id == 8 ,e_dribble_overrun,e_dribble_nutmeg,e_out,e_under_pressure,e_dribble_no_touch)
                        cur.execute(query,values)
                        
                    case "Block":
                        query = """
                            INSERT INTO block(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            counterpress, offensive, deflection, under_pressure, save_block, out)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s,%s, %s,%s,%s, %s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_counterpress, e_block_offensive,e_block_deflection,e_under_pressure,e_block_save_block,e_out)
                        cur.execute(query,values)
                    case "Miscontrol":
                        query = """
                            INSERT INTO miscontrol(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            under_pressure, out, aerial_won)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_under_pressure,e_out,e_miscontrol_aerial_won)
                        cur.execute(query,values)
                        
                    case "Foul Committed":
                        query = """
                            INSERT INTO foul_committed(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            advantage, card_id, card_name, under_pressure, offensive, foul_type_id, foul_type_name, penalty, counterpress)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_foul_committed_advantage,
                                            e_foul_committed_card_id,e_foul_committed_card_name,e_under_pressure,
                                            e_foul_committed_offensive,
                                            e_foul_committed_type_id,e_foul_committed_type_name,e_foul_committed_penalty,e_counterpress)
                        cur.execute(query,values)
                        
                    case "Foul Won":
                        query = """
                            INSERT INTO foul_won(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            advantage, under_pressure, defensive, penalty)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_foul_won_advantage,e_under_pressure,e_foul_won_defensive,e_foul_won_penalty)
                        cur.execute(query,values)
                        
                    case "Duel":
                        query = """
                            INSERT INTO duel(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            duel_type_id, duel_outcome_id, duel_outcome_name, counterpress, duel_type_name)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s,%s,%s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_duel_type_id,e_duel_outcome_id, e_duel_outcome_name,e_counterpress,e_duel_type_name)
                        cur.execute(query,values)
                        
                    case "Dispossessed":
                        query = """
                            INSERT INTO dispossessed(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            under_pressure)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_under_pressure)
                        cur.execute(query,values)
                    case "Clearance":
                        query = """
                            INSERT INTO clearance(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            body_part_id, body_part_name, aerial_won)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s,%s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_clearance_body_part_id,e_clearance_body_part_name,e_clearance_aerial_won)
                        cur.execute(query,values)
                    case "Interception":
                        query = """
                            INSERT INTO interception(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            counterpress, outcome_id, outcome_name, under_pressure)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_counterpress,e_interception_outcome_id,e_interception_outcome_name,e_under_pressure)
                        cur.execute(query,values)
                    case "Shot":
                        query = """
                            INSERT INTO shot(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            one_on_one, statsbomb_xg, end_location_x, end_location_y, end_location_z, key_pass_id, shot_type_id,shot_type_name,outcome_id,
                            technique_id, technique_name, body_part_id, body_part_name, first_time, open_goal, under_pressure, aerial_won,
                            deflected, out, follows_dribble)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s,%s, %s)ON CONFLICT DO NOTHING
                        """
                        if len(e_shot_end_location) == 3:
                            values = values2 + (e_location[0],e_location[1],e_shot_one_on_one,e_shot_statsbomb_xg,e_shot_end_location[0],e_shot_end_location[1],e_shot_end_location[2],e_shot_key_pass_id,
                                                e_shot_type_id,e_shot_type_name,e_shot_outcome_id,e_shot_technique_id,e_shot_technique_name,e_shot_body_part_id,e_shot_body_part_name,e_shot_first_time,e_shot_open_goal,
                                                e_under_pressure,e_shot_aerial_won,e_shot_deflected,e_out, e_shot_follows_dribble)
                        else:
                            values = values2 + (e_location[0],e_location[1],e_shot_one_on_one,e_shot_statsbomb_xg,e_shot_end_location[0],e_shot_end_location[1],None,e_shot_key_pass_id,
                                                e_shot_type_id,e_shot_type_name,e_shot_outcome_id,e_shot_technique_id,e_shot_technique_name,e_shot_body_part_id,e_shot_body_part_name,e_shot_first_time,e_shot_open_goal,
                                                e_under_pressure,e_shot_aerial_won,e_shot_deflected,e_out, e_shot_follows_dribble)
                        cur.execute(query,values)
                        
                        
                    case "Goal Keeper":
                        query = """
                            INSERT INTO goalkeeper(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            goalkeeper_position_id,goalkeeper_position_name,goalkeeper_type_id,goalkeeper_type_name,body_part_id,body_part_name,outcome_id,
                            outcome_name,technique_id,technique_name,out,under_pressure,punched_out,shot_saved_off_target,shot_saved_to_post)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        if e_location:
                            values = values2 + (e_location[0],e_location[1],e_goalkeeper_position_id,e_goalkeeper_position_name,e_goalkeeper_type_id,e_goalkeeper_type_name,e_goalkeeper_body_part_id,e_goalkeeper_body_part_name,
                                                e_goalkeeper_outcome_id,e_goalkeeper_outcome_name,e_goalkeeper_technique_id,e_goalkeeper_technique_name, e_out,e_under_pressure,e_goalkeeper_punched_out,e_shot_saved_off_target,e_goalkeeper_shot_saved_to_post)
                        else:
                            values = values2 + (None,None,e_goalkeeper_position_id,e_goalkeeper_position_name,e_goalkeeper_type_id,e_goalkeeper_type_name,e_goalkeeper_body_part_id,e_goalkeeper_body_part_name,
                                                e_goalkeeper_outcome_id,e_goalkeeper_outcome_name,e_goalkeeper_technique_id,e_goalkeeper_technique_name, e_out,e_under_pressure,e_goalkeeper_punched_out,e_shot_saved_off_target,e_goalkeeper_shot_saved_to_post)
                            
                        
                        cur.execute(query,values)
                    case "Error":
                        query = """
                            INSERT INTO error(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            under_pressure)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_under_pressure)
                        cur.execute(query,values)
                    case "Injury Stoppage":
                        query = """
                            INSERT INTO injury_stoppage(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration,
                            in_chain, under_pressure)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                    
                        values = values2 + (e_injury_stoppage_in_chain,e_under_pressure)
                        
                        
                        
                        cur.execute(query,values)
                    case "Bad Behaviour":
                        query = """
                            INSERT INTO bad_behaviour(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration,
                            card_id, card_name)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_bad_behaviour_card_id, e_bad_behaviour_card_name)
                        cur.execute(query,values)
                    case "Substitution":
                        query = """
                            INSERT INTO substitution(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, 
                            outcome_id,outcome_name,replacement_id,replacement_name)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_substitution_outcome_id,e_substitution_outcome_name,e_substitution_replacement_id,e_substitution_replacement_name)
                        cur.execute(query,values)
                    case "Half End":
                        query = """
                            INSERT INTO half_end(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, duration, under_pressure)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values += (e_duration,e_under_pressure)
                        cur.execute(query,values)
                    case "Tactical Shift":
                        query = """
                            INSERT INTO tactical_shift(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, duration)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values += (e_tactics_formation,)
                        cur.execute(query,values)
                    case "50/50":
                        query = """
                            INSERT INTO fifty_fifty(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            counterpress, under_pressure, outcome_id, outcome_name, out)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_counterpress, e_under_pressure, e_50_50_outcome_id,e_50_50_outcome_name,e_out)
                        cur.execute(query,values)
                    case "Offside":
                        query = """
                            INSERT INTO offside(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1])
                        cur.execute(query,values)
                    case "Shield":
                        query = """
                            INSERT INTO shield(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration, location_x, location_y, 
                            under_pressure)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2 + (e_location[0],e_location[1],e_under_pressure)
                        cur.execute(query,values)
                    case "Own Goal For":
                        query = """
                            INSERT INTO own_goal_for(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2
                        cur.execute(query,values)
                    case "Own Goal Against":
                        query = """
                            INSERT INTO own_goal_against(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2
                        cur.execute(query,values)
                    case "Player Off":
                        query = """
                            INSERT INTO player_off(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2
                        cur.execute(query,values)
                    case "Player On":
                        query = """
                            INSERT INTO player_on(event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession,
                            possession_team_id, possession_team_name, team_id, team_name, play_pattern_name, player_id, player_name, 
                            position_id, position_name, duration)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)ON CONFLICT DO NOTHING
                        """
                        
                        values = values2
                        cur.execute(query,values)
