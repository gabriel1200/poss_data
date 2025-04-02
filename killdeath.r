# ----------------------------------------
# 3. Segment pbp data into individual possessions
# ----------------------------------------

pbp <- pbp %>%
  arrange(game_id, eventnum) %>%
  mutate(
    # Determine which team is on offense based on available description
    offense_team = case_when(
      (!http://is.na(homedescription) & homedescription != "") ~ "home",
      (!http://is.na(visitordescription) & visitordescription != "") ~ "away",
      TRUE ~ NA_character_
    )
  ) %>%
  # Remove rows without a clear offensive indicator
  filter(offense_team %in% c("home", "away")) %>%
  group_by(game_id) %>%
  mutate(possession_id = cumsum(offense_team != lag(offense_team, default = first(offense_team)))) %>%
  ungroup()

# Flag a possession as "scoring" if it contains "PTS" (i.e., a made shot).
possession_outcomes <- pbp %>%
  group_by(game_id, possession_id, offense_team) %>%
  summarize(
    combined_desc = paste(homedescription, visitordescription, sep = " "),
    scoring_possession = if_else(
      grepl("PTS", combined_desc, http://ignore.case = TRUE),
      TRUE, FALSE
    ),
    .groups = "drop"
  )
# -------------------------------------------------------------------
# 5. Calculate consecutive possession streaks to count kills/deaths
# -------------------------------------------------------------------
# Overlapping logic: for a streak of length n, the defense is credited with (n - 2) kills or deaths
# if n >= 3. (Because each additional possession forms a new consecutive block of 3.)

possession_outcomes <- possession_outcomes %>%
  group_by(game_id, offense_team) %>%
  mutate(streak_id = data.table::rleid(scoring_possession)) %>%
  group_by(game_id, offense_team, streak_id, scoring_possession) %>%
  summarize(streak_length = n(), .groups = "drop") %>%
  mutate(
    kill_count = if_else(!scoring_possession & streak_length >= 3, streak_length - 2, 0L),
    death_count = if_else(scoring_possession & streak_length >= 3, streak_length - 2, 0L),
    defensive_team = if_else(offense_team == "home", "away", "home")
  )

game_summary <- possession_outcomes %>%
  group_by(game_id, defensive_team) %>%
  summarize(
    total_kills = sum(kill_count),
    total_deaths = sum(death_count),
    .groups = "drop"
  ) %>%
  mutate(game_id = as.character(game_id)) %>%
  left_join(schedule_lookup %>% select(game_id, home_team_id, away_team_id, winner), by = "game_id") %>%
  mutate(
    team_id = if_else(defensive_team == "home", home_team_id, away_team_id),
    win = if_else(team_id == winner, 1, 0)
  )

team_summary <- game_summary %>%
  group_by(team_id) %>%
  summarize(
    games_played = n(),
    avg_kills = mean(total_kills),
    avg_deaths = mean(total_deaths),
    win_pct_overall = mean(win),
    games_7plus_kills = sum(total_kills >= 7),
    win_pct_7plus_kills = if_else(games_7plus_kills > 0, mean(win[total_kills >= 7]), NA_real_),
    .groups = "drop"
  ) %>%
  left_join(team_lookup %>% mutate(team_id = as.numeric(team_id)), by = "team_id") %>%
  # Convert color strings to valid hex codes
  mutate(
    color = paste0("#", color),
    alternate_color = paste0("#", alternate_color)
  )

p <- team_summary %>%
  ggplot(aes(x = avg_kills, y = avg_deaths)) +
  geom_point(size = 11, stroke = 1.25, shape = 21, alpha = 0.9,
             aes(fill = color, color = after_scale(clr_darken(fill, 0.3)))) +
  geom_text(aes(label = team_abbreviation, color = alternate_color),
            family = "Roboto", hjust = 0.5, size = 3, fontface = 'bold') +
  scale_color_identity() +
  scale_fill_identity() +
  theme_f5() +
  labs(
    x = "Average Kills per Game", 
    y = "Average Deaths per Game", 
    title = "Average Kills vs. Deaths per Game by Team",
    subtitle = "Kill/Death: Overlapping sets of 3 consecutive possessions"
  ) +
  scale_x_continuous(limits = c(0, max(team_summary$avg_kills, na.rm = TRUE) + 1)) +
  scale_y_continuous(limits = c(0, max(team_summary$avg_deaths, na.rm = TRUE) + 1))

print(p)

ggsave("kills_deaths.png", plot = p, height = 5, width = 5, dpi = 600, device = grDevices::png)