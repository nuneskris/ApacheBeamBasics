package com.nuneskris.study.beam;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Builder
@Setter
public class PojoScore implements Serializable {
    private String id;
    private int inning;
    private int over;
    private int ball;
    private String batsman;
    private String non_striker;
    private String bowler;
    private int batsman_runs;
    private int extra_runs;
    private int total_runs;
    private int non_boundary;
    private int is_wicket;
    private String dismissal_kind;
    private String player_dismissed;
    private String fielder;
    private String extras_type;
    private String batting_team;
    private String bowling_team;
    private PojoMatch match;
}