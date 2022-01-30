package com.nuneskris.study.beam;


import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

@Getter
@Setter
@Builder
public class PojoMatch implements Serializable {

    private String id;
    private String city;
    private Date date;
    private String player_of_match;
    private String venue;
    private String neutral_venue;
    private String team1;
    private String team2;
    private String toss_winner;
    private String toss_decision;
    private String winner;
    private String result;
    private int result_margin;
    private String eliminator;
    private String method;
    private String umpire1;
    private String umpire2;

}
