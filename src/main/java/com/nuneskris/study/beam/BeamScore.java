package com.nuneskris.study.beam;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class BeamScore {

    /** A {@link DoFn} that splits lines of text into individual column cells. */
    // The first variable of the doFn is the input and the second variable id the output
    public static class ExtractScore extends DoFn<String, String[]> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] words = c.element().split(",");
            c.output(words);
        }
    }

    private static SimpleDateFormat formatter = new SimpleDateFormat("mm/dd/yy", Locale.ENGLISH);

    public static class ExtractMatchAsObject extends DoFn<String,KV<String, PojoMatch>>{
        @ProcessElement
        public void processElement(ProcessContext c) throws ParseException {
            try {
                String[] lineInArray = c.element().split(",");
                PojoMatch match = PojoMatch.builder()
                        .id(lineInArray[0])
                        .city(lineInArray[1])
                     //   .date(formatter.parse(lineInArray[2]))
                        .player_of_match(lineInArray[3])
                        .venue(lineInArray[4])
                        .neutral_venue(lineInArray[5])
                        .team1(lineInArray[6])
                        .team2(lineInArray[7])
                        .toss_winner(lineInArray[8])
                        .toss_decision(lineInArray[9])
                        .winner(lineInArray[10])
                        .result(lineInArray[11])
                     //   .result_margin((!lineInArray[12].equalsIgnoreCase("NA")) ? new Integer(lineInArray[12]).intValue() : 0)
                        .eliminator(lineInArray[13])
                        .method(lineInArray[14])
                        .umpire1(lineInArray[15])
                        .umpire2(lineInArray[16])
                        .build();

                c.output(KV.of(match.getId(), match));
            }catch(Exception ex){
                ex.printStackTrace();
                System.out.println(c.element());
            }
        }
    }


    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyymmdd");


    public static class ConvertToAvro extends DoFn<PojoScore, AvroScore>{
        @ProcessElement
        public void processElement(ProcessContext c){
            c.output(convertToAvro(c.element()));
        }

        private AvroScore convertToAvro(PojoScore pojoScore) {
            AvroScore avroScore = AvroScore.newBuilder()
                    .setId(pojoScore.getId())
                    .setOver(pojoScore.getOver())
                    .setBall(pojoScore.getBall())
                    .setNonStriker(pojoScore.getNon_striker())
                    .setBowler(pojoScore.getBowler())
                    .setBatsmanRuns(pojoScore.getBatsman_runs())
                    .setInning(pojoScore.getInning())
                    .setBatsman(pojoScore.getBatsman())
                    .setExtraRuns(pojoScore.getExtra_runs())
                    .setTotalRuns(pojoScore.getTotal_runs())
                    .setNonBoundary(pojoScore.getNon_boundary())
                    .setIsWicket(pojoScore.getIs_wicket())
                    .setBattingTeam(pojoScore.getBatting_team())
                    .setBowlingTeam(pojoScore.getBowling_team())
                    .setExtrasType(pojoScore.getExtras_type())
                    .setFielder(pojoScore.getFielder())
                    .setDismissalKind(pojoScore.getDismissal_kind())
                    .setPlayerDismissed(pojoScore.getPlayer_dismissed())
                    .setMatch(convertToAvroMatch(pojoScore.getMatch()))
                    .build();

            return avroScore;
        }

        private AvroMatch convertToAvroMatch(PojoMatch match) {
            AvroMatch avroMatch = AvroMatch.newBuilder()
                    .setPlayerOfMatch(match.getPlayer_of_match())
                    .setCity(match.getCity())

                    .setVenue(match.getVenue())
                    .setId(match.getId())
                    .setNeutralVenue(match.getNeutral_venue())
                    .setEliminator(match.getEliminator())
                    .setTeam1(match.getTeam1())
                    .setTeam2(match.getTeam2())
                    .setTossWinner(match.getToss_winner())
                    .setTossDecision(match.getToss_decision())
                    .setWinner(match.getWinner())
                    .setUmpire1(match.getUmpire1())
                    .setUmpire2(match.getUmpire2())
                    .setResult(match.getResult())
                    .setMethod(match.getMethod())
                    .setResultMargin(1)

                    .build();

            return avroMatch;
        }
    }


    /** A {@link DoFn} that splits lines of text into individual column cells. */
    // The first variable of the doFn is the input and the second variable id the output
    public static class ExtractScoreAsObject extends DoFn<String, PojoScore> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] lineInArray = c.element().split(",");
            PojoScore score = PojoScore.builder()
                    .id(lineInArray[0])
                    .inning(Integer.valueOf(lineInArray[1]))
                    .over(Integer.valueOf(lineInArray[2]))
                    .ball(Integer.valueOf(lineInArray[3]))
                    .batsman(lineInArray[4])
                    .non_striker(lineInArray[5])
                    .bowler(lineInArray[6])
                    .batsman_runs(Integer.valueOf(lineInArray[7]))
                    .extra_runs(Integer.valueOf(lineInArray[8]))
                    .total_runs(Integer.valueOf(lineInArray[9]))
                    .non_boundary(Integer.valueOf(lineInArray[10]))
                    .is_wicket(Integer.valueOf(lineInArray[11]))
                    .dismissal_kind(lineInArray[12])
                    .player_dismissed(lineInArray[13])
                    .fielder(lineInArray[14])
                    .extras_type(lineInArray[15])
                    .batting_team(lineInArray[16])
                    .bowling_team(lineInArray[17])
                    .build();
         c.output(score);

        }
    }


    // We filter by not calling .output for those rows which are filtered out
    public static class FilterWickets extends  DoFn<String[], String[]> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if(c.element()[11].equalsIgnoreCase("1")) {
                c.output(c.element());
            }
        }
    }

    public static class ConvertToKV extends  DoFn<String[], KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] split = {c.element()[11]};
            String key = c.element()[4] + "," + c.element()[12];
            c.output(KV.of(key, new Integer(1)));
        }
    }

    public static class ConvertToKVForBatsman_runs extends  DoFn<PojoScore, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            PojoScore score = c.element();
            String key = score.getId() + "," + score.getBatsman();
            c.output(KV.of(key, score.getBatsman_runs()));
        }
    }

    public static class SumUpValuesByKey extends DoFn<KV<String, Iterable<Integer>>, String>{
        @ProcessElement
        public void processElement(ProcessContext context) {
            int totalWickets = 0;
            String playerAndWicketType = context.element().getKey();
            Iterable<Integer> wickets = context.element().getValue();
            for (Integer amount : wickets) {
                totalWickets += amount.intValue();
            }
            context.output(playerAndWicketType + "," + totalWickets);
        }
    }

    public static class SumUpBatsmanRuns extends DoFn<KV<String, Iterable<Integer>>, KV<String, PojoScore>>{
        @ProcessElement
        public void processElement(ProcessContext context) {
            int totalruns = 0;
            String[] playerAndId= context.element().getKey().split(",");
            Iterable<Integer> runs = context.element().getValue();
            for (Integer amount : runs) {
                totalruns += amount.intValue();
            }
            PojoScore score = PojoScore.builder()
                    .id(playerAndId[0])
                    .batsman(playerAndId[1])
                    .total_runs(totalruns).build();
            context.output(KV.of(score.getId(), score));
        }
    }
}
