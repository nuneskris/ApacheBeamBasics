package com.nuneskris.study.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * batch pipeline.
 * Input file(s)
 * should be a text log in the following format:
 * "id","inning","over","ball","batsman","non_striker","bowler","batsman_runs","extra_runs","total_runs","non_boundary","is_wicket","dismissal_kind","player_dismissed","fielder","extras_type","batting_team","bowling_team"
 */
public class HelloBeam {

    public static final void main(String args[]) throws Exception {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> input = pipeline.apply(TextIO.read().from("/Users/krisnunes/Desktop/Study/archive/IPLBall-by-Ball 2008-2020.csv"));

        // ******** Function: MapElements *************************
        PCollection<String[]> parse =
                input.apply(
                        "parse",
                        MapElements.via(
                                new SimpleFunction<String, String[]>() {
                                    @Override
                                    public String[] apply(String input) {
                                        String[] split = input.split(",");
                                        return split;
                                    }
                                }));

      // ******** Function: Filter *************************
        PCollection<String[]> filterWickets = parse.apply("filterWickets",
                (Filter.by(new SerializableFunction<String[], Boolean>() {
                    @Override
                    public Boolean apply(String[] input) {
                        return input[11].equalsIgnoreCase("1") ;
                    }
                })));

        // ******** Class: KV *************************
        PCollection<KV<String, Integer>> convertToKV =
                filterWickets.apply(
                        "convertToKV",
                        MapElements.via(
                                new SimpleFunction<String[], KV<String, Integer>>() {
                                    @Override
                                    public KV<String, Integer> apply(String[] input) {
                                        String[] split = {input[11]};
                                        String key = input[4]+","+input[12];
                                        return KV.of(key, new Integer(1));
                                    }
                                }));

        // ******** Function: GroupByKey *************************
        PCollection<KV<String, Iterable<Integer>>> kvpCollection =
                convertToKV.apply(
                        GroupByKey.<String, Integer>create()
                );


        // ******** Function: ParDo *************************
        PCollection<String> sumUpValuesByKey =
                kvpCollection.apply(
                        "SumUpValuesByKey",
                        ParDo.of(
                                new DoFn<KV<String, Iterable<Integer>>, String>() {

                                    @ProcessElement
                                    public void processElement(ProcessContext context) {
                                        Integer totalWickets = 0;
                                        String playerAndWicketType = context.element().getKey();
                                        Iterable<Integer> wickets = context.element().getValue();
                                        for (Integer amount : wickets) {
                                            totalWickets += amount;
                                        }
                                        context.output(playerAndWicketType + "," + totalWickets);
                                    }
                                }));
        sumUpValuesByKey.apply(TextIO.write().to("/Users/krisnunes/Desktop/Study/archive/IPLOuts.csv").withoutSharding());
        pipeline.run();
    }
}