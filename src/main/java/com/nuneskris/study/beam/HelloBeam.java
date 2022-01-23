package com.nuneskris.study.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * batch pipeline.
 * Input file(s)
 * This project can be run locally.
 * should be a text log in the following format:
 * "id","inning","over","ball","batsman","non_striker","bowler","batsman_runs","extra_runs","total_runs","non_boundary","is_wicket","dismissal_kind","player_dismissed","fielder","extras_type","batting_team","bowling_team"
 */
public class HelloBeam {


    private static PCollection<String> getLocalData(Pipeline pipeline) {
        return pipeline.apply(TextIO.read().from("/Users/krisnunes/Desktop/Study/archive/IPLBall-by-Ball 2008-2020.csv"));
    }
    // ******** Function: MapElements *************************
    private static PCollection<String[]> parseMapElementsSplitString(PCollection<String> input) {
        return
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
    }


    // ******** Function: Filter *************************
    private static PCollection<String[]> filterWickets(PCollection<String[]> input) {
        return input.apply("filterWickets",
                (Filter.by(new SerializableFunction<String[], Boolean>() {
                    @Override
                    public Boolean apply(String[] input) {
                        return input[11].equalsIgnoreCase("1");
                    }
                })));
    }



    // ******** Class: KV *************************
    private static PCollection<KV<String, Integer>> convertToKV(PCollection<String[]> filterWickets){
            return filterWickets.apply(
                    "convertToKV",
                    MapElements.via(
                    new SimpleFunction<String[], KV<String, Integer>>()
                {
                    @Override
                    public KV<String, Integer> apply (String[]input){
                        String[] split = {input[11]};
                        String key = input[4] + "," + input[12];
                    return KV.of(key, new Integer(1));
                }
                }));
    }



    // ******** Function: GroupByKey *************************
   private static PCollection<KV<String, Iterable<Integer>>> groupByKeysOfKV(PCollection<KV<String, Integer>>  convertToKV) {
       return convertToKV.apply(
                GroupByKey.<String, Integer>create()
        );
    }

    // ******** Function: ParDo *************************
    //we can have a pardo function inside if we not want to reuse it.
    private static PCollection<String> sumUpValuesByKey(PCollection<KV<String, Iterable<Integer>>> kvpCollection){
        return    kvpCollection.apply(
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

    }



    private static void writeLocally(PCollection<String> sumUpValuesByKey){
        sumUpValuesByKey.apply(TextIO.write().to("/Users/krisnunes/Desktop/Study/archive/IPLOuts.csv").withoutSharding());
    }

    private static void processLocalViaPardo(){
        System.out.println("processLocalViaPardo");
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(TextIO.read().from("/Users/krisnunes/Desktop/Study/archive/IPLBall-by-Ball 2008-2020.csv"))
                .apply(ParDo.of(new BeamScore.ExtractScore()))
                .apply(ParDo.of(new BeamScore.FilterWickets()))
                .apply(ParDo.of(new BeamScore.ConvertToKV()))
                .apply(GroupByKey.<String, Integer>create())
                .apply(ParDo.of(new BeamScore.SumUpValuesByKey()))
                .apply(TextIO.write().to("/Users/krisnunes/Desktop/Study/archive/IPLOutsParDo.csv").withoutSharding())
                ;
        pipeline.run();
    }

    private static void processLocal(){
        Pipeline pipeline = Pipeline.create();

        writeLocally(
           sumUpValuesByKey(
               groupByKeysOfKV(
                   convertToKV(
                       filterWickets(
                           parseMapElementsSplitString(
                               getLocalData(pipeline)))))));



        pipeline.run();
    }
    public static final void main(String args[]) throws Exception {
       // processLocal();
        processLocalViaPardo();
    }
}