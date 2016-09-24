/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.examples.nativescript.script.stockaggs;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractExecutableScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Combine script from https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-scripted-metric-aggregation.html
 * <p>
 * partial_sum = 0
 * partial_count = 0
 * _agg['partial_count'].each { key, value ->
 *    avg = _agg['partial_sum'].get(key) / value
 *    results.add(avg)
 *    partial_count+= 1
 *    partial_sum += avg
 * }
 * map_result['array'] = results
 * map_result['partial_count'] = partial_count
 * map_result['partial_sum'] = partial_sum
 * return map_result
 */
public class MyCombineScript implements NativeScriptFactory {

    @Override
    @SuppressWarnings("unchecked")
    public ExecutableScript newScript(final @Nullable Map<String, Object> params) {
        // Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
        // HashMap<Long, Double> partial_sum = (HashMap<Long, Double> ) agg.get(MyInitScript.SUM_FIELD);
        // HashMap<Long, Integer> partial_count = (HashMap<Long, Integer>) agg.get(MyInitScript.COUNT_FIELD);
        HashMap<Long, Double> partial_sum = (HashMap<Long, Double>) params.get(MyInitScript.SUM_FIELD);
        HashMap<Long, Integer> partial_count = (HashMap<Long, Integer>) params.get(MyInitScript.COUNT_FIELD);
        double upper_threshold = (double)params.get(MyCombineScript.UPPER_THRESHOLD_KEY);
        double lower_threshold = (double)params.get(MyCombineScript.LOWER_THRESHOLD_KEY);
        return new CombineScript(partial_sum, partial_count, upper_threshold, lower_threshold);
    }

    // Tian: Added in ES 2.x, commented out for ES 1.7
    // @Override
    // public boolean needsScores() {
    //     return false;
    // }
    public static final String COUNT_KEY = "count";
    public static final String SUM_AVG_KEY = "sum_avg";
    public static final String SUM_AVG_SQUARE_KEY = "sum_avg_square";
    public static final String FAVOUR_COUNT_KEY = "favour_count";
    public static final String UNFAVOUR_COUNT_KEY = "unfavour_count";
    public static final String UPPER_THRESHOLD_KEY = "threshold_upper";
    public static final String LOWER_THRESHOLD_KEY = "threshold_lower";


    private static class CombineScript extends AbstractExecutableScript {

        private final HashMap<Long, Double> partial_sum;
        private final HashMap<Long, Integer> partial_count;
        private final double upper_threshold;
        private final double lower_threshold;

        public CombineScript(HashMap<Long, Double> partial_sum, HashMap<Long, Integer> partial_count, double upper_threshold, double lower_threshold) {
            this.partial_sum = partial_sum;
            this.partial_count = partial_count;
            this.upper_threshold = upper_threshold;
            this.lower_threshold = lower_threshold;
        }

        @Override
        public Object run() {
            Map<String, Object> result = new HashMap<String, Object>();
            
            double sum_avg = 0; // Sum of average of person
            double sum_avg_square = 0;  // // Sum of square of an average of  person
            int count = partial_count.size();
            int favour_count = 0;
            int unfavour_count = 0;
            
            for (Map.Entry<Long, Double> entry : partial_sum.entrySet())
            {
                double avg = entry.getValue() / partial_count.get(entry.getKey());
                sum_avg += avg;
                sum_avg_square += (avg * avg);

                if (avg >= upper_threshold) {
                    favour_count += 1;
                }
                else if (avg <= lower_threshold) {
                    unfavour_count += 1;
                }
            }
            // result.put("count", count);
            // result.put("sum_avg", sum_avg);
            // result.put("sum_avg_square", sum_avg_square);
            // result.put("favour_count", favour_count);
            // result.put("unfavour_count", unfavour_count);
            result.put(MyCombineScript.COUNT_KEY, count);
            result.put(MyCombineScript.SUM_AVG_KEY, sum_avg);
            result.put(MyCombineScript.SUM_AVG_SQUARE_KEY, sum_avg_square);
            result.put(MyCombineScript.FAVOUR_COUNT_KEY, favour_count);
            result.put(MyCombineScript.UNFAVOUR_COUNT_KEY, unfavour_count);
            return result;
        }
    }
}
