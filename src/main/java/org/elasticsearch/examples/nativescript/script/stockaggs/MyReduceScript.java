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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Combine script from https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-scripted-metric-aggregation.html
 * <p>
 * for (a in _aggs) {
 *   // Skip the null from shard return non result
 *   if (a) {
 *       final_result += a['array']
 *       sum += a['partial_sum']
 *       count += a['partial_count']
 *   }
 * }
 */
public class MyReduceScript implements NativeScriptFactory {

    @Override
    @SuppressWarnings("unchecked")
    public ExecutableScript newScript(final @Nullable Map<String, Object> params) {
        final ArrayList<HashMap<String, Object>> aggs = (ArrayList<HashMap<String, Object>>) params.get("_aggs");
        return new ReduceScript(aggs);
    }

    // Tian: Added in ES 2.x, commented out for ES 1.7
    // @Override
    // public boolean needsScores() {
    //     return false;
    // }

    private static class ReduceScript extends AbstractExecutableScript {

        private final ArrayList<HashMap<String, Object>> aggs;

        public ReduceScript(ArrayList<HashMap<String, Object>> aggs) {
            this.aggs = aggs;
        }

        @Override
        public Object run() {
            double sumAvgSquare = 0;
            double sumAvg = 0;
            int count = 0;
            int favour_count = 0;
            int unfavour_count = 0;

            for (HashMap<String, Object> t : aggs) {
                if (t != null) {
                    // count += (int)t.get("count");
                    // sumAvg += (double)t.get("sum_avg");
                    // sumAvgSquare += (double)t.get("sum_avg_square");
                    // favour_count += (int)t.get("favour_count");
                    // unfavour_count += (int)t.get("unfavour_count");
                    count += (int)t.get(MyCombineScript.COUNT_KEY);
                    sumAvg += (double)t.get(MyCombineScript.SUM_AVG_KEY);
                    sumAvgSquare += (double)t.get(MyCombineScript.SUM_AVG_SQUARE_KEY);
                    favour_count += (int)t.get(MyCombineScript.FAVOUR_COUNT_KEY);
                    unfavour_count += (int)t.get(MyCombineScript.UNFAVOUR_COUNT_KEY);
                }
            }
            double mean = sumAvg / count;
            double SSTotal = sumAvgSquare + (count * (mean * mean)) - (2 * mean * sumAvg);
            double std = Math.sqrt(SSTotal / count);

            HashMap<String, Object> result = new HashMap<String, Object>();
            result.put("mean", mean);
            result.put("sum_of_square", sumAvgSquare);
            result.put("std_dev", std);
            result.put("count", count);
            result.put("favourable_count", favour_count);
            result.put("unfavourable_count", unfavour_count);
            return result;
        }
    }
}
