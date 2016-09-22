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
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Map script from https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-scripted-metric-aggregation.html
 * <p>
 *   Long str_key = _source['nest_source']['long_person_id']
 *   count_item = _agg['partial_count'].get(str_key)
 *   sum_item = _agg['partial_sum'].get(str_key)
 *   if (!count_item) {
 *       count_item = 0
 *       sum_item = 0
 *   }
 *   _agg['partial_count'].put(str_key, count_item + 1)
 *   _agg['partial_sum'].put(str_key, sum_item + _source['double_value'].value)//
 */
public class MyMapScript implements NativeScriptFactory {

    @Override
    @SuppressWarnings("unchecked")
    public ExecutableScript newScript(final @Nullable Map<String, Object> params) {
        // Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
        //HashMap<Long, Double> partial_sum = (HashMap<Long, Double> ) agg.get(MyInitScript.SUM_FIELD);
        //HashMap<Long, Integer> partial_count = (HashMap<Long, Integer>) agg.get(MyInitScript.COUNT_FIELD);
        HashMap<Long, Double> partial_sum = (HashMap<Long, Double> ) params.get(MyInitScript.SUM_FIELD);
        HashMap<Long, Integer> partial_count = (HashMap<Long, Integer>) params.get(MyInitScript.COUNT_FIELD);
        return new MapScript(partial_sum, partial_count);
    }

    // Tian: Added in ES 2.x, commented out for ES 1.7
    // @Override
    // public boolean needsScores() {
    //     return false;
    // }

    public static final String PERSON_KEY = "nest_source.long_person_id";
    public static final String VALUE_KEY = "double_value";

    private static class MapScript extends AbstractSearchScript {

        private final HashMap<Long, Double> partial_sum;
        private final HashMap<Long, Integer> partial_count;

        public MapScript(HashMap<Long, Double> partial_sum, HashMap<Long, Integer> partial_count) {
            this.partial_sum = partial_sum;
            this.partial_count = partial_count;
        }

        @Override
        public Object run() {
            ScriptDocValues.Longs person_id_field = (ScriptDocValues.Longs) doc().get(PERSON_KEY);
            ScriptDocValues.Doubles value_field = (ScriptDocValues.Doubles) doc().get(VALUE_KEY);
            long person_id = person_id_field.getValue();
            double value = value_field.getValue();

            int count = 0;
            double sum = 0;
            if (partial_count.containsKey(person_id)){
                count = partial_count.get(person_id);
                sum = partial_sum.get(person_id);
            }
            sum += value;
            count += 1;
            partial_count.put(person_id, count);
            partial_sum.put(person_id, sum);
            return null;
        }
    }
}
