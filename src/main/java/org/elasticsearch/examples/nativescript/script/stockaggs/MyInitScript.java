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
import java.util.Map;
import java.util.HashMap;

/**
 * Init script from https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-scripted-metric-aggregation.html
 *
 *_agg['partial_sum'] = new HashMap<Long, Float>(100000)
 *_agg['partial_count'] = new HashMap<Long, Integer>(100000)
 */
public class MyInitScript implements NativeScriptFactory {

    public static final String SUM_FIELD = "sum";
    public static final String COUNT_FIELD = "count";

    @Override
    @SuppressWarnings("unchecked")
    public ExecutableScript newScript(final @Nullable Map<String, Object> params) {
        return new AbstractExecutableScript() {
            @Override
            public Object run() {
                // Params is argument that contains the input args
                //((Map<String, Object>)params.get("_agg")).put(SUM_FIELD, new HashMap<Long, Double>(100000));
                //((Map<String, Object>)params.get("_agg")).put(COUNT_FIELD, new HashMap<Long, Integer>(100000));
                params.put(SUM_FIELD, new HashMap<Long, Double>(100000));
                params.put(COUNT_FIELD, new HashMap<Long, Integer>(100000));
                return null;
            }
        };
    }

    // Tian: Added in ES 2.x, commented out for ES 1.7
    // @Override
    // public boolean needsScores() {
    //     return false;
    // }
}
