package io.vegafusion;

import java.io.IOException;
import java.nio.file.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VegaFusionRuntimeTest {
    @Test
    void testVersion() throws IOException {
        String expectedVersion = new String(Files.readAllBytes(Paths.get("../version.txt"))).trim();
        System.out.println("Expected Version: " + expectedVersion);

        var version = VegaFusionRuntime.version();
        assertEquals(version, expectedVersion);
    }

    @Test
    void testCreate() {
        VegaFusionRuntime runtime = new VegaFusionRuntime();
        runtime.destroy();
    }

    @Test
    void testPatchPretransformedSpec() {
        VegaFusionRuntime runtime = new VegaFusionRuntime();
        String spec1 = "{\"width\": 100, \"height\": 200}";
        String preTransformedSpec1 = "{\"width\": 100, \"height\": 150}";
        String spec2 = "{\"width\": 150, \"height\": 200}";

        String preTransformedSpec2 = runtime.patchPreTransformedSpec(spec1, preTransformedSpec1, spec2);
        System.out.println(preTransformedSpec2);

        assertEquals(preTransformedSpec2, "{\"$schema\":\"https://vega.github.io/schema/vega/v5.json\",\"width\":150,\"height\":150}");
        runtime.destroy();
    }

    @Test
    void testPretransformSpec() {
        VegaFusionRuntime runtime = new VegaFusionRuntime();
        String spec = histSpec();

        VegaFusionRuntime.PreTransformSpecResult preTransformedSpecResult = runtime.preTransformSpec(
                spec, "UTC", "UTC", 0, true
        );
        String preTransformedSpec = preTransformedSpecResult.preTransformedSpec;
        String preTransformedSpecWarnings = preTransformedSpecResult.preTransformWarnings;
        System.out.println(preTransformedSpec);
        System.out.println(preTransformedSpecWarnings);
        runtime.destroy();
    }

    String histSpec() {
        return """
                 {
                   "$schema": "https://vega.github.io/schema/vega/v5.json",
                   "background": "white",
                   "description": "https://vega.github.io/vega-lite/examples/histogram.html",
                   "padding": 5,
                   "width": 200,
                   "height": 200,
                   "style": "cell",
                   "data": [
                     {
                       "name": "source_0",
                       "url": "https://raw.githubusercontent.com/vega/vega-datasets/master/data/movies.json",
                       "format": {"type": "json"},
                       "transform": [
                         {
                           "type": "extent",
                           "field": "IMDB Rating",
                           "signal": "bin_maxbins_10_IMDB_Rating_extent"
                         },
                         {
                           "type": "bin",
                           "field": "IMDB Rating",
                           "as": [
                             "bin_maxbins_10_IMDB Rating",
                             "bin_maxbins_10_IMDB Rating_end"
                           ],
                           "signal": "bin_maxbins_10_IMDB_Rating_bins",
                           "extent": {"signal": "bin_maxbins_10_IMDB_Rating_extent"},
                           "maxbins": 10
                         },
                         {
                           "type": "aggregate",
                           "groupby": [
                             "bin_maxbins_10_IMDB Rating",
                             "bin_maxbins_10_IMDB Rating_end"
                           ],
                           "ops": ["count"],
                           "fields": [null],
                           "as": ["__count"]
                         },
                         {
                           "type": "filter",
                           "expr": "isValid(datum[\\"bin_maxbins_10_IMDB Rating\\"]) && isFinite(+datum[\\"bin_maxbins_10_IMDB Rating\\"])"
                         }
                       ]
                     }
                   ],
                   "marks": [
                     {
                       "name": "marks",
                       "type": "rect",
                       "style": ["bar"],
                       "from": {"data": "source_0"},
                       "encode": {
                         "update": {
                           "fill": {"value": "#4c78a8"},
                           "ariaRoleDescription": {"value": "bar"},
                           "description": {
                             "signal": "\\"IMDB Rating (binned): \\" + (!isValid(datum[\\"bin_maxbins_10_IMDB Rating\\"]) || !isFinite(+datum[\\"bin_maxbins_10_IMDB Rating\\"]) ? \\"null\\" : format(datum[\\"bin_maxbins_10_IMDB Rating\\"], \\"\\") + \\" – \\" + format(datum[\\"bin_maxbins_10_IMDB Rating_end\\"], \\"\\")) + \\"; Count of Records: \\" + (format(datum[\\"__count\\"], \\"\\"))"
                           },
                           "x2": [
                             {
                               "test": "!isValid(datum[\\"bin_maxbins_10_IMDB Rating\\"]) || !isFinite(+datum[\\"bin_maxbins_10_IMDB Rating\\"])",
                               "value": 0
                             },
                             {"scale": "x", "field": "bin_maxbins_10_IMDB Rating", "offset": 1}
                           ],
                           "x": [
                             {
                               "test": "!isValid(datum[\\"bin_maxbins_10_IMDB Rating\\"]) || !isFinite(+datum[\\"bin_maxbins_10_IMDB Rating\\"])",
                               "value": 0
                             },
                             {"scale": "x", "field": "bin_maxbins_10_IMDB Rating_end"}
                           ],
                           "y": {"scale": "y", "field": "__count"},
                           "y2": {"scale": "y", "value": 0}
                         }
                       }
                     }
                   ],
                   "scales": [
                     {
                       "name": "x",
                       "type": "linear",
                       "domain": {
                         "signal": "[bin_maxbins_10_IMDB_Rating_bins.start, bin_maxbins_10_IMDB_Rating_bins.stop]"
                       },
                       "range": [0, {"signal": "width"}],
                       "bins": {"signal": "bin_maxbins_10_IMDB_Rating_bins"},
                       "zero": false
                     },
                     {
                       "name": "y",
                       "type": "linear",
                       "domain": {"data": "source_0", "field": "__count"},
                       "range": [{"signal": "height"}, 0],
                       "nice": true,
                       "zero": true
                     }
                   ],
                   "axes": [
                     {
                       "scale": "y",
                       "orient": "left",
                       "gridScale": "x",
                       "grid": true,
                       "tickCount": {"signal": "ceil(height/40)"},
                       "domain": false,
                       "labels": false,
                       "aria": false,
                       "maxExtent": 0,
                       "minExtent": 0,
                       "ticks": false,
                       "zindex": 0
                     },
                     {
                       "scale": "x",
                       "orient": "bottom",
                       "grid": false,
                       "title": "IMDB Rating (binned)",
                       "labelFlush": true,
                       "labelOverlap": true,
                       "tickCount": {"signal": "ceil(width/10)"},
                       "zindex": 0
                     },
                     {
                       "scale": "y",
                       "orient": "left",
                       "grid": false,
                       "title": "Count of Records",
                       "labelOverlap": true,
                       "tickCount": {"signal": "ceil(height/40)"},
                       "zindex": 0
                     }
                   ]
                 }
                """;
    }
}
