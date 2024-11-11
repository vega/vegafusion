# Supported Transforms

VegaFusion implements a subset of [Vega's transforms](https://vega.github.io/vega/). Below is a detailed breakdown of transform support status.

:::{note}

When a Vega spec includes unsupported transforms, these transforms will be included in the client Vega spec for evaluation by the standard Vega library in the browser. See [How it works](./how_it_works.md) for more details.

:::

> **Key**:
> - ✅: Full support for the transform and all its options
> - 🟡: Partial support with limitations
> - ❌: Transform is not currently supported

## Basic Transforms

:::{list-table}
:header-rows: 1
:widths: 40 10 100

*   - Transform
    - Status
    - Details
*   - [aggregate](https://vega.github.io/vega/docs/transforms/aggregate/)
    - 🟡
    - :::{dropdown} More Info
      Supported aggregate `ops`:
      - `count`
      - `valid`
      - `missing`
      - `distinct`
      - `sum`
      - `mean` or `average`
      - `variance`
      - `variancep`
      - `stdev`
      - `stdevp`
      - `median`
      - `q1`
      - `q3`
      - `min`
      - `max`

      Unsupported aggregate `ops`:
      - `product`
      - `ci0`
      - `ci1`
      - `stderr`
      - `argmin`
      - `argmax`

      The `cross`, `drop`, and `key` options are not yet supported.
      :::
*   - [formula](https://vega.github.io/vega/docs/transforms/formula/)
    - ✅
    - :::{dropdown} More Info
      See [supported expressions](./supported_expressions.md) for details on supported
      expression features that may be used in the `expr` field.
      :::
*   - [window](https://vega.github.io/vega/docs/transforms/window/)
    - 🟡
    - :::{dropdown} More Info
      Supported window operations:
      - Ranking operations:
        - `row_number`
        - `rank`
        - `dense_rank`
        - `percent_rank`
        - `cume_dist`
      - Value operations:
        - `first_value`
        - `last_value`
      - All supported aggregate transform ops

      Not yet supported:
      - `ntile`
      - `lag`/`lead`
      - `nth_value`
      - `prev_value`/`next_value`

      All frame specifications and peer handling options are supported.
      :::
*   - [lookup](https://vega.github.io/vega/docs/transforms/lookup/)
    - ❌
    - <p></p>
*   - [bin](https://vega.github.io/vega/docs/transforms/bin/)
    - ✅
    - :::{dropdown} More Info
      `interval` option ignored and both bin edges are always included.
      :::
*   - [collect](https://vega.github.io/vega/docs/transforms/collect/)
    - ✅
    - <p></p>
*   - [extent](https://vega.github.io/vega/docs/transforms/extent/)
    - ✅
    - <p></p>
*   - [filter](https://vega.github.io/vega/docs/transforms/filter/)
    - ✅
    - :::{dropdown} More Info
      See [supported expressions](./supported_expressions.md) for details on supported
      expression features that may be used in the `expr` field.
      :::
*   - [flatten](https://vega.github.io/vega/docs/transforms/flatten/)
    - ❌
    - :::{dropdown} More Info
      May be possible in the future with DataFusion's 
      [unnest](https://github.com/apache/datafusion/pull/10044) transform.
      :::
*   - [fold](https://vega.github.io/vega/docs/transforms/fold/)
    - ✅
    - <p></p>
*   - [project](https://vega.github.io/vega/docs/transforms/project/)
    - ✅
    - <p></p>
*   - [sequence](https://vega.github.io/vega/docs/transforms/sequence/)
    - ✅
    - <p></p>
*   - [timeunit](https://vega.github.io/vega/docs/transforms/timeunit/)
    - 🟡
    - :::{dropdown} More Info
      `interval` option ignored and both bin edges are always included.

      `step`, `extent`, and `maxbins` options are not supported, so `units` must be
      provided.
      :::
*   - [countpattern](https://vega.github.io/vega/docs/transforms/countpattern/)
    - ❌
    - <p></p>
*   - [cross](https://vega.github.io/vega/docs/transforms/cross/)
    - ❌
    - <p></p>
*   - [density](https://vega.github.io/vega/docs/transforms/density/)
    - ❌
    - <p></p>
*   - [dotbin](https://vega.github.io/vega/docs/transforms/dotbin/)
    - ❌
    - <p></p>
*   - [identifier](https://vega.github.io/vega/docs/transforms/identifier/)
    - ✅
    - <p></p>
*   - [impute](https://vega.github.io/vega/docs/transforms/impute/)
    - 🟡
    - :::{dropdown} More Info
      Supported:
      - `field` - Field to impute
      - `key` - Unique identifier field
      - `value` - Custom value for imputation
      - `groupby` - Group-based imputation
    
      Unsupported:
      - `method` parameter with options other than `value`
      - `keyvals` array for custom key values
      :::
*   - [kde](https://vega.github.io/vega/docs/transforms/kde/)
    - ❌
    - <p></p>
*   - [loess](https://vega.github.io/vega/docs/transforms/loess/)
    - ❌
    - <p></p>
*   - [pivot](https://vega.github.io/vega/docs/transforms/pivot/)
    - ✅
    - <p></p>
*   - [quantile](https://vega.github.io/vega/docs/transforms/quantile/)
    - ❌
    - <p></p>
*   - [regression](https://vega.github.io/vega/docs/transforms/regression/)
    - ❌
    - <p></p>
*   - [sample](https://vega.github.io/vega/docs/transforms/sample/)
    - ❌
    - <p></p>
:::

## Geographic and Spatial Transforms

:::{list-table}
:header-rows: 1
:widths: 20 10 100

*   - Transform
    - Status
    - Details
*   - [contour](https://vega.github.io/vega/docs/transforms/contour/)
    - ❌
    - <p></p>
*   - [geojson](https://vega.github.io/vega/docs/transforms/geojson/)
    - ❌
    - <p></p>
*   - [geopath](https://vega.github.io/vega/docs/transforms/geopath/)
    - ❌
    - <p></p>
*   - [geopoint](https://vega.github.io/vega/docs/transforms/geopoint/)
    - ❌
    - <p></p>
*   - [geoshape](https://vega.github.io/vega/docs/transforms/geoshape/)
    - ❌
    - <p></p>
*   - [graticule](https://vega.github.io/vega/docs/transforms/graticule/)
    - ❌
    - <p></p>
*   - [heatmap](https://vega.github.io/vega/docs/transforms/heatmap/)
    - ❌
    - <p></p>
*   - [isocontour](https://vega.github.io/vega/docs/transforms/isocontour/)
    - ❌
    - <p></p>
*   - [kde2d](https://vega.github.io/vega/docs/transforms/kde2d/)
    - ❌
    - <p></p>
:::

## Layout Transforms

:::{list-table}
:header-rows: 1
:widths: 20 10 100

*   - Transform
    - Status
    - Details
*   - [stack](https://vega.github.io/vega/docs/transforms/stack/)
    - ✅
    - <p></p>
*   - [force](https://vega.github.io/vega/docs/transforms/force/)
    - ❌
    - <p></p>
*   - [label](https://vega.github.io/vega/docs/transforms/label/)
    - ❌
    - <p></p>
*   - [linkpath](https://vega.github.io/vega/docs/transforms/linkpath/)
    - ❌
    - <p></p>
*   - [pie](https://vega.github.io/vega/docs/transforms/pie/)
    - ❌
    - <p></p>
*   - [voronoi](https://vega.github.io/vega/docs/transforms/voronoi/)
    - ❌
    - <p></p>
*   - [wordcloud](https://vega.github.io/vega/docs/transforms/wordcloud/)
    - ❌
    - <p></p>
:::

## Hierarchy Transforms

:::{list-table}
:header-rows: 1
:widths: 20 10 100

*   - Transform
    - Status
    - Details
*   - [nest](https://vega.github.io/vega/docs/transforms/nest/)
    - ❌
    - <p></p>
*   - [stratify](https://vega.github.io/vega/docs/transforms/stratify/)
    - ❌
    - <p></p>
*   - [treelinks](https://vega.github.io/vega/docs/transforms/treelinks/)
    - ❌
    - <p></p>
*   - [pack](https://vega.github.io/vega/docs/transforms/pack/)
    - ❌
    - <p></p>
*   - [partition](https://vega.github.io/vega/docs/transforms/partition/)
    - ❌
    - <p></p>
*   - [tree](https://vega.github.io/vega/docs/transforms/tree/)
    - ❌
    - <p></p>
*   - [treemap](https://vega.github.io/vega/docs/transforms/treemap/)
    - ❌
    - <p></p>
:::

## Cross-Filter Transforms

:::{list-table}
:header-rows: 1
:widths: 20 10 100

*   - Transform
    - Status
    - Details
*   - [crossfilter](https://vega.github.io/vega/docs/transforms/crossfilter/)
    - ❌
    - <p></p>
*   - [resolvefilter](https://vega.github.io/vega/docs/transforms/resolvefilter/)
    - ❌
    - <p></p>
:::
