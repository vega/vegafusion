# Supported Expressions

VegaFusion supports a subset of [Vega's expression language](https://vega.github.io/vega/docs/expressions/). Below is a detailed breakdown of supported expression features.

:::{note}
When a Vega spec includes unsupported expressions, these expressions will be included in the client Vega spec for evaluation by the standard Vega library in the browser. See [How it works](./how_it_works.md) for more details.
:::

> **Key**:
> - ✅: Full support for the feature and all its options
> - 🟡: Partial support with limitations
> - ❌: Feature is not currently supported

## Core Language Features

:::{list-table}
:header-rows: 1
:widths: 30 10 100

*   - Feature
    - Status
    - Details
*   - `a + b`
    - ✅
    - <p></p>
*   - `a - b`
    - ✅
    - <p></p>
*   - `a * b`
    - ✅
    - <p></p>
*   - `a / b`
    - ✅
    - <p></p>
*   - `a % b`
    - ✅
    - <p></p>
*   - `-a`
    - ✅
    - <p></p>
*   - `+a`
    - ✅
    - <p></p>
*   - `a == b`
    - ✅
    - <p></p>
*   - `a != b`
    - ✅
    - <p></p>
*   - `a < b`
    - ✅
    - <p></p>
*   - `a > b`
    - ✅
    - <p></p>
*   - `a <= b`
    - ✅
    - <p></p>
*   - `a >= b`
    - ✅
    - <p></p>
*   - `a && b`
    - ✅
    - <p></p>
*   - `a || b`
    - ✅
    - <p></p>
*   - `!a`
    - ✅
    - <p></p>
*   - `a ? b : c`
    - ✅
    - <p></p>
*   - `{a: 1, b: 2}`
    - ✅
    - <p></p>
*   - `[1, 2, 3]`
    - ✅
    - <p></p>
*   - `a.b`
    - ✅
    - <p></p>
*   - `a["b"]`
    - ✅
    - <p></p>
:::

## Bound Variables
:::{list-table}
:header-rows: 1
:widths: 30 10 100

*   - Variable
    - Status
    - Details
*   - `datum`
    - ✅
    - <p></p>
*   - `event`
    - ❌
    - <p></p>
*   - `item`
    - ❌
    - <p></p>
*   - `parent`
    - ❌
    - <p></p>
*   - Signal Names
    - ✅
    - <p></p>
:::

## Constants

:::{list-table}
:header-rows: 1
:widths: 20 10 100

*   - Constant
    - Status
    - Details
*   - `NaN`
    - ✅
    - <p></p>
*   - `E`
    - ✅
    - <p></p>
*   - `LN2`
    - ✅
    - <p></p>
*   - `LN10`
    - ✅
    - <p></p>
*   - `LOG2E`
    - ✅
    - <p></p>
*   - `LOG10E`
    - ✅
    - <p></p>
*   - `MAX_VALUE`
    - ✅
    - <p></p>
*   - `MIN_VALUE`
    - ✅
    - <p></p>
*   - `PI`
    - ✅
    - <p></p>
*   - `SQRT1_2`
    - ✅
    - <p></p>
*   - `SQRT2`
    - ✅
    - <p></p>
:::

## Type Checking

:::{list-table}
:header-rows: 1
:widths: 40 10 100

*   - Function
    - Status
    - Details
*   - `isArray(value)`
    - ❌
    - <p></p>
*   - `isBoolean(value)`
    - ❌
    - <p></p>
*   - `isDate(value)`
    - ✅
    - <p></p>
*   - `isDefined(value)`
    - ❌
    - <p></p>
*   - `isNumber(value)`
    - ❌
    - <p></p>
*   - `isObject(value)`
    - ❌
    - <p></p>
*   - `isRegExp(value)`
    - ❌
    - <p></p>
*   - `isString(value)`
    - ❌
    - <p></p>
*   - `isValid(value)`
    - ✅
    - <p></p>
:::

## Type Coercion

:::{list-table}
:header-rows: 1
:widths: 40 10 100

*   - Function
    - Status
    - Details
*   - `toBoolean(value)`
    - ✅
    - <p></p>
*   - `toDate(value)`
    - ✅
    - <p></p>
*   - `toNumber(value)`
    - ✅
    - <p></p>
*   - `toString(value)`
    - ✅
    - <p></p>
:::

## Control Flow
:::{list-table}
:header-rows: 1
:widths: 80 10 100

*   - Function
    - Status
    - Details
*   - `if(test, thenValue, elseValue)`
    - ✅
    - <p></p>
:::


## Math Functions

:::{list-table}
:header-rows: 1
:widths: 50 10 100

*   - Function
    - Status
    - Details
*   - `isNaN(value)`
    - ✅
    - <p></p>
*   - `isFinite(value)`
    - ✅
    - <p></p>
*   - `abs(value)`
    - ✅
    - <p></p>
*   - `acos(value)`
    - ✅
    - <p></p>
*   - `asin(value)`
    - ✅
    - <p></p>
*   - `atan(value)`
    - ✅
    - <p></p>
*   - `atan2(dy, dx)`
    - ❌
    - <p></p>
*   - `ceil(value)`
    - ✅
    - <p></p>
*   - `clamp(value, min, max)`
    - ❌
    - <p></p>
*   - `cos(value)`
    - ✅
    - <p></p>
*   - `exp(exponent)`
    - ✅
    - <p></p>
*   - `floor(value)`
    - ✅
    - <p></p>
*   - `hypot(value)`
    - ❌
    - <p></p>
*   - `log(value)`
    - ✅
    - <p></p>
*   - `max(value1, value2, …)`
    - ❌
    - <p></p>
*   - `min(value1, value2, …)`
    - ❌
    - <p></p>
*   - `pow(value, exponent)`
    - ✅
    - <p></p>
*   - `random()`
    - ❌
    - <p></p>
*   - `round(value)`
    - ✅
    - <p></p>
*   - `sin(value)`
    - ✅
    - <p></p>
*   - `sqrt(value)`
    - ✅
    - <p></p>
*   - `tan(value)`
    - ✅
    - <p></p>
:::

## Statistical Functions
:::{list-table}
:header-rows: 1
:widths: 80 10 40

*   - Function
    - Status
    - Details
*   - `sampleNormal([mean, stdev])`
    - ❌
    - <p></p>
*   - `cumulativeNormal(value[, mean, stdev])`
    - ❌
    - <p></p>
*   - `densityNormal(value[, mean, stdev])`
    - ❌
    - <p></p>
*   - `quantileNormal(probability[, mean, stdev])`
    - ❌
    - <p></p>
*   - `sampleLogNormal([mean, stdev])`
    - ❌
    - <p></p>
*   - `cumulativeLogNormal(value[, mean, stdev])`
    - ❌
    - <p></p>
*   - `densityLogNormal(value[, mean, stdev])`
    - ❌
    - <p></p>
*   - `quantileLogNormal(probability[, mean, stdev])`
    - ❌
    - <p></p>
*   - `sampleUniform([min, max])`
    - ❌
    - <p></p>
*   - `cumulativeUniform(value[, min, max])`
    - ❌
    - <p></p>
*   - `densityUniform(value[, min, max])`
    - ❌
    - <p></p>
*   - `quantileUniform(probability[, min, max])`
    - ❌
    - <p></p>
:::

## Date-Time Functions

:::{list-table}
:header-rows: 1
:widths: 120 10 40

*   - Function
    - Status
    - Details
*   - `now()`
    - ❌
    - <p></p>
*   - `datetime(year, month[, day, hour, min, sec, millisec])`
    - ✅
    - <p></p>
*   - `date(datetime)`
    - ✅
    - <p></p>
*   - `day(datetime)`
    - ✅
    - <p></p>
*   - `dayofyear(datetime)`
    - ✅
    - <p></p>
*   - `year(datetime)`
    - ✅
    - <p></p>
*   - `quarter(datetime)`
    - ✅
    - <p></p>
*   - `month(datetime)`
    - ✅
    - <p></p>
*   - `week(datetime)`
    - ❌
    - <p></p>
*   - `hours(datetime)`
    - ✅
    - <p></p>
*   - `minutes(datetime)`
    - ✅
    - <p></p>
*   - `seconds(datetime)`
    - ✅
    - <p></p>
*   - `milliseconds(datetime)`
    - ✅
    - <p></p>
*   - `time(datetime)`
    - ✅
    - <p></p>
*   - `timezoneoffset(datetime)`
    - ❌
    - <p></p>
*   - `timeOffset(unit, date[, step])`
    - ✅
    - <p></p>
*   - `timeSequence(unit, start, stop[, step])`
    - ❌
    - <p></p>
*   - `utc(year, month[, day, hour, min, sec, millisec])`
    - ✅
    - <p></p>
*   - `utcdate(datetime)`
    - ✅
    - <p></p>
*   - `utcday(datetime)`
    - ✅
    - <p></p>
*   - `utcdayofyear(datetime)`
    - ✅
    - <p></p>
*   - `utcyear(datetime)`
    - ✅
    - <p></p>
*   - `utcquarter(datetime)`
    - ✅
    - <p></p>
*   - `utcmonth(datetime)`
    - ✅
    - <p></p>
*   - `utcweek(datetime)`
    - ❌
    - <p></p>
*   - `utchours(datetime)`
    - ✅
    - <p></p>
*   - `utcminutes(datetime)`
    - ✅
    - <p></p>
*   - `utcseconds(datetime)`
    - ✅
    - <p></p>
*   - `utcmilliseconds(datetime)`
    - ✅
    - <p></p>
*   - `utcOffset(unit, date[, step])`
    - ❌
    - <p></p>
*   - `utcSequence(unit, start, stop[, step])`
    - ❌
    - <p></p>
:::

## Array Functions

:::{list-table}
:header-rows: 1
:widths: 80 10 100

*   - Function
    - Status
    - Details
*   - `extent(array)`
    - ❌
    - <p></p>
*   - `clampRange(range, min, max)`
    - ❌
    - <p></p>
*   - `indexof(array, value)`
    - ✅
    - <p></p>
*   - `inrange(value, range)`
    - ❌
    - <p></p>
*   - `join(array[, separator])`
    - ❌
    - <p></p>
*   - `lastindexof(array, value)`
    - ❌
    - <p></p>
*   - `length(array)`
    - ✅
    - <p></p>
*   - `lerp(array, fraction)`
    - ❌
    - <p></p>
*   - `peek(array)`
    - ❌
    - <p></p>
*   - `pluck(array, field)`
    - ❌
    - <p></p>
*   - `reverse(array)`
    - ❌
    - <p></p>
*   - `sequence([start, ]stop[, step])`
    - ❌
    - <p></p>
*   - `slice(array, start[, end])`
    - ❌
    - <p></p>
*   - `sort(array)`
    - ❌
    - <p></p>
*   - `span(array)`
    - ✅
    - <p></p>
:::

## String Functions

:::{list-table}
:header-rows: 1
:widths: 70 10 40

*   - Function
    - Status
    - Details
*   - `indexof(string, substring)`
    - ❌
    - <p></p>
*   - `lastindexof(string, substring)`
    - ❌
    - <p></p>
*   - `length(string)`
    - ❌
    - <p></p>
*   - `lower(string)`
    - ❌
    - <p></p>
*   - `pad(string, length[, character, align])`
    - ❌
    - <p></p>
*   - `parseFloat(string)`
    - ❌
    - <p></p>
*   - `parseInt(string)`
    - ❌
    - <p></p>
*   - `replace(string, pattern, replacement)`
    - ❌
    - <p></p>
*   - `slice(string, start[, end])`
    - ❌
    - <p></p>
*   - `split(string, separator[, limit])`
    - ❌
    - <p></p>
*   - `substring(string, start[, end])`
    - ❌
    - <p></p>
*   - `trim(string)`
    - ❌
    - <p></p>
*   - `truncate(string, length[, align, ellipsis])`
    - ❌
    - <p></p>
*   - `upper(string)`
    - ❌
    - <p></p>
:::


## Object Functions
:::{list-table}
:header-rows: 1
:widths: 100 10 60

*   - Function
    - Status
    - Details
*   - `merge(object1[, object2, …])`
    - ❌
    - <p></p>
:::

## Formatting Functions
:::{list-table}
:header-rows: 1
:widths: 80 10 60

*   - Function
    - Status
    - Details
*   - `dayFormat(day)`
    - ❌
    - <p></p>
*   - `dayAbbrevFormat(day)`
    - ❌
    - <p></p>
*   - `format(value, specifier)`
    - ✅
    - <p></p>
*   - `monthFormat(month)`
    - ❌
    - <p></p>
*   - `monthAbbrevFormat(month)`
    - ❌
    - <p></p>
*   - `timeUnitSpecifier(units[, specifiers])`
    - ❌
    - <p></p>
*   - `timeFormat(value, specifier)`
    - ✅
    - <p></p>
*   - `timeParse(string, specifier)`
    - ❌
    - <p></p>
*   - `utcFormat(value, specifier)`
    - ✅
    - <p></p>
*   - `utcParse(value, specifier)`
    - ❌
    - <p></p>
:::

## RegExp Functions
:::{list-table}
:header-rows: 1
:widths: 80 10 60

*   - Function
    - Status
    - Details
*   - `regexp(pattern[, flags])`
    - ❌
    - <p></p>
*   - `test(regexp[, string])`
    - ❌
    - <p></p>
:::


## Color Functions
:::{list-table}
:header-rows: 1
:widths: 60 10 60

*   - Function
    - Status
    - Details
*   - `rgb(r, g, b[, opacity])`
    - ❌
    - <p></p>
*   - `hsl(h, s, l[, opacity])`
    - ❌
    - <p></p>
*   - `lab(l, a, b[, opacity])`
    - ❌
    - <p></p>
*   - `hcl(h, c, l[, opacity])`
    - ❌
    - <p></p>
*   - `luminance(specifier)`
    - ❌
    - <p></p>
*   - `contrast(specifier1, specifier2)`
    - ❌
    - <p></p>
:::

## Event Functions
:::{list-table}
:header-rows: 1
:widths: 50 10 100

*   - Function
    - Status
    - Details
*   - `item()`
    - ❌
    - <p></p>
*   - `group([name])`
    - ❌
    - <p></p>
*   - `xy([item])`
    - ❌
    - <p></p>
*   - `x([item])`
    - ❌
    - <p></p>
*   - `y([item])`
    - ❌
    - <p></p>
*   - `pinchDistance(event)`
    - ❌
    - <p></p>
*   - `pinchAngle(event)`
    - ❌
    - <p></p>
*   - `inScope(item)`
    - ❌
    - <p></p>
:::

## Data Functions
:::{list-table}
:header-rows: 1
:widths: 60 10 60

*   - Function
    - Status
    - Details
*   - `data(name)`
    - ✅
    - <p></p>
*   - `indata(name, field, value)`
    - ❌
    - <p></p>
*   - `modify`
    - ✅
    - :::{dropdown} More Info
      This is a private function that Vega-Lite uses to implement
      selections.
      :::
:::

## Scale and Projection Functions
:::{list-table}
:header-rows: 1
:widths: 80 10 40

*   - Function
    - Status
    - Details
*   - `scale(name, value[, group])`
    - ❌
    - <p></p>
*   - `invert(name, value[, group])`
    - ❌
    - <p></p>
*   - `copy(name[, group])`
    - ❌
    - <p></p>
*   - `domain(name[, group])`
    - ❌
    - <p></p>
*   - `range(name[, group])`
    - ❌
    - <p></p>
*   - `bandwidth(name[, group])`
    - ❌
    - <p></p>
*   - `bandspace(count[, paddingInner, paddingOuter])`
    - ❌
    - <p></p>
*   - `gradient(scale, p0, p1[, count])`
    - ❌
    - <p></p>
*   - `panLinear(domain, delta)`
    - ❌
    - <p></p>
*   - `panLog(domain, delta)`
    - ❌
    - <p></p>
*   - `panPow(domain, delta, exponent)`
    - ❌
    - <p></p>
*   - `panSymlog(domain, delta, constant)`
    - ❌
    - <p></p>
*   - `zoomLinear(domain, anchor, scaleFactor)`
    - ❌
    - <p></p>
*   - `zoomLog(domain, anchor, scaleFactor)`
    - ❌
    - <p></p>
*   - `zoomPow(domain, anchor, scaleFactor, exponent)`
    - ❌
    - <p></p>
*   - `zoomSymlog(domain, anchor, scaleFactor, constant)`
    - ❌
    - <p></p>
:::

## Geographic Functions
:::{list-table}
:header-rows: 1
:widths: 60 10 40

*   - Function
    - Status
    - Details
*   - `geoArea(projection, feature[, group])`
    - ❌
    - <p></p>
*   - `geoBounds(projection, feature[, group])`
    - ❌
    - <p></p>
*   - `geoCentroid(projection, feature[, group])`
    - ❌
    - <p></p>
*   - `geoScale(projection[, group])`
    - ❌
    - <p></p>
:::

## Tree (Hierarchy) Functions
:::{list-table}
:header-rows: 1
:widths: 60 10 60

*   - Function
    - Status
    - Details
*   - `treePath(name, source, target)`
    - ❌
    - <p></p>
*   - `treeAncestors(name, node)`
    - ❌
    - <p></p>
:::

## Browser Functions
:::{list-table}
:header-rows: 1
:widths: 40 10 100

*   - Function
    - Status
    - Details
*   - `containerSize()`
    - ❌
    - <p></p>
*   - `screen()`
    - ❌
    - <p></p>
*   - `windowSize()`
    - ❌
    - <p></p>
:::

## Logging Functions
:::{list-table}
:header-rows: 1
:widths: 50 10 70

*   - Function
    - Status
    - Details
*   - `warn(value1[, value2, …])`
    - ❌
    - <p></p>
*   - `info(value1[, value2, …])`
    - ❌
    - <p></p>
*   - `debug(value1[, value2, …])`
    - ❌
    - <p></p>
:::

## Selection Functions
These are private functions that Vega-Lite uses to implement selections.

:::{list-table}
:header-rows: 1
:widths: 50 10 100

*   - Function
    - Status
    - Details
*   - `vlSelectionTest`
    - ✅
    - <p></p>
*   - `vlSelectionResolve`
    - ✅
    - <p></p>
:::
