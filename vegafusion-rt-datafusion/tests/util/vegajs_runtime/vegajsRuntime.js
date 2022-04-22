/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
let vega = require('vega');
let {truthy} = require('vega-util');
const _ = require("lodash");
let { optimize } = require('svgo');
let fs = require('fs');

// Install custom JSON serializers
Object.defineProperty(Date.prototype, "toJSON", {value: function() {return "__$datetime:" + this.getTime()}})

function parseExpression(expr_str) {
    let expr = vega.parseExpression(expr_str);
    return JSON.stringify(expr)
}

function lookupSignalOp(view, name, scope) {
    // name is an array that may have leading integer group indices
    var parent_runtime = view._runtime;
    for (const index of scope) {
        if (!parent_runtime.subcontext) {
            throw `Missing subcontext for ${name} with scope ${scope}`
        }
        parent_runtime = parent_runtime.subcontext[index];
    }
    return parent_runtime.signals[name];
}

function lookupDataOp(view, name, scope) {
    // name is an array that may have leading integer group indices
    var parent_runtime = view._runtime;
    for (const index of scope) {
        if (!parent_runtime.subcontext) {
            throw `Missing subcontext for ${name} with scope ${scope}`
        }

        parent_runtime = parent_runtime.subcontext[index];
    }

    return parent_runtime.data[name];
}

function getWatchValues(view, watches) {
    var watch_values = [];
    for (const watch of watches) {
        let {namespace, name, scope} = watch;
        if (namespace === "signal") {
            let signalValue = lookupSignalOp(view, name, scope).value;
            watch_values.push({watch: {namespace, name, scope}, value: _.clone(signalValue)});
        } else if (namespace === "data") {
            let dataOp = lookupDataOp(view, name, scope);
            watch_values.push({watch: {namespace, name, scope}, value: _.clone(dataOp.values.value)});
        } else {
            throw `Invalid watch namespace: ${namespace}`
        }
    }
    return watch_values
}


async function evalSpec(spec, watches) {
    var view = new vega.View(vega.parse(spec), {renderer: 'none'});
    await view.runAsync();
    return JSON.stringify(getWatchValues(view, watches));
}


async function viewToSvgJson(view) {
    // generate a static SVG image
    let svg = await view.toSVG();
    let svg_opt = optimize(svg, {
        js2svg: {
            indent: 2,
            pretty: true
        }}).data;
    return {svg: svg_opt}
}


async function viewToPngJson(view) {
    // generate a static SVG image
    let png = await view.toImageURL('png');
    // Remove leading data uri
    return {png: png.slice('data:image/png;base64,'.length, png.length)}
}


async function viewToImageJson(view, format) {
    if (format === "svg") {
        return await viewToSvgJson(view)
    } else {
        return await viewToPngJson(view)
    }
}

async function saveViewToImageJson(view, file, format) {
    let data = await viewToImageJson(view, format);
    fs.writeFileSync(file, JSON.stringify( data), (err) => {
        if (err) throw err;
    })
}

async function exportSingle(spec, file, format) {
    // create a new view instance for a given Vega JSON spec
    let view = new vega.View(vega.parse(spec), {renderer: 'none'});
    await saveViewToImageJson(view, file, format);
}


async function exportSequence(spec, file, format, init, updates, watches) {
    // create a new view instance for a given Vega JSON spec
    var view = new vega.View(vega.parse(spec), {renderer: 'none'});

    // Normalize watches
    watches = watches || [];

    // Apply initial updates
    // These updates must be applied before the first run command
    for (const update of init) {
        let {namespace, name, scope, value} = update;
        if (namespace === "signal") {
            let signalOp = lookupSignalOp(view, name, scope);
            view.update(signalOp, value);
        } else if (namespace === "data") {
            let dataset = lookupDataOp(view, name, scope);
            let changeset = view.changeset().remove(truthy).insert(value)
            dataset.modified = true;
            view.pulse(dataset.input, changeset);
        } else {
            throw `Invalid update namespace: ${namespace}`
        }
    }

    // For initial updates, run is not applied until after all init updates are applied
    await view.runAsync();

    // Collect initial watch values
    let result = [
        [await viewToImageJson(view, format), getWatchValues(view, watches)]
    ];

    // Apply iterative updates
    for (const i of _.range(0, updates.length)) {
        var update_element = updates[i];
        if (!_.isArray(update_element)) {
            update_element = [update_element];
        }

        for (const update of update_element) {
            let {namespace, name, scope, value} = update;
            if (namespace === "signal") {
                let signalOp = lookupSignalOp(view, name, scope);
                await view.update(signalOp, value).runAsync();
            } else if (namespace === "data") {
                let dataset = lookupDataOp(view, name, scope);
                let changeset = view.changeset().remove(truthy).insert(value)
                dataset.modified = true;
                await view.pulse(dataset.input, changeset).runAsync();
            } else {
                throw `Invalid update namespace: ${namespace}`
            }
        }

        result.push([await viewToImageJson(view, format), getWatchValues(view, watches)])
    }

    fs.writeFileSync(file, JSON.stringify(result), (err) => {
        if (err) throw err;
    })
}


module.exports = {
    parseExpression,
    evalSpec,
    exportSingle,
    exportSequence,
}
