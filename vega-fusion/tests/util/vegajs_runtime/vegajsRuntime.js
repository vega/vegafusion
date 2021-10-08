let vega = require('vega');
const _ = require("lodash");

// Install custom JSON serializers
Object.defineProperty(Date.prototype, "toJSON", {value: function() {return "__$datetime:" + this.getTime()}})

function parseExpression(expr_str) {
    let expr = vega.parseExpression(expr_str);
    return JSON.stringify(expr)
}

function lookupSignalOp(view, name, path) {
    // name is an array that may have leading integer group indices
    var parent_runtime = view._runtime;
    for (const index of path) {
        parent_runtime = parent_runtime.subcontext[index];
    }
    return parent_runtime.signals[name];
}

function lookupDataOp(view, name, path) {
    // name is an array that may have leading integer group indices
    var parent_runtime = view._runtime;
    for (const index of path) {
        parent_runtime = parent_runtime.subcontext[index];
    }

    return parent_runtime.data[name];
}

function get_watch_values(view, watches) {
    var watch_values = [];
    for (const watch of watches) {
        let {namespace, name, path} = watch;
        if (namespace === "signal") {
            let signalValue = lookupSignalOp(view, name, path).value;
            watch_values.push({watch: {namespace, name, path}, value: _.clone(signalValue)});
        } else if (namespace === "data") {
            let dataOp = lookupDataOp(view, name, path);
            watch_values.push({watch: {namespace, name, path}, value: _.clone(dataOp.values.value)});
        } else {
            throw `Invalid watch namespace: ${namespace}`
        }
    }
    return watch_values
}


async function evalSpec(spec, watches) {
    var view = new vega.View(vega.parse(spec), {renderer: 'none'});
    await view.runAsync();
    return JSON.stringify(get_watch_values(view, watches));
}

module.exports = {
    parseExpression,
    evalSpec,
}
