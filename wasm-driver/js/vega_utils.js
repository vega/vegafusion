import { version } from "vega"
import { truthy } from "vega-util"

import _ from "lodash"

export function vega_version() {
    return version
}

function getNestedRuntime(view, scope) {
    // name is an array that may have leading integer group indices
    var runtime = view._runtime;
    for (const index of scope) {
        runtime = runtime.subcontext[index];
    }
    return runtime
}

function lookupSignalOp(view, name, scope) {
    // name is an array that may have leading integer group indices
    let parent_runtime = getNestedRuntime(view, scope);
    return parent_runtime.signals[name];
}

function dataref(view, name, scope) {
    // name is an array that may have leading integer group indices
    let parent_runtime = getNestedRuntime(view, scope);
    return parent_runtime.data[name];
}

export function getSignalValue(view, name, scope) {
    let signal_op = lookupSignalOp(view, name, scope);
    return _.cloneDeep(signal_op.value)
}

export function setSignalValue(view, name, scope, value) {
    let signal_op = lookupSignalOp(view, name, scope);
    view.update(signal_op, value);
}

export function getDataValue(view, name, scope) {
    let data_op = dataref(view, name, scope);
    return _.cloneDeep(data_op.values.value)
}

export function setDataValue(view, name, scope, value) {
    let dataset = dataref(view, name, scope);
    let changeset = view.changeset().remove(truthy).insert(value)
    dataset.modified = true;
    view.pulse(dataset.input, changeset);
}

export function addSignalListener(view, name, scope, handler) {
    let signal_op = lookupSignalOp(view, name, scope);
    return addOperatorListener(
        view,
        name,
        signal_op,
        _.debounce(handler, 20, {'maxWait': 50}),
    );
}

export function addDataListener(view, name, scope, handler) {
    let dataset = dataref(view, name, scope).values;
    return addOperatorListener(
        view,
        name,
        dataset,
        _.debounce(handler, 20, {'maxWait': 50}),
    );
}

// Private helpers from Vega
function findOperatorHandler(op, handler) {
    const h = (op._targets || [])
        .filter(op => op._update && op._update.handler === handler);
    return h.length ? h[0] : null;
}

function addOperatorListener(view, name, op, handler) {
    let h = findOperatorHandler(op, handler);
    if (!h) {
        h = trap(view, () => handler(name, op.value));
        h.handler = handler;
        view.on(op, null, h);
    }
    return view;
}

function trap(view, fn) {
    return !fn ? null : function() {
        try {
            fn.apply(this, arguments);
        } catch (error) {
            view.error(error);
        }
    };
}


