/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
import { version } from "vega"
import { truthy } from "vega-util"
import {Handler } from 'vega-tooltip';
import * as grpcWeb from 'grpc-web';

import _ from "lodash"

export function vega_version() {
    return version
}

export function localTimezone() {
    return Intl.DateTimeFormat().resolvedOptions().timeZone
}

// JSON Serialize Dates to milliseconds
Object.defineProperty(Date.prototype, "toJSON", {value: function() {return this.getTime()}})

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

export function addSignalListener(view, name, scope, handler, wait, maxWait) {
    let signal_op = lookupSignalOp(view, name, scope);
    let options = {};
    if (maxWait) {
        options["maxWait"] = maxWait;
    }

    return addOperatorListener(
        view,
        name,
        signal_op,
        _.debounce(handler, wait, options),
    );
}

export function addDataListener(view, name, scope, handler, wait, maxWait) {
    let dataset = dataref(view, name, scope).values;
    let options = {};
    if (maxWait) {
        options["maxWait"] = maxWait;
    }
    return addOperatorListener(
        view,
        name,
        dataset,
        _.debounce(handler, wait, options),
    );
}

export function setupTooltip(view) {
    let tooltip_opts = {};
    let handler = new Handler(tooltip_opts).call;
    view.tooltip(handler)
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

// Other utility functions
export function make_grpc_send_message_fn(client, hostname) {
    let send_message_grpc = (send_msg_bytes, receiver) => {
        let grpc_route = '/services.VegaFusionRuntime/TaskGraphQuery'

        // Make custom MethodDescriptor that does not perform serialization
        const methodDescriptor = new grpcWeb.MethodDescriptor(
            grpc_route,
            grpcWeb.MethodType.UNARY,
            Uint8Array,
            Uint8Array,
            (v) => v,
            (v) => v,
        );

        let promise = client.unaryCall(
            hostname + grpc_route,
            send_msg_bytes,
            {},
            methodDescriptor,
        );
        promise.then((response) => {
            receiver.receive(response)
        })
    }
    return send_message_grpc
}
