import vegaEmbed from "https://esm.sh/vega-embed@6?deps=vega@5&deps=vega-lite@5.19.0";
import lodashDebounce from "https://esm.sh/lodash-es@4.17.21/debounce";

// Note: For offline support, the import lines above are removed and the remaining script
// is bundled using vl-convert's javascript_bundle function. See the documentation of
// the javascript_bundle function for details on the available imports and their names.
// If an additional import is required in the future, it will need to be added to vl-convert
// in order to preserve offline support.
async function render({ model, el }) {
    let finalize;

    function showError(error){
        el.innerHTML = (
            '<div style="color:red;">'
            + '<p>JavaScript Error: ' + error.message + '</p>'
            + "<p>This usually means there's a typo in your chart specification. "
            + "See the javascript console for the full traceback.</p>"
            + '</div>'
        );
    }

    const reembed = async () => {
        if (finalize != null) {
          finalize();
        }

        model.set("local_tz", Intl.DateTimeFormat().resolvedOptions().timeZone);

        let spec = structuredClone(model.get("transformed_spec"));
        if (spec == null) {
            // Remove any existing chart and return
            while (el.firstChild) {
                el.removeChild(el.lastChild);
            }
            model.save_changes();
            return;
        }
        let embedOptions = structuredClone(model.get("embed_options")) ?? undefined;

        let api;
        try {
            api = await vegaEmbed(el, spec, embedOptions);
        } catch (error) {
            showError(error)
            return;
        }

        finalize = api.finalize;

        // Debounce config
        const wait = model.get("debounce_wait") ?? 10;
        const debounceOpts = {leading: false, trailing: true};
        if (model.get("max_wait") ?? true) {
            debounceOpts["maxWait"] = wait;
        }
        model.save_changes();

        // Add signal/data listeners
        for (const watch of model.get("_js_watch_plan") ?? []) {
            if (watch.namespace === "data") {
                const dataHandler = (_, value) => {
                    const updates = [{
                        namespace: "data",
                        name: watch.name,
                        scope: watch.scope,
                        value: cleanJson(value)
                    }]
                    model.send({type: "update_state", updates: updates});
                };
                addDataListener(api.view, watch.name, watch.scope, lodashDebounce(dataHandler, wait, debounceOpts))

            } else if (watch.namespace === "signal") {
                const signalHandler = (_, value) => {
                    const updates = [{
                        namespace: "signal",
                        name: watch.name,
                        scope: watch.scope,
                        value: cleanJson(value)
                    }]
                    model.send({type: "update_state", updates: updates});
                };

                addSignalListener(api.view, watch.name, watch.scope, lodashDebounce(signalHandler, wait, debounceOpts))
            }
        }

        // Add signal/data updaters as messages
        model.on("msg:custom", msg => {
            if (msg.type === "update_view") {
                const updates = msg.updates;
                for (const update of updates) {
                    if (update.namespace === "signal") {
                        setSignalValue(api.view, update.name, update.scope, update.value);
                    } else if (update.namespace === "data") {
                        setDataValue(api.view, update.name, update.scope, update.value);
                    }
                }
            }
        });
    }

    model.on('change:transformed_spec', reembed);
    model.on('change:embed_options', reembed);
    model.on('change:debounce_wait', reembed);
    model.on('change:max_wait', reembed);
    await reembed();
}

function cleanJson(data) {
    return JSON.parse(JSON.stringify(data))
}

function getNestedRuntime(view, scope) {
    var runtime = view._runtime;
    for (const index of scope) {
        runtime = runtime.subcontext[index];
    }
    return runtime
}

function lookupSignalOp(view, name, scope) {
    let parent_runtime = getNestedRuntime(view, scope);
    return parent_runtime.signals[name] ?? null;
}

function dataRef(view, name, scope) {
    let parent_runtime = getNestedRuntime(view, scope);
    return parent_runtime.data[name];
}

export function setSignalValue(view, name, scope, value) {
    let signal_op = lookupSignalOp(view, name, scope);
    view.update(signal_op, value);
}

export function setDataValue(view, name, scope, value) {
    let dataset = dataRef(view, name, scope);
    let changeset = view.changeset().remove(() => true).insert(value)
    dataset.modified = true;
    view.pulse(dataset.input, changeset);
}

export function addSignalListener(view, name, scope, handler) {
    let signal_op = lookupSignalOp(view, name, scope);
    return addOperatorListener(
        view,
        name,
        signal_op,
        handler,
    );
}

export function addDataListener(view, name, scope, handler) {
    let dataset = dataRef(view, name, scope).values;
    return addOperatorListener(
        view,
        name,
        dataset,
        handler,
    );
}

// Private helpers from Vega for dealing with nested signals/data
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

export default { render }
