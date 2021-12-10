// Copyright (c) Jon Mease
// Distributed under the terms of the Modified BSD License.

import {
  DOMWidgetModel,
  DOMWidgetView,
  ISerializers,
} from '@jupyter-widgets/base';

import { MODULE_NAME, MODULE_VERSION } from './version';

// Import the CSS
import '../css/widget.css';

export class VegaFusionModel extends DOMWidgetModel {
  defaults() {
    return {
      ...super.defaults(),
      _model_name: VegaFusionModel.model_name,
      _model_module: VegaFusionModel.model_module,
      _model_module_version: VegaFusionModel.model_module_version,
      _view_name: VegaFusionModel.view_name,
      _view_module: VegaFusionModel.view_module,
      _view_module_version: VegaFusionModel.view_module_version,
      spec: null,
      full_vega_spec: null,
      client_vega_spec: null,
      server_vega_spec: null,
      vegafusion_handle: null,
    };
  }

  static serializers: ISerializers = {
    ...DOMWidgetModel.serializers,
    // Add any extra serializers here
  };

  static model_name = 'VegaFusionModel';
  static model_module = MODULE_NAME;
  static model_module_version = MODULE_VERSION;
  static view_name = 'VegaFusionView'; // Set to null if no view
  static view_module = MODULE_NAME; // Set to null if no view
  static view_module_version = MODULE_VERSION;
}

export class VegaFusionView extends DOMWidgetView {
  vegafusion_handle: import("vegafusion-wasm").MsgReceiver;
  viewElement = document.createElement("div");
  render_vegafusion: typeof import("vegafusion-wasm").render_vegafusion;
  vegalite_compile: typeof import("vega-lite").compile;

  async render() {
    const { render_vegafusion } = await import("vegafusion-wasm");
    this.render_vegafusion = render_vegafusion;

    const { compile } = await import("vega-lite");
    this.vegalite_compile = compile;

    this.el.appendChild(this.viewElement);
    this.value_changed();
    this.model.on('change:spec', this.value_changed, this);
    this.model.on("msg:custom", (ev: any, buffers: [DataView]) => {
      // console.log("js: receive");
      let bytes = new Uint8Array(buffers[0].buffer)
      // console.log(bytes);
      this.vegafusion_handle.receive(bytes)
    })
  }

  value_changed() {
    let spec = this.model.get('spec');
    if (spec !== null) {
      let parsed = JSON.parse(spec);
      let vega_spec_json;
      if (parsed["$schema"].endsWith("schema/vega/v5.json")) {
        vega_spec_json = spec
      } else {
        // Assume we have a Vega-Lite spec, compile to vega
        let vega_spec = this.vegalite_compile(parsed);
        vega_spec_json = JSON.stringify(vega_spec.spec);
      }

      // console.log("js: value_changed");
      this.vegafusion_handle = this.render_vegafusion(this.viewElement, vega_spec_json, (request: ArrayBuffer) => {
        // console.log("js: request");
        this.send({type: "request"}, [request])
      });

      // Update vega spec properties
      this.model.set('full_vega_spec', vega_spec_json);
      this.model.set('client_vega_spec', this.vegafusion_handle.client_spec_json());
      this.model.set('server_vega_spec', this.vegafusion_handle.server_spec_json());
      this.model.set('comm_plan', this.vegafusion_handle.comm_plan_json());

      this.touch();
    }
  }
}
