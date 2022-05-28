/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */

// -----------------------------------------------------------
// Dropdown menu implementation is based heavily on vega-embed
// (https://github.com/vega/vega-embed) which is released
// under the BSD-3-Clause License: https://github.com/vega/vega-embed/blob/next/LICENSE

import {DOMWidgetModel, DOMWidgetView, ISerializers,} from '@jupyter-widgets/base';
import {MODULE_NAME, MODULE_VERSION} from './version';


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
      verbose: null,
      debounce_wait: 30,
      debounce_max_wait: 60,
      download_source_link: null,
      _request_msg: null,
      _response_msg: null,
    };
  }

  static serializers: ISerializers = {
    ...DOMWidgetModel.serializers,
    // Add any extra serializers here
    _request_msg: {
      serialize: (value: any): DataView | null => {
        if (value.buffer) {
          return new DataView(value.buffer.slice(0));
        } else {
          return null;
        }
      },
    },
  };

  static model_name = 'VegaFusionModel';
  static model_module = MODULE_NAME;
  static model_module_version = MODULE_VERSION;
  static view_name = 'VegaFusionView'; // Set to null if no view
  static view_module = MODULE_NAME; // Set to null if no view
  static view_module_version = MODULE_VERSION;
}

export class VegaFusionView extends DOMWidgetView {
  vegafusion_handle: import("vegafusion-embed").MsgReceiver;
  embedVegaFusion: typeof import("vegafusion-embed").embedVegaFusion;
  vegalite_compile: typeof import("vega-lite").compile;

  async render() {
    const { embedVegaFusion } = await import("vegafusion-embed");
    this.embedVegaFusion = embedVegaFusion;

    const { compile } = await import("vega-lite");
    this.vegalite_compile = compile;

    this.value_changed();
    this.model.on('change:spec', this.value_changed, this);
    this.model.on('change:verbose', this.value_changed, this);
    this.model.on('change:debounce_wait', this.value_changed, this);
    this.model.on('change:debounce_max_wait', this.value_changed, this);
    this.model.on('change:download_source_link', this.value_changed, this);
    this.model.on('change:_response_msg', () => {
      const msgBytes: DataView = this.model.get("_response_msg");
      if (msgBytes !== null) {
        if (this.model.get("verbose")) {
          console.log("VegaFusion(js): Received response");
          console.log(msgBytes.buffer);
        }
        const bytes = new Uint8Array(msgBytes.buffer);
        this.vegafusion_handle.receive(bytes);
      }
    });
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
        vega_spec_json = JSON.stringify(vega_spec.spec, null, 2);
      }

      let config = {
        verbose: this.model.get("verbose") || false,
        debounce_wait: this.model.get("debounce_wait") || 30,
        debounce_max_wait: this.model.get("debounce_max_wait"),
        download_source_link: this.model.get('download_source_link')
      };

      // this.vegafusion_handle = this.embedVegaFusion(
      this.vegafusion_handle = this.embedVegaFusion(
          this.el,
          vega_spec_json,
          (request: Uint8Array) => {
            if (this.model.get("verbose")) {
              console.log("VegaFusion(js): Send request");
            }

            this.model.set("_request_msg", new DataView(request.buffer));
            this.touch();
            this.model.set("_request_msg", {});
          },
          config
      );

      // Update vega spec properties
      this.model.set('full_vega_spec', vega_spec_json);
      this.model.set('client_vega_spec', this.vegafusion_handle.client_spec_json());
      this.model.set('server_vega_spec', this.vegafusion_handle.server_spec_json());
      this.model.set('comm_plan', this.vegafusion_handle.comm_plan_json());

      this.touch();
    }
  }
}
