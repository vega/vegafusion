// Copyright (c) Jon Mease
// Distributed under the terms of the Modified BSD License.

import {
  DOMWidgetModel,
  DOMWidgetView,
  ISerializers,
} from '@jupyter-widgets/base';

import { compile } from 'vega-lite'

// Not sure why imports need to work this way. When MsgReceiver is imported
// in the await import, it is not seen as a type
const { render_vegafusion } = await import("vegafusion-wasm")
import { MsgReceiver } from "vegafusion-wasm";

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
      vegalite_spec: null,
      vega_spec_full: null,
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
  vegafusion_handle: MsgReceiver;
  viewElement = document.createElement("div");

  render() {
    this.el.appendChild(this.viewElement);
    this.value_changed();
    this.model.on('change:vegalite_spec', this.value_changed, this);
    this.model.on("msg:custom", (ev: any, buffers: [DataView]) => {
      // console.log("js: receive");
      let bytes = new Uint8Array(buffers[0].buffer)
      // console.log(bytes);
      this.vegafusion_handle.receive(bytes)
    })
  }

  value_changed() {
    let vegalite_json = this.model.get('vegalite_spec');
    if (vegalite_json !== null) {
      let vega_spec = compile(JSON.parse(vegalite_json));
      let vega_spec_json = JSON.stringify(vega_spec.spec);
      this.model.set('vega_spec_full', vega_spec_json);
      this.touch();
      // console.log("js: value_changed");
      this.vegafusion_handle = render_vegafusion(this.viewElement, vega_spec_json, (request: ArrayBuffer) => {
        // console.log("js: request");
        this.send({type: "request"}, [request])
      });
    }
  }
}
