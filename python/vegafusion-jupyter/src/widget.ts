// Copyright (c) Jon Mease
// Distributed under the terms of the Modified BSD License.

import {DOMWidgetModel, DOMWidgetView, ISerializers,} from '@jupyter-widgets/base';

import {MODULE_NAME, MODULE_VERSION} from './version';

// Import the CSS
import '../css/widget.css';
import '../css/vegafusion-embed.css';
// @ts-ignore
import logo_svg from '../images/VegaFusionLogo-SmallGrey.svg';

const I18N = {
  CLICK_TO_VIEW_ACTIONS: 'Click to view actions',
  COMPILED_ACTION: 'View Compiled Vega',
  EDITOR_ACTION: 'Open in Vega Editor',
  PNG_ACTION: 'Save as PNG',
  SOURCE_ACTION: 'View Source',
  SVG_ACTION: 'Save as SVG',
};

// const SVG_CIRCLES = `
// <svg viewBox="0 0 16 16" fill="currentColor" stroke="none" stroke-width="1" stroke-linecap="round" stroke-linejoin="round">
//   <circle r="2" cy="8" cx="2"></circle>
//   <circle r="2" cy="8" cx="8"></circle>
//   <circle r="2" cy="8" cx="14"></circle>
// </svg>`;



const CHART_WRAPPER_CLASS = 'chart-wrapper';

let downloadFileName = "visualization";

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
  containerElement = document.createElement("div");
  render_vegafusion: typeof import("vegafusion-wasm").render_vegafusion;
  vegalite_compile: typeof import("vega-lite").compile;

  generate_menu() {
    const details = document.createElement('details');
    details.title = I18N.CLICK_TO_VIEW_ACTIONS;

    const summary = document.createElement('summary');
    summary.innerHTML = logo_svg;

    details.append(summary);

    let documentClickHandler = (ev: MouseEvent) => {
      if (!details.contains(ev.target as any)) {
        details.removeAttribute('open');
      }
    };
    document.addEventListener('click', documentClickHandler);

    // popup
    const ctrl = document.createElement('div');
    details.append(ctrl);
    ctrl.classList.add('vegafusion-actions');

    // image export
    for (const ext of ['svg', 'png'] as const) {
      let scale_factor = 1.0;

        const i18nExportAction = (I18N as {[key: string]: string})[`${ext.toUpperCase()}_ACTION`];
        const exportLink = document.createElement('a');

        exportLink.text = i18nExportAction;
        exportLink.href = '#';
        exportLink.target = '_blank';
        exportLink.download = `${downloadFileName}.${ext}`;

        // Disable browser tooltip
        exportLink.title = '';

        // add link on mousedown so that it's correct when the click happens
        let that = this;
        exportLink.addEventListener('mousedown', async function (this, e) {
          e.preventDefault();
          if (that.vegafusion_handle) {
            this.href = await that.vegafusion_handle.to_image_url(ext, scale_factor);
          }
        });
        ctrl.append(exportLink);
    }

    // Add hr
    ctrl.append(document.createElement("hr"));

    // Add About
    const aboutLink = document.createElement('a');
    aboutLink.text = "About VegaFusion";
    aboutLink.href = '';
    aboutLink.target = '_blank';
    aboutLink.title = '';
    ctrl.append(aboutLink);

    // Add License
    const licenseLink = document.createElement('a');
    licenseLink.text = "AGPL License";
    licenseLink.href = 'https://www.gnu.org/licenses/agpl-3.0.en.html';
    licenseLink.target = '_blank';
    licenseLink.title = '';
    ctrl.append(licenseLink);

    return details
  }

  async render() {
    const { render_vegafusion } = await import("vegafusion-wasm");
    this.render_vegafusion = render_vegafusion;

    const { compile } = await import("vega-lite");
    this.vegalite_compile = compile;

    let menu = this.generate_menu();
    this.containerElement.appendChild(this.viewElement);
    this.containerElement.classList.add(CHART_WRAPPER_CLASS);

    this.el.appendChild(this.containerElement);
    this.el.appendChild(menu);
    this.el.classList.add("vegafusion-embed");
    this.el.classList.add("has-actions");

    this.value_changed();
    this.model.on('change:spec', this.value_changed, this);
    this.model.on('change:verbose', this.value_changed, this);
    this.model.on('change:debounce_wait', this.value_changed, this);
    this.model.on('change:debounce_max_wait', this.value_changed, this);

    this.model.on("msg:custom", (ev: any, buffers: [DataView]) => {
      if (this.model.get("verbose")) {
        console.log("VegaFusion(js): Received response");
      }

      let bytes = new Uint8Array(buffers[0].buffer)
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

      this.vegafusion_handle = this.render_vegafusion(
          this.viewElement,
          vega_spec_json,
          this.model.get("verbose") || false,
          this.model.get("debounce_wait") || 30,
          this.model.get("debounce_max_wait"),
          (request: ArrayBuffer) => {
            if (this.model.get("verbose")) {
              console.log("VegaFusion(js): Send request");
            }

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
