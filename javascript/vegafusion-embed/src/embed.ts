import {MODULE_NAME, MODULE_VERSION} from './version';
import {MsgReceiver} from 'vegafusion-wasm'
const { render_vegafusion } = await import("vegafusion-wasm");

import '../css/vegafusion-embed.css';
// @ts-ignore
import logo_svg from '../images/VegaFusionLogo-SmallGrey.svg';

const CHART_WRAPPER_CLASS = 'chart-wrapper';
let DOWNLOAD_FILE_NAME = "visualization";

const I18N = {
    CLICK_TO_VIEW_ACTIONS: 'Click to view actions',
    PNG_ACTION: 'Save as PNG',
    SVG_ACTION: 'Save as SVG',
};

export function foo() {
    console.log([MODULE_NAME, MODULE_VERSION]);
}

export interface EmbedConfig {
    verbose: boolean;
    debounce_wait: number;
    debounce_max_wait: number | undefined;
    download_source_link: string | undefined;
}

const defaultEmbedConfig: EmbedConfig = {
    verbose: false, debounce_wait: 30, debounce_max_wait: 60, download_source_link: undefined
}

export function embedVegaFusion(
    element: Element,
    spec_str: string,
    send_msg_fn: Function,
    config: EmbedConfig | undefined,
): MsgReceiver {
    // Clear existing children from element
    // Eventually we should detect when element is already setup and just make the necessary
    // changes
    while (element.firstChild) {
        element.removeChild(element.firstChild);
    }

    // Element that will be passed to render_vegafusion
    let chartElement = document.createElement("div");

    // Handle null config
    config = config || defaultEmbedConfig;

    // Render to chart element
    let receiver = render_vegafusion(
        chartElement, spec_str,
        config.verbose || defaultEmbedConfig.verbose,
        config.debounce_wait || defaultEmbedConfig.debounce_wait,
        config.debounce_max_wait || defaultEmbedConfig.debounce_max_wait,
        send_msg_fn
    );

    // Build container element that will hold the vegafusion chart
    let containerElement = document.createElement("div");
    containerElement.appendChild(chartElement)
    containerElement.classList.add(CHART_WRAPPER_CLASS);

    // Element that holds the dropdown menu
    let menuElement = document.createElement("div");
    menuElement.appendChild(buildMenu(receiver, undefined));

    // Add children to top-level element
    element.appendChild(containerElement);
    element.appendChild(menuElement);
    element.classList.add("vegafusion-embed");
    element.classList.add("has-actions");

    return receiver
}

function buildMenu(receiver: MsgReceiver, download_source_link: string | undefined): Element {
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
        exportLink.download = `${DOWNLOAD_FILE_NAME}.${ext}`;

        // Disable browser tooltip
        exportLink.title = '';

        // add link on mousedown so that it's correct when the click happens
        exportLink.addEventListener('mousedown', async function (this, e) {
            e.preventDefault();
            if (receiver) {
                this.href = await receiver.to_image_url(ext, scale_factor);
            }
        });
        ctrl.append(exportLink);
    }

    // Add hr
    ctrl.append(document.createElement("hr"));

    // Add About
    const aboutLink = document.createElement('a');
    const about_href = 'https://vegafusion.io/';
    aboutLink.text = "About VegaFusion";
    aboutLink.href = about_href;
    aboutLink.target = '_blank';
    aboutLink.title = about_href;
    ctrl.append(aboutLink);

    return details;
}

