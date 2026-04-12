/**
 * <mosaic-viz> — reusable web component for Mosaic/VGPlot visualizations.
 *
 * Attributes:
 *   viz        — JSON string defining the Mosaic dashboard spec.
 *                Use {{KEY}} placeholders; they are substituted from `properties`.
 *   query      — SQL string to execute before rendering (creates temp tables, etc.).
 *                Use {{KEY}} placeholders; they are substituted from `properties`.
 *   properties — JSON object of key→value pairs substituted into both `query` and `viz`.
 *
 * All <mosaic-viz> elements on a page share a single DuckDB coordinator (Mosaic's
 * global singleton). The first element to connect initialises it; subsequent elements
 * reuse the existing connection.
 *
 * Example usage:
 *
 *   <mosaic-viz
 *     properties='{"VIEW_NAME":"scores_2026","YEAR":"2026"}'
 *     query="CREATE OR REPLACE TEMP TABLE {{VIEW_NAME}} AS SELECT ..."
 *     viz='{"params":{"filter":{"select":"crossfilter"}},"vconcat":[{"plot":[{"mark":"rectY","data":{"from":"{{VIEW_NAME}}","filterBy":"$filter"},"x":{"bin":"score"},"y":{"count":null},"fill":"steelblue"}]}]}'
 *   ></mosaic-viz>
 */

import * as vg from "https://cdn.jsdelivr.net/npm/@uwdata/vgplot/+esm";
import { parseSpec, astToDOM } from "https://cdn.jsdelivr.net/npm/@uwdata/mosaic-spec/+esm";

function parseViz(str) {
  try {
    return JSON.parse(str);
  } catch (e) {
    throw new Error(`viz must be valid JSON: ${e.message}`);
  }
}

// Module-level promise so only the first component initialises the coordinator.
let _coordReady = null;

function getCoordinator() {
  if (!_coordReady) {
    _coordReady = (async () => {
      const coord = vg.coordinator();
      await coord.databaseConnector(vg.wasmConnector());
      await coord.exec(`INSTALL httpfs; LOAD httpfs;`);
      return coord;
    })();
  }
  return _coordReady;
}

// --------------------------------------------------------------------------

class MosaicViz extends HTMLElement {
  static get observedAttributes() {
    return ["viz", "query", "properties"];
  }

  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._connected = false;
    this._version = 0;
  }

  connectedCallback() {
    this._buildShell();
    this._connected = true;
    this._render();
  }

  disconnectedCallback() {
    this._connected = false;
  }

  attributeChangedCallback(_name, oldVal, newVal) {
    if (this._connected && oldVal !== newVal) {
      this._render();
    }
  }

  // ---------- private ----------

  _buildShell() {
    this.shadowRoot.innerHTML = `
      <style>
        :host { display: block; }
        #spinner { font-size: .85rem; color: #888; margin-bottom: .5rem; display: none; }
        #error {
          color: #c00; padding: .75rem; background: #fff0f0;
          border-radius: 6px; font-family: monospace; font-size: .85rem; display: none;
        }
        /* mosaic inputs: give the select/input breathing room from its label */
        #charts label { margin-right: .35rem; }
        #charts select, #charts input[type="text"], #charts input[type="number"] {
          margin-left: .35rem;
        }
      </style>
      <div id="spinner">⏳ Loading…</div>
      <div id="error"></div>
      <div id="charts"></div>
    `;
  }

  _getProps() {
    try {
      return JSON.parse(this.getAttribute("properties") || "{}");
    } catch {
      return {};
    }
  }

  _sub(template, props) {
    return template.replace(/\{\{(\w+)\}\}/g, (match, key) =>
      Object.prototype.hasOwnProperty.call(props, key) ? props[key] : match
    );
  }

  async _render() {
    const vizYaml  = (this.getAttribute("viz")   || "").trim();
    const sqlQuery = (this.getAttribute("query") || "").trim();
    if (!vizYaml) return;

    // Debounce: cancel any in-flight render
    const token = Symbol();
    this._renderToken = token;

    const spinner = this.shadowRoot.getElementById("spinner");
    const errorEl = this.shadowRoot.getElementById("error");
    const charts  = this.shadowRoot.getElementById("charts");

    spinner.style.display = "block";
    errorEl.style.display = "none";

    try {
      const coord = await getCoordinator();
      if (this._renderToken !== token) return; // superseded

      // Use a versioned table name on every render so mosaic never gets a
      // cache hit against a replaced temp table, and old clients become
      // harmlessly orphaned (they point to a table that no longer exists).
      this._version += 1;
      const props = this._getProps();
      const viewName = props.VIEW_NAME;
      const versionedProps = viewName
        ? { ...props, VIEW_NAME: `${viewName}_r${this._version}` }
        : props;

      if (sqlQuery) {
        await coord.exec(this._sub(sqlQuery, versionedProps));
        if (this._renderToken !== token) return;
      }

      const spec = parseViz(this._sub(vizYaml, versionedProps));
      const ast  = parseSpec(spec);
      const { element } = await astToDOM(ast);
      if (this._renderToken !== token) return;

      charts.innerHTML = "";
      charts.appendChild(element);
    } catch (err) {
      console.error("[mosaic-viz]", err);
      errorEl.textContent   = `Error: ${err.message ?? err}`;
      errorEl.style.display = "block";
      this.shadowRoot.getElementById("charts").innerHTML = "";
    } finally {
      if (this._renderToken === token) spinner.style.display = "none";
    }
  }
}

customElements.define("mosaic-viz", MosaicViz);
