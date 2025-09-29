---
theme: dashboard
footer: ""
sidebar: false
head: |
  <link rel="stylesheet" href="./lib/tabs.css" />
  <link rel="stylesheet" href="./lib/spatial.css" />

---

<h1 style="max-width:100%">Index to Marine and Lacustrine Geological Samples</h1>

<details>
    <summary>About</summary>
<div class="grid grid-cols-2">
<div class="note">This site provides a read-only view of a snaptot of the 
<a href='https://www.ncei.noaa.gov/products/index-marine-lacustrine-samples'>Index to Marine and Lacustrine Geological Samples</a>
retrieved immediately prior to site deomissioning on 2025-05-05.</div>
<div class="warning">This is a development site and not the final location of this resource. Links to this site and its content are likely
to break as content evolves.</div>
</div>
</div>
</details>


```js
import {
    IMLGSData, 
    getIdFromURL, 
    jdToDate,
    intervalComment,
    debounce
} from "./lib/common.js";

// OpenLayers pieces
import * as ol from "ol";
import {defaults as defaultControls} from 'ol/control/defaults';
import CircleStyle from 'ol/style/Style';
import Fill from 'ol/style/Fill';
import FullScreen from 'ol/control/FullScreen';
import GeoJSON from 'ol/format/GeoJSON';
import ImageLayer from 'ol/layer/Image';
import ImageWMS from 'ol/source/ImageWMS';
import ImageArcGISRest from 'ol/source/ImageArcGISRest';
import OSM from 'ol/source/OSM';
import Select from 'ol/interaction/Select';
import StadiaMaps from 'ol/source/StadiaMaps';
import Stroke from 'ol/style/Stroke';
import Style from 'ol/style/Style';
import TileLayer from 'ol/layer/Tile';
import Vector from 'ol/source/Vector';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import WebGLVectorLayer from 'ol/layer/WebGLVector';
import WKB from 'ol/format/WKB';
import WMTSCapabilities from 'ol/format/WMTSCapabilities';
import WMTS, {optionsFromCapabilities} from 'ol/source/WMTS';
import WMTSTileGrid from 'ol/tilegrid/WMTS';
import XYZ from 'ol/source/XYZ';

import {transformExtent} from 'ol/proj';

const display_fields = [
    "imlgs",
    "sample",
    "igsn",
    "platform",
    "cruise.cruise as cruise",
    "device",
    "facility.facility_code as repository",
    "begin_jd",
    "water_depth",
];

const pq_source = "https://s3.beehivebeach.com/imlgs/imlgs_full_2.parquet";
//const pq_source = "https://zenodo.org/api/records/16389102/files/imlgs_full_2.parquet/content";
const imlgs_data = new IMLGSData(pq_source, "imlgs", display_fields);
await imlgs_data.initialize()
```

<div class="grid grid-cols-4">
<div class="card grid-colspan-1">

```js
let recordCount = Mutable(0);

function setRecordCount(v) {
    recordCount.value = v;
}

let visibleRecordCount = Mutable(0);

function setVisibleRecordCount(v) {
    visibleRecordCount.value = v;
}

const [_platform_input, platform_input] = await imlgs_data.newInputObserver(
    "platform", "Platform", "regexp_matches(platform,?,'i')"
);
const [_device_input, device_input] = await imlgs_data.newInputObserver(
    "device", "Device", "regexp_matches(device,?,'i')"
);
const [_repository_input, repository_input] = await imlgs_data.newInputObserver(
    "facility.facility_code", "Repository", "regexp_matches(facility.facility_code ,?,'i')"
);
const [_cruise_input, cruise_input] = await imlgs_data.newTextInputObserver(
    "cruise.cruise", "Cruise", "cruise.cruise=?"
);

display(_platform_input);
display(_device_input);
display(_repository_input);
display(_cruise_input);

const selectedRecordJson = Mutable("");

const setSelectedRecordJson =  (v) => {
    if (!v) {
        selectedRecordJson.value = "";
        return;
    }
    selectedRecordJson.value = v;
}

async function updateSelectedRecordJson(pid) {
    console.log(`Update record: ${pid}`);
    if (!pid) {
        setSelectedRecordJson("");
        return;
    }
    const query = `select * from ${imlgs_data.tbl} where imlgs=?`;
    const res = await imlgs_data.db.queryRow(query, [pid]);
    setSelectedRecordJson(JSON.stringify(res, null, 2));
}

const selectedRecordImlgs = Mutable("");

function updateSelectedRecordImlgs(pid) {
    console.log(`updateSelectedRecordImlgs: ${pid}`);
    selectedRecordImlgs.value = pid;
}

const currentTab = Mutable("map");

function updateCurrentTab(v) {
    currentTab.value = v;
}
```

```js
const ui_inputs = [];

async function getMatchingRecords(inputs) {
    const wc = imlgs_data.getWhereClause(inputs);
    return imlgs_data.getDisplayRecords(wc);
}

async function getMatchingCount(inputs) {
    const wc = imlgs_data.getWhereClause(inputs);
    return imlgs_data.count(wc);
}

ui_inputs.push(platform_input);
ui_inputs.push(device_input)
ui_inputs.push(repository_input);
ui_inputs.push(cruise_input);

//const inputs = [platform_input, device_input, repository_input]
let theRecords = getMatchingRecords(ui_inputs);
setRecordCount(getMatchingCount(ui_inputs));

```

Matching: ${recordCount}

Visible in map: ${visibleRecordCount}

<!--
<div id="recordViewContainer">
<p>Loading...</p>
</div>
-->

</div> <!--facets -->

<div class="grid-colspan-3 card">
    <div class="tabset">
        <!-- Tab 1 -->
        <input type="radio" name="tabset" id="tab1" aria-controls="mapview" checked onchange="onTabChange">
        <label for="tab1">Map</label>
        <!-- Tab 2 -->
        <input type="radio" name="tabset" id="tab2" aria-controls="tableview" onchange="onTabChange">
        <label for="tab2">Table</label>
<div class="tab-panels">
                    
<!-- The map container -->
<section id="olmap" class="map tab-panel">
<div id="infooverlay"></div>


```js
async function installMap(mapElement, infoElement) {
    // Setup the ESRI world ocean base layer
    const parser = new WMTSCapabilities();
    const response = await fetch("https://services.arcgisonline.com/arcgis/rest/services/Ocean/World_Ocean_Base/MapServer/WMTS/1.0.0/WMTSCapabilities.xml")
        .then(function (response) {
            return response.text();
        })
    const result = parser.read(response);
    const options = optionsFromCapabilities(result, {
        layer: 'Ocean_World_Ocean_Base',
        matrixSet: 'EPSG:3857',
        //matrixSet: 'EPSG:4326',
    });
    options.wrapX = true;
    const _target = document.getElementById(mapElement);
    _target.innerHTML = `<div id='${infoElement}'></div>`;
    const map = new ol.Map({
        //controls: defaultControls().extend([new FullScreen()]),
        layers: [
            new TileLayer({
                opacity: 1,
                source: new WMTS(options)
            }),
        ],
        target: _target,
        view: new ol.View({
            center: [0, 0],
            zoom: 2,
        }),
    });
    return map;
};

const map = await installMap('olmap', 'infooverlay');
```            

```js
const map_data_layers = {
    "samples": null
};
async function makeColors(db) {
    console.log("makeColors start")
    const results = await db.distinct("facility.facility_code");
    const data = [[173, 216, 230],
            [0, 191, 255],
            [30, 144, 255],
            [0,   0, 255],
            [0,   0, 139],
            [72,  61, 139],
            [123, 104, 238],
            [138,  43, 226],
            [128,   0, 128],
            [218, 112, 214],
            [255,   0, 255],
            [255,  20, 147],
            [176,  48,  96],
            [220,  20,  60],
            [240, 128, 128],
            [255,  69,   0],
            [255, 165,   0],
            [244, 164,  96],
            [240, 230, 140],
            [128, 128,   0],
            [139,  69,  19],
            [255, 255,   0],
            [154, 205,  50],
            [124, 252,   0],
            [144, 238, 144],
            [143, 188, 143],
            [34, 139,  34],
            [0, 255, 127],
            [0, 255, 255],
            [0, 139, 139],
            [128, 128, 128],
            [255, 255, 255]];
    const color_rules = ["case"];
    let last_c = 0;
    for (let row of results) {
        const c = data[last_c];
        last_c += 1;
        const _clr = `rgba(${c[0]},${c[1]},${c[2]},0.5)`;
        color_rules.push(["==",["get","repository"], row.d]);
        color_rules.push(_clr)
    }
    color_rules.push("red");
    const _style = {
            'circle-radius': 2,
            'circle-fill-color': color_rules,
            'circle-stroke-color': 'gray',
            'circle-stroke-width': 0.5
    }
    console.log("makeColors end")
    return _style;
}

const repositoryStyle = makeColors(imlgs_data);
```

```js
let selected = null;

async function loadParquetLayer(db, inputs) {
    const where_clause = db.getWhereClause(inputs, "geometry is not null");
    const data = await db.select(db.field_sets.spatial, where_clause, false);
    const format = new WKB();
    let i = 0;
    const psource = new VectorSource();
    for (const row of data) {
        const feature = format.readFeature(row.wkb, {
            dataProjection: 'EPSG:4326',
            featureProjection: 'EPSG:3857',
            //featureProjection: 'EPSG:4326',
        });
        feature.setId(row.imlgs);
        feature.set("repository", row.repository, true);
        feature.set("beginjd", row.begin_jd, true);
        psource.addFeature(feature);
        i += 1;
    }
    console.log(`rows: ${i}`);
    const player = new WebGLVectorLayer({
        source: psource,
        style: repositoryStyle
    });
    return player;
}

async function countSamplesInPolygon(db, inputs, wkt) {
    if (!db) {
        return 0;
    }
    const spatial_clause = `geometry IS NOT NULL AND st_within(geometry, st_geomFromText('${wkt}'))`;
    const where_clause = db.getWhereClause(inputs, spatial_clause);
    return await db.count(where_clause);
}


const where_clause = null;
//getRecordCount(imlgsdb, where_clause.clause, where_clause.params);
loadParquetLayer(imlgs_data, ui_inputs).then((pql) => {
    //global the_map_layer;
    if (map_data_layers.samples !== null) {
        console.log("remove layer")
        map.removeLayer(map_data_layers.samples);
        //map_data_layers.samples.dispose();
    }
    console.log("Add layer");
    map_data_layers.samples = pql;
    map.addLayer(map_data_layers.samples);
});

function selectStyle(feature) {
  const color = feature.get('COLOR') || '#eeeeee';
  selected.getFill().setColor(color);
  return selected;
}

const info = document.getElementById('infooverlay');
let currentFeature;
let res = "";
const displayFeatureInfo = function(pixel, target) {
  const feature = target.closest('.ol-control')
    ? undefined
    : map.forEachFeatureAtPixel(pixel, function (feature) {
        return feature;
      });
  if (feature) {
    info.style.left = pixel[0] + 'px';
    info.style.top = pixel[1] + 'px';
    if (feature !== currentFeature) {
      info.style.visibility = 'visible';
      res = feature.getId();
      info.innerText = `${feature.get('repository')} : ${res}`;
    }
  } else {
    info.style.visibility = 'hidden';
    res = "";
  }
  currentFeature = feature;
  return res;
}

map.on('pointermove', function (evt) {
  if (evt.dragging) {
    info.style.visibility = 'hidden';
    currentFeature = undefined;
    return;
  }
  displayFeatureInfo(evt.pixel, evt.originalEvent.target);
});

map.on('click', async function (evt) {
  const imlgsid = displayFeatureInfo(evt.pixel, evt.originalEvent.target);
  //setSelectedFeature("", "");
  //const res = await getRecord(imlgsdb, imlgsid);
  console.log(`Select ${ imlgsid }`);
  //debugger;
  //setSelectedFeature(imlgsid);
  if (currentTab == "map") {
      updateSelectedRecordImlgs(imlgsid);
  }
});

map.getTargetElement().addEventListener('pointerleave', function () {
  currentFeature = undefined;
  info.style.visibility = 'hidden';
});

//map.getView().calculateExtent(map.getSize());

map.on('moveend', async function(evt) {

    const fp = (v) => {
        return v.toFixed(3);
    }

    const extent = map.getView().calculateExtent(map.getSize());
    const ge = transformExtent(extent, 'EPSG:3857', 'EPSG:4326');
    const wkt = `POLYGON((${fp(ge[0])} ${fp(ge[1])}, ${fp(ge[2])} ${fp(ge[1])}, ${fp(ge[2])} ${fp(ge[3])}, ${fp(ge[0])} ${fp(ge[3])}, ${fp(ge[0])} ${fp(ge[1])}))`;
    console.log(wkt);
    const n = await countSamplesInPolygon(imlgs_data, ui_inputs, wkt);
    setVisibleRecordCount(n);
})

function zoomToPid(pid) {
    const src = map_data_layers.samples.getSource().getFeatures();
    for (const feature of src) {
        if (feature.id_ === pid) {
            console.log(feature);
            const view = map.getView();
            const geo = feature.getGeometry();
            view.fit(geo, {maxZoom: 6});
            return;
        }
    }
}

```


</section> <!-- Map view section -->

<!-- The table view container -->
<section id="tableview" class="tab-panel" style="min-height: 400px; ">

```js
function sparkbar(max) {
  return (x) => htl.html`<div style="
    background: var(--theme-foreground-faintest);
    color: var(--theme-foreground);
    font: 10px/1.6 var(--sans-serif);
    width: ${100 * x / max}%;
    float: right;
    padding-right: 3px;
    box-sizing: border-box;
    overflow: visible;
    display: flex;
    justify-content: end;">${x.toLocaleString("en-US")}`
}

function dateSparkbar(min, max) {
  return (x) => htl.html`<div style="
    background: Azure;
    color: black;
    font: 10px/1.6 var(--sans-serif);
    width: ${100 * (x-min) / (max-min)}%;
    float: right;
    padding-right: 3px;
    box-sizing: border-box;
    overflow: visible;
    display: flex;
    justify-content: end;">${jdToDate(x)}`
}

function findRow(pid) {
    // Expensive- iterates all rows to find the imlgs value.
    // but surprisingly fast.
    let i = 0;
    for (const r of theRecords) {
        if (r.imlgs === pid) {
            return i;
        }
        i += 1;
    }
}

function doRowClicked(e) {
    const tr = e.target.closest('tr');
    tr.querySelector('input').click();
}

function doCruiseClicked(e) {
    const v = e.target.innerText;
    _cruise_input.value = v;
    const event = new Event("input");
    _cruise_input.dispatchEvent(event);
}

const dataTable = Inputs.table(theRecords, {
    multiple: false,
    required: false,
    select: true,
    rows: 16,
    format: {
        imlgs: (v) => {return html`<span onclick=${doRowClicked}>${v}</span>`},
        sample: (v) => {return html`<span onclick=${doRowClicked}>${v}</span>`},
        igsn: (v) => {
            if (v) {
                return html`<a target="_blank" href="https://igsn.rslv.xyz/igsn:${v}">igsn:${v}</a>`
            }
            return v;
        },
        cruise: (v) => {return html`<span onclick=${doCruiseClicked}>${v}</span>`},
        begin_jd: (x) => jdToDate(x),
        water_depth: sparkbar(10415),
    },
    header: {
        "imlgs": "IMLGS ID",
        "sample": "Sample",
        "igsn": "IGSN",
        "platform": "Platform",
        "cruise": "Cruise",
        "device": "Device",
        "repository": "Repository",
        "begin_jd": "Date",
        "water_depth": "Water Depth",
    }
});
```

```js
const tableview = view(dataTable);
try {
    dataTable.querySelector('input[value="0"]').click();
} catch {

}


```

</section>
        </div>
    </div>
</div>

</div>



```js
function mapTabSelected() {
    // Called when the map tab is made active
    console.log("Map Selected");
    //updateCurrentTab("map");
    /*
    console.log(selectedRecordImlgs);
    if (!selectedRecordImlgs) {
        return;
    }
    zoomToPid(selectedRecordImlgs);
    */
}

function tableTabSelected() {
    // called when the tabe tab is made active
    console.log("Table Selected");
    //updateCurrentTab("table");
    /*
    if (!selectedRecordImlgs) {
        return;
    }
    if (tableview) {
        if (tableview.imlgs !== selectedRecordImlgs) {
            console.log(`Update table selecteion to ${selectedRecordImlgs}`);
            const rowindex = findRow(selectedRecordImlgs);
            console.log(`seek to row ${rowindex}`);
            //dataTable.select(rowindex);
            // sigh...
        }
    }
    */
}

async function loadSampleRecord(pid) {
    if (!pid) {
        return html`<p>Waiting for record selection...</p>`;
    }
    return imlgs_data.getRecordHtml(pid);
}

async function loadSampleRecordTV(tv, pid) {
    if (window.visibleTab == "map") {
        if (pid) {
            return imlgs_data.getRecordHtml(pid);
        } else {
            return html`<p>Waiting for record selection...</p>`;            
        }
    } else {
        if (tv) {
            return imlgs_data.getRecordHtml(tv.imlgs);
        } else {
            return html`<p>Waiting for record selection...</p>`;            
        }
    }
}
```

```js

/*if (tableview) {
    const tele = document.getElementById("tableview");
    if (tele.checkVisibility()){
        updateSelectedRecordImlgs(tableview.imlgs);
    }
}*/


/*async function showSelectedRecord(pid) {
    const target = document.getElementById("recordViewContainer");
    if (pid) {
        const content = await loadSampleRecord(pid);
        //debugger;
        target.innerHTML = content.innerHTML;
    } else {
        target.innerHTML = "<p>No record selected...</p>";
    }
}*/

//view(await loadSampleRecord(selectedRecordImlgs));
view(await loadSampleRecordTV(tableview, selectedRecordImlgs));
//showSelectedRecord(selectedRecordImlgs);
```




```js
window.visibleTab = "map";
window.selectedPID = null;

function onTabChange(e) {
    switch (e.target.id) {
        case "tab1":
            window.visibleTab = "map";
            //mapTabSelected();
            break;
        case "tab2":
            window.visibleTab = "table";
            //tableTabSelected();
            break;
    }
}
window.onTabChange = onTabChange;

function setupTabChangeEvents() {
    const radioElements = document.querySelectorAll("input[name='tabset']");
    for (const e of radioElements) {
        e.addEventListener("change", onTabChange);
    }
}

setupTabChangeEvents();
```

