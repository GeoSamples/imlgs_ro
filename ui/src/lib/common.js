/**
 * Common JS used by different views
 */
import {DuckDBClient} from "npm:@observablehq/duckdb";
import * as Inputs from "npm:@observablehq/inputs";
import {Generators} from "observablehq:stdlib";
import {html} from "npm:htl";

const DEFAULT_DISPLAY_FIELDS = [
    "imlgs",
    "platform",
    "device",
    "facility.facility_code as repository"
];

const DEFAULT_RECORD_FIELDS = [
    "imlgs",
    "igsn",
    "platform",
    "cruise",
    "sample",
    "device",
    "water_depth",
    "facility",
    "ship_code",
    "links",
    "intervals",
    "storage_meth",
    "cored_length",
    "cored_diam",
    "pi",
    "igsn",
    "province",
    "lake",
    "leg",
    "sample_comments",
    "ST_AsText(geometry) as geometry",
    "begin_jd"
];

const DEFAULT_SPATIAL_FIELDS = [
    "imlgs", 
    "ST_AsWKB(geometry) as wkb", 
    "facility.facility_code as repository", 
    "platform", 
    "begin_jd"
];

export const debounce = (callback, wait) => {
  let timeoutId = null;
  return (...args) => {
    window.clearTimeout(timeoutId);
    timeoutId = window.setTimeout(() => {
      callback(...args);
    }, wait);
  };
}


export const ui_inputs = [];

/**
 * Retrieve the fragment portion of the current URL.
 */
export function getIdFromURL() {
    const hash = window.location.hash;
    if (hash.length > 0) {
        return hash.slice(1);
    }
    return null;
}

/**
 * Holds an SQL WHERE clause and associated parameters.
 */
export class WhereClause {
    constructor(clause, params) {
        this.clause = clause;
        this.params = params;
    }
}

const NULL_WHERE_CLAUSE = new WhereClause("", []);

export async function addJSONLD(pid, jld_data) {
    const soeles = document.querySelectorAll("script[type='application/ld+json']");
    let soele = null;
    let create_new = true;
    if (soeles.length > 0) {
        soele = soeles[0];
        soele.setAttribute("id", pid);
        create_new = false;
    } else {
        soele = document.createElement("script");
        soele.type = "application/ld+json";
        soele.setAttribute("id", pid);
    }
    soele.text = JSON.stringify(jld_data, null, 2);
    //console.log(soele.text);
    if (create_new) {
        document.querySelector("head").appendChild(soele);
    }
}


/**
 * Adds an observer to an input and yield a custom value, in this 
 * case, the input value and a template string associated with the input.
 * 
 * This is used to generate an SQL where clause for the input value
 * @param {*} inputer 
 * @param {*} template 
 * @returns 
 */
export function newInputObserver(inputer, template) {
    const res = Generators.observe((notify) => {
        const inputted = () => {
            notify({"v":inputer.value, "c": template});
        };
        inputted();
        inputer.addEventListener("input", inputted);
        return () => inputer.removeEventListener("input", inputted);
    });
    ui_inputs.push(res);
    return res;
}


export function newTextInputObserver(inputer, template) {
    const res = Generators.observe((notify) => {
        const inputted = () => {
            notify({"v":inputer.value, "c": template});
        };
        inputted();
        inputer.addEventListener("input", inputted);
        return () => inputer.removeEventListener("input", inputted);
    });
    ui_inputs.push(res);
    return res;
}



export class Facet {
    constructor(name, field, options={}) {
        this.name = name;
        this.field = field;
        this.source = options.source || "imlgs";
        this.v = options.sel_value  || UNSELECTED;
        this._where = options.where || field;
        this.input = null;
    }

    get value() {
        if (this.input === null) {
            return this.v;
        }
        return this.input.value;
    }

    async values(facets) {
        const rows = await facets.valuesFor(this);
        const res = [UNSELECTED, ];
        for (const row of rows) {
            res.push(row.f);
        }
        return res;
    }

    /**
     * Return the value clause for this facet,
     *   SELECT ${facet.valueClause()} FROM source
     */
    valueClause(name="v") {
        return `${this.field} AS ${name}`;
    }

    /**
     * Return the FROM clause for this facet,
     *   SELECT value FROM ${facet.fromClause()}
     */
    fromClause() {
        return `${this.source}`
    }

    /** 
     * Return the where clause for this facet,
     *   SELECT value FROM source WHERE ${facet.whereClause()}
     */
    whereClause() {
        return `${this._where}=?`;
    }

    async initialize(facets) {
        const opts = {
            label: this.name,
            value: this.value
        };
        //if (this.value !== UNSELECTED) {
        //    opts.value = this.value;
        //}
        this.input = Inputs.select(await this.values(facets), opts)
    }
}


export class IMLGSData {
    constructor(data_source, data_view, display_fields, max_distinct) {
        this.data_source = data_source;
        this.data_view = data_view ? data_view : "imlgs";
        this.field_sets = {
            "table": display_fields ? display_fields : DEFAULT_DISPLAY_FIELDS,
            "record": DEFAULT_RECORD_FIELDS,
            "spatial": DEFAULT_SPATIAL_FIELDS
        }
        //this.display_fields = display_fields ? display_fields : DEFAULT_DISPLAY_FIELDS;
        this.where_clause_join = " AND ";
        this.ddb = null;
        this.MAX_DISTINCT = max_distinct || 7000;
    }

    async initialize() {
        this.ddb = await DuckDBClient.of();
        await this.ddb.query(`create view ${this.data_view} as select * from read_parquet('${this.data_source}')`);
    }

    get db() {
        return this.ddb;
    }

    get tbl() {
        return this.data_view;
    }
    
    getWhereClause(inputs, where_clause_extra="") {
        const params = [];
        let where_clause = "";
        if (inputs.length > 0) {
            const clauses = [];
            for (const inp of inputs) {
                if (inp.v) {
                    clauses.push(inp.c);
                    params.push(inp.v);
                }
            }
            if (params.length > 0){
                where_clause += ` WHERE ${clauses.join(this.where_clause_join)}`;
            }
        }
        if (where_clause_extra !== "") {
            if (where_clause !== "") {
                where_clause = `${where_clause} ${this.where_clause_join} ${where_clause_extra}`;
            } else {
                where_clause = ` WHERE ${where_clause_extra}`;
            }

        }
        //console.log(`getWhereClause: ${where_clause}`);
        return new WhereClause(where_clause, params);
    }    

    async getColumns() {
        const q = `select column_name, column_type from (describe ${this.data_view})`;
        const result = await this.ddb.query(q);
        //return Array.from(result, (row) => [row.column_name, row.column_type]);
        return result;
    }

    async columnStats(column, key) {
        const q = `select '${key}' as k, min(${column}) as min, max(${column}) as max, count(distinct ${column}) as n from ${this.data_view}`;
        return await this.ddb.queryRow(q);
}    

    async count(where_clause=null) {
        let query = `SELECT count(*) AS n FROM ${this.data_view}`;
        let params = [];
        if (where_clause !== null) {
            query = query + where_clause.clause;
            params = where_clause.params;
        }
        const result = await this.ddb.queryRow(query, params);
        return result.n;
    }

    async countDistinct(column, where_clause=null) {
        let query = `SELECT count(distinct ${column}) AS n FROM ${this.data_view}`;
        let params = [];
        if (where_clause !== null) {
            query = query + where_clause.clause;
            params = where_clause.params;
        }
        const result = await this.ddb.queryRow(query, params);
        return result.n;
    }

    async distinct(column, where_clause=null) {
        let query = `SELECT distinct ${column} AS d FROM ${this.data_view} order by d`;
        let params = [];
        if (where_clause !== null) {
            query = query + where_clause.clause;
            params = where_clause.params;
        }
        const result = await this.ddb.query(query, params || []);
        return result;
    }

    async distinctCounts(column, where_clause=null) {
        let query = `SELECT ${column} AS d, count(*) AS n FROM ${this.data_view} `
        let params = [];
        if (where_clause !== null) {
            query = query + where_clause.clause;
            params = where_clause.params;
        }
        query += ` GROUP BY d ORDER BY d`;
        const result = await this.ddb.query(query, params);
        return result;
    }

    async getDisplayRecords(where_clause) {
        return this.select(this.field_sets.table, where_clause);
    }

    async select(fields, where_clause, single_row=false) {
        let query = `SELECT ${fields.join(", ")} FROM ${this.data_view}`;
        let params = [];
        if (where_clause !== null) {
            // Always add a space, just in case clause comes in without one.
            query = query + " " + where_clause.clause;
            params = where_clause.params;
        }
        if (single_row) {
            return this.ddb.queryRow(query, params)
        }
        return this.ddb.query(query, params);
    }

    /**
     * Perform a general regexp search across a few common fields.
     * @param {*} term 
     */
    async search(term){
        const query = `SELECT ${this.field_sets.table.join(',')} FROM ${this.data_view} WHERE 
        regexp_matches(imlgs ,?,'i') OR regexp_matches(sample,?,'i') OR regexp_matches(igsn,?,'i')
        OR regexp_matches(description,?,'i')`;
        return this.db.query(query, [term, term, term, term]);
    }

    /**
     * retrieve 
     * @param {*} pid 
     */
    async getRecord(pid) {
        const clause = {
            "clause":" WHERE imlgs=?",
            "params": [pid]
        }
        return this.select(this.field_sets.record, clause, true);
    }

    recordToJSONLD(record) {
        const jld = {
            "@context": "https://schema.org/",
            "@type": "Thing",
            "isPartOf": window.location.origin,
            "identifier": [{
                "@type":"PropertyValue",
                "propertyID":"https://w3id.org/imlgs/sample",
                "value": record.sample
            }, {
                "@type":"PropertyValue",
                "propertyID":"https://w3id.org/imlgs/id",
                "value": record.imlgs
            }],
            "name": record.sample
        };
        if (record.igsn) {
            jld.identifier.push({
                "@type":"PropertyValue",
                "propertyID":"https://igsn.org/",
                "value": record.igsn
            });
        }
        return jld;
    }


    async getRecordHtml(imlgs) {
        const R= await this.getRecord(imlgs);
        return html`<div class="card">
<table style="width:100%; max-width:100%;">
<tbody>
<tr><td>Repository</td><td>${R.facility.facility}</td></tr>
<tr><td>Ship/Platform</td><td>${R.platform}</td></tr>
<tr><td>Cruise ID</td><td>${R.cruise.cruise}</td></tr>
<tr><td>Sample ID</td><td>${R.sample}</td></tr>
<tr><td>Sampling Device</td><td>${R.device}</td></tr>
<tr><td>Location</td><td><code>${R.geometry}</code></td></tr>
<tr><td>Water Depth (m)</td><td>${R.water_depth}</td></tr>
<tr><td>Date Sample Collected</td><td>${jdToDate(R.begin_jd)}</td></tr>
<tr><td>Principal Investigator</td><td>${R.pi}</td></tr>
<tr><td>Physiographic Province</td><td>${R.province}</td></tr>
<tr><td>Lake</td><td>${R.lake}</td></tr>
<tr><td>Core Length(cm)</td><td>${R.cored_length}</td></tr>
<tr><td>Core Diamter(cm)</td><td>${R.cored_diam}</td></tr>
<tr><td>Sample Comments</td><td>${R.sample_comments}</td></tr>
<tr><td>Repository Archive Overview</td><td><a target="_blank" href='${R.facility.other_link}'>${R.facility.other_link}</a></td></tr>
</tbody>
</table>
    </div>

    <div class="card">
<table style="width:100%; max-width:100%;">
<thead><tr>
<th>Depth</th><th>Geologic Age</th><th>Texture</th><th>Composition</th><th>Lithology</th><th>Comments</th>
</tr></thhead>
<tbody>${Array.from(R.intervals, (interval, i) => html.fragment
`<tr><td>${interval.depth_top} - ${interval.depth_bot}</td>
<td>${interval.ages}</td>
<td>${interval.textures}</td>
<td>${interval.comps}</td>
<td>${interval.liths}</td>
<td>${intervalComment(interval)}</td>
</tr>`)}
</tbody></table>
    </div>
    `        
    }

    async newTextInput(column, label) {
        const datalist = [];
        const nvalues = await this.countDistinct(column, NULL_WHERE_CLAUSE);
        if (nvalues < this.MAX_DISTINCT) {
            const rows = await this.distinctCounts(column, NULL_WHERE_CLAUSE)
            for (const v of rows) {
                //datalist.push(`${v.d} (${v.n})`);
                datalist.push(`${v.d}`);
            }
        }
        return Inputs.text({
            label: `${label} (${nvalues})`,
            submit: false,
            datalist: datalist,
            autocomplete:"off"
        }            
        );
    }

    async newSelectInput(column, label) {
        const nvalues = await this.countDistinct(column, NULL_WHERE_CLAUSE);
        const datalist = [['All', nvalues]];
        if (nvalues < this.MAX_DISTINCT) {
            const rows = await this.distinctCounts(column, NULL_WHERE_CLAUSE)
            for (const v of rows) {
                datalist.push([v.d, v.n]);
            }
        }
        return Inputs.select(datalist, {
            label: `${label} (${nvalues})`,
            multiple: false,
            submit: true,
            format: (v) => {return `${v[0]} (${v[1]})`},
            valueof: (v) => {
                if (v[0] === 'All') {
                    return ''
                };
                return v[0];
            }
            }            
        );
    }


    /**
     * Adds an observer to an input and yield a custom value, in this 
     * case, the input value and a template string associated with the input.
     * 
     * This is used to generate an SQL where clause for the input value
     * @param {*} inputer 
     * @param {*} template 
     * @returns 
     */
    async newInputObserver(column, label, template) {
        //const inputer = await this.newTextInput(column, label);
        const inputer = await this.newSelectInput(column, label);
        const res = Generators.observe((notify) => {
            const inputted = () => {
                notify({"v":inputer.value, "c": template});
            };
            inputted();
            inputer.addEventListener("input", inputted);
            return () => inputer.removeEventListener("input", inputted);
        });
        ui_inputs.push(res);
        return [inputer, res];
    }

    /**
     * Adds an observer to an input and yield a custom value, in this 
     * case, the input value and a template string associated with the input.
     * 
     * This is used to generate an SQL where clause for the input value
     * @param {*} inputer 
     * @param {*} template 
     * @returns 
     */
    async newTextInputObserver(column, label, template) {
        //const inputer = await this.newTextInput(column, label);
        const inputer = await this.newTextInput(column, label);
        const res = Generators.observe((notify) => {
            const inputted = () => {
                notify({"v":inputer.value, "c": template});
            };
            inputted();
            inputer.addEventListener("input", inputted);
            return () => inputer.removeEventListener("input", inputted);
        });
        ui_inputs.push(res);
        return [inputer, res];
    }

}


//-----
// From: https://github.com/stevebest/julian/blob/master/index.js

const DAY = 86400000;
const UNIX_EPOCH_JULIAN_DATE = 2440587.5;

function convertToDate(julian) {
  return new Date((Number(julian) - UNIX_EPOCH_JULIAN_DATE) * DAY);
};
//---------

export function jdToDate(jd) {
    if (jd) {
        const d = convertToDate(jd);
        const year = new Intl.DateTimeFormat('en', { year: 'numeric' }).format(d);
        const month = new Intl.DateTimeFormat('en', { month: '2-digit' }).format(d);
        const day = new Intl.DateTimeFormat('en', { day: '2-digit' }).format(d);
        return `${year}-${month}-${day}`;
    }
    return jd;
}

function formatDict(d) {
    if (!d) {
        return "";
    }
    if (typeof d === 'object') {
        const entries = [];
        for (const [k, v] of Object.entries(d)) {
            entries.push(`${k}: ${v}`);
        }
        return entries.join("<br />")
    };
    return d;
}

export function intervalComment(interval) {
    //console.log(interval)
    const c = [formatDict(interval.int_comments)];
    if (interval.description) {
        c.push(formatDict(interval.description));
    }
    if (interval.remarks) {
        c.push(formatDict(interval.remarks))
    }
    return c.join("; ");
}


/** WIP - from https://observablehq.com/@tophtucker/custom-table-click-to-select-row
 * Need to update this to work in Framework.
export function CTable(data, keys = Object.keys(data[0])) {
  const onclick = (e, row) => {
    for (const row of node.querySelectorAll("tbody tr")) row.style.background = "none";
    e.currentTarget.style.background = "#eee";
    set(node, row);
  };
  const node = html`<div style="max-height: 500px; overflow-y: auto;">
    <table style="margin: 0; border-collapse: separate; border-spacing: 0;">
      <thead>
        <tr style="border-bottom: none;">
          ${keys.map(key => html`<th style="position: sticky; top: 0; border-bottom: solid 1px #ccc; background: #fff;">
            ${key}
          </th>`)}
        </tr>
      </thead>
      <tbody>
        ${data.map(row => html`<tr onclick=${(e) => onclick(e, row)}>
          ${keys.map(key => html`<td style="border-bottom: solid 1px #eee;">
            ${row[key]}
          </td>`)}
        </tr>`)}
      </tbody>
    </table>
  </div>`;
  set(node, null);
  return node;
}
*/
