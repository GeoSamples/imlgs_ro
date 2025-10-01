# Version info

```js
const ddb = await DuckDBClient.of()
```

```js
const ddbver = await ddb.queryRow("select version() as duckdb_version");
view(html`<p>Duckdb version: ${(ddbver.duckdb_version)}</p>`);

view(Inputs.table(await ddb.query("select * from duckdb_extensions()")));
```

