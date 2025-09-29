import logging
import click
import duckdb

_L = logging.getLogger("imlgs")

@click.group("imlgs")
def click_main():
    pass


@click_main.command("toparquet")
@click.argument("csv_source", type=click.Path(exists=True))
@click.argument("pq_dest", type=click.Path())
def csv_to_parquet(csv_source, pq_dest):
    coldef = [
        {
            "label":"Repository",
            "colname":"repository", 
            "coltype":"VARCHAR DEFAULT NULL",
        },
        {
            "label": "Ship/Platform",
            "colname":"ship_platform", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Cruise ID",
            "colname":"cruiseid", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Sample ID",
            "colname":"sampleid", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Sampling Device",
            "colname":"sampl_device", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Date Sample Collected",
            "colname":"coll_date", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Date Sample Collection Ended",
            "colname":"coll_date_end", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Latitude",
            "colname":"latitude", 
            "coltype":"DOUBLE DEFAULT NULL"
        },
        {
            "label": "Ending Latitude",
            "colname":"latitude_end", 
            "coltype":"DOUBLE DEFAULT NULL"
        },
        {
            "label": "Longitude",
            "colname":"longitude", 
            "coltype":"DOUBLE DEFAULT NULL"
        },
        {
            "label": "Ending Longitude",
            "colname":"longitude_end", 
            "coltype":"DOUBLE DEFAULT NULL"
        },
        {
            "label": "Water Depth (m)",
            "colname":"depth", 
            "coltype":"DOUBLE DEFAULT NULL"
        },
        {
            "label": "Ending Water Depth (m)",
            "colname":"depth_end", 
            "coltype":"DOUBLE DEFAULT NULL"
        },
        {
            "label": "Storage Method",
            "colname":"storage_method", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Core Length (cm)",
            "colname":"core_length", 
            "coltype":"DOUBLE DEFAULT NULL"
        },
        {
            "label": "Core Diameter (cm)",
            "colname":"core_diameter", 
            "coltype":"DOUBLE DEFAULT NULL"
        },
        {
            "label": "Principal Investigator",
            "colname":"p_investigator", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Physiographic Province",
            "colname":"physio_province", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Lake",
            "colname":"lake", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "IGSN",
            "colname":"igsn", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Alternate Cruise/Leg",
            "colname":"alt_cruise", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Sample Comments",
            "colname":"comments", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "Data and Information for Sample",
            "colname":"sample_info", 
            "coltype":"VARCHAR DEFAULT NULL"
        },
        {
            "label": "IMLGS Number",
            "colname":"imlgs_number", 
            "coltype":"VARCHAR PRIMARY KEY"
        },
    ]

    colnames = []
    coldefstr = "\n{\n"
    for col in coldef:
        coldefstr += f"  '{col['colname']}': '{col['coltype']}',\n"
        colnames.append(f"'{col['colname']}'")
    coldefstr += "}"
    colnames = "[\n" + ",\n".join(colnames) + "]"

    q = f"CREATE TABLE imlgs AS SELECT * FROM read_csv('{csv_source}',skip=1, names={colnames});"
    print(q)


if __name__ == "__main__":
    click_main()
