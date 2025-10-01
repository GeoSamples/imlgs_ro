'''
Script to download the detail JSON records from the iMLGS API
'''
import asyncio
import json
import logging
import os.path
import random

import genson
import httpx
import juliandate

_TESTING = False
IMLGS_API = "https://www.ngdc.noaa.gov/geosamples-api/api"
PAGE_SIZE = 10 if _TESTING else 2000


def getLogger():
    return logging.getLogger("get_detail_json")


def datestr_to_jd(dstr: str|None) -> tuple[float, float] | tuple[None, None]:
    if dstr is None:
        return (None, None)
    dd = 182.0
    mnth = 6
    dy = 15
    yr = int(dstr[:4])
    # There's one occurrence of year 9001
    # Other records from New Horizon WHOI are from 2001
    if yr == 9001:
        yr = 2001
    if len(dstr) >= 6:
        mnth = int(dstr[4:6])
        dd = 15.0
        if (len(dstr) >= 8):
            dy = int(dstr[6:8])
            dd = 0.5
    jd = juliandate.from_gregorian(yr, mnth, dy)
    return (jd, dd)


class IMLGS:

    def __init__(self, dest_folder="."):
        headers = {
            "User-Agent":"Python;IMLGS-archiver;v0.1",
            "Accept": "application/json"
        }
        #self.client = httpx.Client(timeout=30, headers=headers)
        self.client = httpx.AsyncClient(timeout=30, headers=headers, limits=httpx.Limits(max_connections=5, max_keepalive_connections=5))
        self.dest = os.path.abspath(dest_folder)
        self.pids = None

    def _list_identifiers(self, max_rows:int=-1):
        """Download the list of identifiers by paging through the summary records.
        """
        _L = getLogger()
        url = f"{IMLGS_API}/samples/summary"
        params = {
            "items_per_page": PAGE_SIZE,
            "page":1,   
        }
        _total = 0
        if max_rows < 0:
            max_rows = 9999999
        _total_pages = 0
        while _total < max_rows:
            _L.info("Page %s/%s", params['page'], _total_pages)
            response = self.client.get(url, params=params)
            data = response.json()
            if _total_pages < 1:
                _total_pages = data.get("total_pages")
            for item in data.get("items", []):
                _id = item.get("imlgs")
                yield _id
                _total += 1
            if data["page"] == _total_pages:
                break
            params["page"] += 1

    def load_identifiers(self, reload:bool=False):
        """Load the list of identifiers. If reload, then re-download the lsit.
        """
        _L = getLogger()
        pid_file = os.path.join(self.dest, "pids.json")
        if not reload and os.path.exists:
            with open(pid_file, "r") as src:
                self.pids = json.load(src)
            _L.info("Loaded %s pids", len(self.pids))
            return
        self.pids = []
        _L.info("Loading identifiers from API...")
        for pid in self._list_identifiers():
            self.pids.append(pid)
        with open(pid_file,"w") as dest:
            json.dump(self.pids, dest)
        _L.info("Done loading %s pids", len(self.pids))

    def _record_path(self, id:str)->str:
        _sub = id[:8]
        _path = os.path.join(self.dest, _sub)
        os.makedirs(_path, exist_ok=True)
        return os.path.join(_path, f"{id}.json")
    
    def fix_interval(self, i:dict)->dict:
        '''
      id INTEGER, facility STRUCT(id INTEGER, facility VARCHAR, facility_code VARCHAR, other_link VARCHAR), platform VARCHAR, 
      cruise VARCHAR, "sample" VARCHAR, device VARCHAR, "interval" INTEGER, ages VARCHAR[], int_comments VARCHAR, imlgs VARCHAR, depth_top INTEGER,
      depth_bot INTEGER, text1 VARCHAR, text2 VARCHAR, comp1 VARCHAR, lith2 VARCHAR, comp2 VARCHAR, lith1 VARCHAR, exhaust_code VARCHAR,
      lake VARCHAR, munsell_code VARCHAR, description VARCHAR, rock_lith VARCHAR, remark VARCHAR, rock_min VARCHAR, weath_meta VARCHAR,
      weight DOUBLE, comp3 VARCHAR, comp4 VARCHAR, comp5 VARCHAR, comp6 VARCHAR)
       '''
        textures = []
        comps = []
        liths = []
        n = {}
        for k,v in i.items():
            if k in ["text1", "text2"]:
                textures.append(v)
            elif k in ["comp1", "comp2", "comp3", "comp4", "comp5", "comp6"]:
                comps.append(v)
            elif k in ["lith1", "lith2"]:
                liths.append(v)
            elif k in ["facility", "imlgs", "cruise", "sample", "device", "lake", "platform", ]:
                # These properties are just repeats of the top level
                pass

            else:
                n[k] = v
        n["textures"] = textures
        n["comps"] = comps
        n["liths"] = liths
        return n

    async def get_record(self, id:str):
        """Retrieve a single record, downloading if necessary.
        """
        _path = self._record_path(id)
        if os.path.exists(_path):
            with open(_path, "r") as src:
                data = json.load(src)
                if "begin_id" not in data:
                    jd_begin = datestr_to_jd(data.get("begin_date", None))
                    jd_end = datestr_to_jd(data.get("end_date", None))
                    data["begin_jd"] = jd_begin[0]
                    data["begin_jderr"] = jd_begin[1]
                    data["end_jd"] = jd_end[0]
                    data["end_jderr"] = jd_end[1]
                for i in range(0, len(data.get("intervals", []))):
                    data["intervals"][i] = self.fix_interval(data["intervals"][i])
                return data
        _L = getLogger()
        _L.info("retrieving %s", id)        
        url = f"{IMLGS_API}/samples/detail/{id}"
        response = await self.client.get(url)
        data = response.json()
        jd_begin = datestr_to_jd(data.get("begin_date", None))
        jd_end = datestr_to_jd(data.get("end_date", None))
        data["begin_jd"] = jd_begin[0]
        data["begin_jderr"] = jd_begin[1]
        data["end_jd"] = jd_end[0]
        data["end_jderr"] = jd_end[1]
        with open(_path, "w") as dest:
            json.dump(data, dest)
        _L.info("completed %s", id)        
        return data

    async def load_all_records(self, limit:int=-1):
        """Ensure all the documents in self.pids are downloaded.
        """
        _L = getLogger()
        if limit < 0:
            limit = 9999999
        self.load_identifiers()
        _n = 0
        tasks = []
        for pid in self.pids:
            _L.info("Getting %s : %s...", _n, pid)
            tasks.append(self.get_record(pid))
            _n += 1
            if _n > limit:
                _L.info("Breaking at %s/%s", _n, len(self.pids))
                break
            if len(tasks) > 50:
                _L.info("Waiting for batch...")
                await asyncio.gather(*tasks)
                tasks = []
        _L.info("Waiting for batch...")
        await asyncio.gather(*tasks)
        _L.info("Done.")


    async def compute_json_schema(self, seed_id:str|None=None, max_docs:int=10):
        """Infer a JSON schema by examining some or all of the JSON records.
        """
        _L = getLogger()
        builder = genson.SchemaBuilder()
        self.load_identifiers()
        if seed_id is None:
            idx = int(random.random() * len(self.pids))
            seed_id = self.pids[idx]
        _L.info("Seeding schema with %s", seed_id)
        seed = await self.get_record(seed_id)
        builder.add_object(seed)
        if max_docs < 1:
            max_docs = len(self.pids)
            population = range(max_docs)
        else:
            population = random.sample(range(len(self.pids)), max_docs)
        n = 0
        for idx in population:
            pid = self.pids[idx]
            if n % 1000 == 0:
                _L.info("Loaded %s / %s.", n, max_docs)
            builder.add_object(await self.get_record(pid))
            n += 1
        print(builder.to_json(indent=2))

    async def to_nl_json(self, dest_fn:str="imlgs_records_2.jsonl"):
        """Generate a single new-line delimited json file containing all records.
        """
        _L = getLogger()
        self.load_identifiers()
        n = 0
        nrecs = len(self.pids)
        with open(dest_fn, "w") as dest:
            for pid in self.pids:
                if n % 1000 == 0:
                    _L.info("Loaded %s / %s", n, nrecs)
                record = await self.get_record(pid)
                json.dump(record, dest)
                dest.write("\n")
                n += 1
        _L.info("Done.")


async def main():
    logging.basicConfig(level=logging.INFO)
    _L = getLogger()
    #num_to_get = 100 if _TESTING else 999999
    imlgs = IMLGS(dest_folder = "../data")
    #await imlgs.load_all_records(limit=num_to_get)
    #await imlgs.compute_json_schema(max_docs=-1)
    await imlgs.to_nl_json()


if __name__ == "__main__":
    asyncio.run(main())