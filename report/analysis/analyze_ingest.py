import json

import matplotlib.pyplot as plt

from datetime import datetime
from typing import List

# - Gesamtzeit (CPU) für Einführung ppn wall clock time
# - Gesamtzeit (CPU) für Einführung ppn cpu time

def parse_timestamp(s):
    dt = datetime.strptime(s, '%H:%M:%S.%f')
    seconds = dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
    return seconds

def get_times_from_file(path: str) -> List[List[float]]:
    res = []
    with open(path, "r") as fp:
        for line in fp:
            json_obj = "{\"x\":" + line.replace("'", '"') + "}"
            arr = json.loads(json_obj)["x"]
            res.append([parse_timestamp(s) for s in arr])
    return res

def plot_cputime(ppns):
    labels = []
    values = []
    for i, vals in ppns.items():
        # sum em up
        agg = 0.0
        cnt = 0
        for arr in vals:
            for x in arr:
                agg += x
                cnt += 1
        labels.append(f"{i}ppn")
        values.append(agg)
        print(i, agg, cnt)
    plt.bar(labels, values)
    plt.xlabel("different processes per node (ppn)")
    plt.ylabel("ingest time in s")
    plt.title("Ingest: Total CPU Time")
    plt.savefig("cputime.png")
    plt.clf()

def plot_wallclocktime(ppns):
    labels = []
    values = []
    for i, vals in ppns.items():
        # get max time from each load ingestors
        max_agg = 0.0
        cnt = 0
        for arr in vals:
            agg = 0.0
            for x in arr:
                agg += x
                cnt += 1
            if agg > max_agg:
                max_agg = agg
        labels.append(f"{i}ppn")
        values.append(max_agg)
        print(i, max_agg, cnt)
    plt.bar(labels, values)
    plt.xlabel("different processes per node (ppn)")
    plt.ylabel("ingest time in s")
    plt.title("Ingest: Total Wall Clock Time")
    plt.savefig("wallclocktime.png")
    plt.clf()

testfile = "./nyc_baseline/ingest_nyc_3n_1ppn"

P_3N_1PPN = "./nyc_baseline/ingest_nyc_3n_1ppn"
P_3N_2PPN = "./nyc_baseline/ingest_nyc_3n_2ppn"
P_3N_4PPN = "./nyc_baseline/ingest_nyc_3n_4ppn"
P_3N_8PPN = "./nyc_baseline/ingest_nyc_3n_8ppn"

PPNS = {
    1: get_times_from_file(P_3N_1PPN),
    2: get_times_from_file(P_3N_2PPN),
    4: get_times_from_file(P_3N_4PPN),
    8: get_times_from_file(P_3N_8PPN),
}

print("CPU")
plot_cputime(PPNS)
print("\nWALL")
plot_wallclocktime(PPNS)
print("\nOverTime")
