import json

import matplotlib.pyplot as plt

# order of steps
# 00: match all
# 01: match all 100
# 02: match all 1000
# 03: match all 10000
# 04: range
# 05: range 100
# 06: range 1000
# 07: range 10000
# 08: range 1000 sleep 0.0
# 09: range 1000 sleep 0.02
# 10: range 1000 sleep 0.05
# 11: range 1000 sleep 0.1
# 12: range 1000 sleep 0.2
# 13: range 1000 sleep 0.5
# 14: range 1000 sleep 1
# ...aggs
def extract_range_aggs(path):
    with open(path, "r") as fp:
        j = json.load(fp)
    return {
        "range10": j[4],
        "range100": j[5],
        "range1000": j[6],
        "range10000": j[7],
        "range1000s0": j[8],
        "range1000s0.02": j[9],
        "range1000s0.05": j[10],
        "range1000s0.1": j[11],
        "range1000s0.2": j[12],
        "range1000s0.5": j[13],
        "range1000s1": j[14]
    }

# PRO PPN

# Plot 1: range query throughput along doc size, plot per ppn
def plot_query_throughput(PPNS):
    for i, ppn_vals in PPNS.items():
        responses = {}
        for label, j in ppn_vals.items():
            flattened_responses = sum(j["search_results"][0]["responses"], [])

            # agg
            sum_ = 0.0
            cnt = 0
            for obj in flattened_responses:
                sum_ += obj["docs"]
                cnt += 1

            # add avg
            responses[label] = sum_

        xs = [10, 100, 1000, 10000]
        ys = [responses["range10"], responses["range100"], 
              responses["range1000"], responses["range10000"]]
        plt.plot(xs, ys, marker="x", label=f"{i}ppn")
    plt.legend()
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("Docs sent per BULK request")
    plt.ylabel("total documents sent in 3min")
    plt.title("Query: Throughput per BULK size")
    plt.savefig("querythroughput.png")
    plt.clf()

def plot_query_throughput_sleep(PPNS):
    for i, ppn_vals in PPNS.items():
        responses = {}
        for label, j in ppn_vals.items():
            flattened_responses = sum(j["search_results"][0]["responses"], [])

            # agg
            sum_ = 0.0
            cnt = 0
            for obj in flattened_responses:
                sum_ += obj["docs"]
                cnt += 1

            # add avg
            responses[label] = sum_

        xs = [0, 0.02, 0.05, 0.1, 0.2, 0.5, 1]
        ys = [
            responses["range1000"],
            responses["range1000s0.02"],
            responses["range1000s0.05"],
            responses["range1000s0.1"],
            responses["range1000s0.2"],
            responses["range1000s0.5"],
            responses["range1000s1"]
        ]
        plt.plot(xs, ys, marker="x", label=f"{i}ppn")
    plt.legend()
    plt.xlabel("sleep in s between request")
    plt.ylabel("total documents sent in 3min")
    plt.xscale("linear")
    plt.yscale("linear")
    plt.title("Query: Throughput with increased sleeps (bulk size 1000)")
    plt.savefig("querythroughputsleep.png")
    plt.clf()

P_3N_1PPN = "./nyc_baseline/query_nyc_3n_1ppn.json"
P_3N_2PPN = "./nyc_baseline/query_nyc_3n_2ppn.json"
P_3N_4PPN = "./nyc_baseline/query_nyc_3n_4ppn.json"
P_3N_8PPN = "./nyc_baseline/query_nyc_3n_8ppn.json"

PPNS = {
    1: extract_range_aggs(P_3N_1PPN),
    2: extract_range_aggs(P_3N_2PPN),
    4: extract_range_aggs(P_3N_4PPN),
    8: extract_range_aggs(P_3N_8PPN)
}

plot_query_throughput(PPNS)
plot_query_throughput_sleep(PPNS)
