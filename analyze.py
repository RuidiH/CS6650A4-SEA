#!/usr/bin/env python3
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

DATA_DIR = "."
MIXES    = ["1_99", "10_90", "50_50", "90_10"]
BINS     = 50

for mix in MIXES:
    prefix = f"run_{mix}"
    # 1) Merge request logs
    req_pattern = os.path.join(DATA_DIR, f"{prefix}_requests*.csv")
    req_files   = sorted(glob.glob(req_pattern))
    if not req_files:
        print(f"No request logs for {mix}, skipping")
        continue

    print(f"🔗 Merging {len(req_files)} request logs for mix {mix}")
    df_list = []
    for fn in req_files:
        try:
            df = pd.read_csv(
                fn,
                engine="python",
                on_bad_lines="skip",
                usecols=[0,1,2,3,4,5]
            )
            if not df.empty:
                df_list.append(df)
        except Exception as e:
            print(f"   ⚠️  Could not parse {fn}: {e}")

    if not df_list:
        print(f"   ✖ No valid request data for {mix}")
        continue

    req_df = pd.concat(df_list, ignore_index=True)
    req_df = req_df[req_df["name"].isin(["/get","/put"])]

    reads  = req_df[req_df["name"] == "/get"]["response_time"].dropna()
    writes = req_df[req_df["name"] == "/put"]["response_time"].dropna()

    # 2) Plot read latency
    plt.figure()
    plt.hist(reads, bins=BINS)
    plt.title(f"Read Latency Distribution ({mix.replace('_','/')})")
    plt.xlabel("Response Time (ms)")
    plt.ylabel("Count")
    out = os.path.join(DATA_DIR, f"read_latency_{mix}.png")
    plt.savefig(out)
    plt.close()
    print(f"   • Saved {out}")

    # 3) Plot write latency
    plt.figure()
    plt.hist(writes, bins=BINS)
    plt.title(f"Write Latency Distribution ({mix.replace('_','/')})")
    plt.xlabel("Response Time (ms)")
    plt.ylabel("Count")
    out = os.path.join(DATA_DIR, f"write_latency_{mix}.png")
    plt.savefig(out)
    plt.close()
    print(f"   • Saved {out}")

    # 4) Merge interval logs
    iv_pattern = os.path.join(DATA_DIR, f"intervals_{mix}*.csv")
    iv_files   = sorted(glob.glob(iv_pattern))
    if iv_files:
        iv_dfs = []
        for fn in iv_files:
            try:
                iv = pd.read_csv(fn, usecols=["interval_ms"])
                if not iv["interval_ms"].dropna().empty:
                    iv_dfs.append(iv)
            except Exception as e:
                print(f"   ⚠️  Could not parse {fn}: {e}")

        if iv_dfs:
            iv_all = pd.concat(iv_dfs, ignore_index=True)["interval_ms"].dropna()
            plt.figure()
            plt.hist(iv_all, bins=BINS)
            plt.title(f"Read-After-Write Interval Distribution ({mix.replace('_','/')})")
            plt.xlabel("Interval (ms)")
            plt.ylabel("Count")
            out = os.path.join(DATA_DIR, f"intervals_{mix}.png")
            plt.savefig(out)
            plt.close()
            print(f"   • Saved {out}")
        else:
            print(f"   ✖ No valid interval data for {mix}")
    else:
        print(f"No interval logs for {mix}, skipping")

    # 5) Cleanup merged files
    for fn in req_files + iv_files:
        try:
            os.remove(fn)
            print(f"   ✖ Removed {fn}")
        except OSError as e:
            print(f"   ⚠️ Could not remove {fn}: {e}")

print("✅ All done.")
