#!/usr/bin/python3
import argparse
from jsonrpcclient import request, parse, Ok
import json
import requests
from time import sleep


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="rpc blaster", description="Send transactions to the rpc server"
    )

    parser.add_argument("--rpc", type=str, help="rpc server address", required=True)
    parser.add_argument("--data", type=str, help="data file path", required=True)

    args = parser.parse_args()

    rpc_addr = args.rpc
    data_path = args.data

    with open(data_path, "r") as f:
        txns_json = json.load(f)

    # read from a json file and send requests to server
    for txn in txns_json:
        raw_txn = txn["transaction"]
        json_rpc_request = request("eth_sendRawTransaction", params=[raw_txn])
        for _ in range(5):
            try:
                response = requests.post(rpc_addr, json=json_rpc_request)
            # rpc server might not be running, retry..
            except Exception as e:
                print("rpc connection error: ", e)
                sleep(0.5)
                continue
            break
        
        parsed = parse(response.json())

        if isinstance(parsed, Ok):
            resp_hash = parsed.result
            if resp_hash == txn["hash"]:
                txn["submitted"] = True
            else:
                print(f"Expected txn hash: {txn["hash"]}, got {resp_hash}")
        else:
            print("rpc error: ", response.json())

        sleep(1)

    with open(data_path, "w") as f:
        json.dump(txns_json, f)
