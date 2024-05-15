import pandas as pd
import ipaddress
import argparse
from tqdm import tqdm
import os
import re
from datetime import datetime
from multiprocessing import Pool, Manager
import shutil
import logging
import json

# Setup basic logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Load settings from a JSON file
with open('settings.json') as f:
    settings = json.load(f)

# Setup argument parsing
args = argparse.Namespace()
args.csv_file = settings['csv_file']
args.geoname_id = settings['geoname_ids']

def valid_cidr(cidr):
    try:
        network = ipaddress.ip_network(cidr, strict=False)
        return True, network
    except ValueError:
        return False, None

def estimate_total_ips(df):
    total_ips = 0
    for cidr in df['network']:
        is_valid, network = valid_cidr(cidr)
        if is_valid:
            total_ips += network.num_addresses
    return total_ips

def worker_init(q):
    global queue
    queue = q

def is_valid_ip(ip_str):
    """Validates whether the given string is a valid IP address."""
    return re.match(r'^(\d{1,3}\.){3}\d{1,3}$', ip_str) is not None

def expand_network(row, temp_dir, geoname_id, temp_file_index):
    global queue
    is_valid, network = valid_cidr(row['network'])
    if not is_valid:
        queue.put(0)
        return
    
    num_addresses = network.num_addresses
    temp_file_name = f"temp_{geoname_id}_{temp_file_index}.csv"
    temp_file_path = os.path.join(temp_dir, temp_file_name)
        
    try:
        with open(temp_file_path, 'w') as temp_file:
            for ip in network.hosts():
                ip_str = str(ip)
                if not is_valid_ip(ip_str):
                    logging.error(f"Invalid IP generated: {ip_str} from CIDR: {row['network']}")
                    continue
                temp_file.write(f"{ip_str}\n")
        queue.put(num_addresses)
    except IOError as e:
        logging.error(f"Failed to write IPs for {row['network']} to {temp_file_path}: {e}")
        queue.put(0)

def merge_temp_files_into_final_output(temp_dir, output_file_path):
    """Merges temporary files into a final output file with enhanced validation."""
    with open(output_file_path, 'w') as output_file:
        temp_files = sorted([f for f in os.listdir(temp_dir) if f.startswith("temp_")], key=lambda x: int(re.match(r'temp_\d+_(\d+).csv', x).group(1)))
        for temp_file_name in temp_files:
            temp_file_path = os.path.join(temp_dir, temp_file_name)
            with open(temp_file_path, 'r') as temp_file:
                for line in temp_file:
                    ip_str = line.strip()
                    if not is_valid_ip(ip_str):
                        logging.error(f"Invalid IP found during merging: {ip_str}")
                        continue  # Skip invalid IPs
                    output_file.write(ip_str + '\n')

def parallel_expand_cidr_to_ipv4(csv_file, geoname_id=None, processes=4):
    logging.info("Start parallel_expand_cidr_to_ipv4")
    start_time = datetime.now()

    df = pd.read_csv(csv_file, usecols=['network', 'geoname_id'])
    if geoname_id:
        df = df[df['geoname_id'] == geoname_id]
    logging.info(f"Number of rows in dataframe after filtering by geoname_id: {len(df)}")
    
    total_ips = estimate_total_ips(df)
    print(f"Estimated total IPs to expand: {total_ips}")

    manager = Manager()
    queue = manager.Queue()
    pbar = tqdm(total=total_ips, desc="Expanding IPs", unit="ip")
    
    pool = Pool(processes=processes, initializer=worker_init, initargs=(queue,))
    temp_dir = 'temp_expanded'
    os.makedirs(temp_dir, exist_ok=True)

    tasks = []
    temp_file_index = 0
    for index, row in df.iterrows():
        tasks.append(pool.apply_async(expand_network, args=(row, temp_dir, geoname_id, temp_file_index)))
        temp_file_index += 1

    pool.close()
    total_completed = 0
    while total_completed < len(tasks):
        while not queue.empty():
            pbar.update(queue.get())
            total_completed += 1
    pool.join()
    pbar.close()

    end_time = datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    ips_per_second = total_ips / elapsed_time if elapsed_time > 0 else 0
    print(f"Completed in {elapsed_time} seconds, {ips_per_second:.2f} IPs/second.")

    results_dir = 'extracted'
    os.makedirs(results_dir, exist_ok=True)
    output_file_name = f'{geoname_id}.txt'
    output_file_path = os.path.join(results_dir, output_file_name)
    
    merge_temp_files_into_final_output(temp_dir, output_file_path)
    
    shutil.rmtree(temp_dir)
    logging.info(f"Saving expanded IPs to {output_file_path}")
    print(f"Expanded IPs have been saved to {output_file_path}")
    logging.info("End parallel_expand_cidr_to_ipv4")

if __name__ == "__main__":
    for geoname_id in args.geoname_id:
        output_file_path = f'extracted/{geoname_id}.txt'
        if not os.path.exists(output_file_path):
            output_file_path = parallel_expand_cidr_to_ipv4(args.csv_file, geoname_id)
            # No explicit call to validate_final_output function here, assuming it's implemented elsewhere or removed for brevity.
