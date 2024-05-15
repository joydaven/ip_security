import os
import glob
import subprocess
import geoip2.database
import mysql.connector
import datetime
import json
import logging
import csv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_db(settings):
    logging.info("Connecting to the database...")
    conn = mysql.connector.connect(
        host=settings['host'],
        user=settings['user'],
        password=settings['password'],
        database=settings['database'],
        port=settings['port'],
        allow_local_infile=True  # Enable local infile loading
    )
    logging.info("Database connection established.")
    return conn

def run_zmap(input_file, output_file):
    logging.info(f"Starting ZMap scan for {input_file}...")
    command = f'zmap -p 443 -w {input_file} -M quic_initial -B 800000000 --probe-args="padding:1200" -o {output_file} -v 1'
    try:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in iter(process.stdout.readline, ''):
            logging.info(line.strip())
        process.stdout.close()
        return_code = process.wait()
        if return_code == 0:
            logging.info(f"ZMap scan completed. Results saved to {output_file}")
        else:
            logging.error(f"ZMap scan failed with return code {return_code}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during ZMap scan: {e}")

def enrich_ips_with_geo(ip_list, db_path='GeoIP2-Country.mmdb'):
    logging.info("Enriching IPs with GEO data...")
    geo_data = []
    with geoip2.database.Reader(db_path) as reader:
        for ip in ip_list:
            try:
                response = reader.country(ip)
                geo_data.append({'ip': ip, 'country': response.country.name})
            except geoip2.errors.AddressNotFoundError:
                geo_data.append({'ip': ip, 'country': 'Unknown'})
    logging.info("GEO data enrichment completed.")
    return geo_data

def parse_zmap_results(output_file):
    logging.info(f"Parsing ZMap results from {output_file}...")
    open_ips = []
    with open(output_file, 'r') as file:
        for line in file:
            open_ips.append(line.strip())
    logging.info(f"Parsed {len(open_ips)} open IPs.")
    return open_ips

def write_to_csv(enriched_ips, filepath):
    logging.info(f"Writing enriched IPs to CSV at {filepath}...")
    with open(filepath, 'w', newline='') as file:
        writer = csv.writer(file)
        for ip_data in enriched_ips:
            writer.writerow([ip_data['ip'], ip_data['country'], datetime.datetime.now()])
    logging.info("CSV file created.")

def load_data_infile(conn, filepath):
    logging.info("Loading data into database using LOAD DATA INFILE...")
    cursor = conn.cursor()
    load_query = f"""
    LOAD DATA LOCAL INFILE '{filepath}'
    INTO TABLE scan_results
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    (ip, country, @var1)
    SET timestamp = STR_TO_DATE(@var1, '%Y-%m-%d %H:%i:%s');
    """
    cursor.execute(load_query)
    conn.commit()
    cursor.close()
    logging.info("Data loaded into database using LOAD DATA INFILE.")

def scan_and_upload():
    with open('settings.json') as f:
        settings = json.load(f)

    conn = connect_db(settings)
    extracted_dir = 'extracted'
    results_dir = 'results'
    database_inserts_dir = 'database_inserts'
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(database_inserts_dir, exist_ok=True)

    input_files = glob.glob(f"{extracted_dir}/*")
    for input_file in input_files:
        base_name = os.path.basename(input_file).replace('.txt', '.csv')
        output_file = os.path.join(results_dir, base_name)
        csv_file_path = os.path.join(database_inserts_dir, base_name)
        run_zmap(input_file, output_file)
        open_ips = parse_zmap_results(output_file)
        enriched_ips = enrich_ips_with_geo(open_ips)
        write_to_csv(enriched_ips, csv_file_path)
        load_data_infile(conn, csv_file_path)

if __name__ == "__main__":
    scan_and_upload()
