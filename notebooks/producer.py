import os
import csv
import json
import time
from concurrent import futures
from kafka import KafkaProducer

# =============================================================
# OmniFleet V003 - Sensor Producer
# =============================================================
# Reads the per-vehicle sensor shards and streams them to Kafka.
#   quick_sensors/[id].csv -> topic vehicle.quick.sensors  (every 3 s)
#   slow_sensors/[id].csv  -> topic vehicle.slow.sensors   (every 60 s)
#
# We stream the FIRST 100 vehicles (0..99). That slice already contains
# every anomaly type (fuel theft, reefer fail, door breach, bad battery,
# engine wear, aggressive driving) so the live map shows real incidents.
#
# How to run (from PowerShell, detached):
#   docker exec -d omnifleet-spark-v003 python /home/jovyan/work/producer.py
# =============================================================

# 1. Kafka producer setup
# kafka:9092 because this runs INSIDE a container on the compose network
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    acks=1,
    compression_type='gzip'
)

# 2. Folder + topic settings (paths inside the container)
QUICK_FOLDER = "/home/jovyan/work/data/quick_sensors"
QUICK_TOPIC = "vehicle.quick.sensors"
QUICK_DELAY = 3.0      # quick sensors tick every 3 seconds

SLOW_FOLDER = "/home/jovyan/work/data/slow_sensors"
SLOW_TOPIC = "vehicle.slow.sensors"
SLOW_DELAY = 60.0      # slow sensors tick every 1 minute

# how many vehicles to stream (0 .. NUM_CARS-1)
NUM_CARS = 100

# checkpoint file so we can resume after a restart
CHECKPOINT_FILE = "/home/jovyan/work/producer_checkpoint_v003.json"

# 3. Columns we send for each topic (full schema -> enrichment needs it)
QUICK_COLUMNS = ['vehicle_id', 'sensor_id', 'timestamp', 'lat', 'lon',
                 'odometer_km', 'rpm', 'throttle_pct', 'accel_ay_abs', 'battery_v']

SLOW_COLUMNS = ['vehicle_id', 'sensor_id', 'timestamp',
                'fuel_amount_l', 'is_door_open', 'cargo_temp_c']


# 4. Two helper functions to manage the checkpoint file
def load_checkpoints():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_checkpoint(file_key, last_row_index):
    # read current checkpoints and update, so parallel threads don't erase each other
    checkpoints = load_checkpoints()
    checkpoints[file_key] = last_row_index
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoints, f)


# 5. Streaming function for a single vehicle file
def stream_one_file(file_path, topic_name, columns_filter, sleep_delay):
    file_name = os.path.basename(file_path)
    vehicle_id = os.path.splitext(file_name)[0]
    file_key = f"{topic_name}_{file_name}"   # unique key per file

    # where did we stop last time for this file?
    checkpoints = load_checkpoints()
    start_row = checkpoints.get(file_key, 0)

    # if the checkpoint says -1 the file is fully done, skip it
    if start_row == -1:
        print(f"[Done] Vehicle {vehicle_id} -> {topic_name} already finished, skipping")
        return

    if start_row > 0:
        print(f"[Resuming] Vehicle {vehicle_id} -> {topic_name} from row {start_row}")
    else:
        print(f"[Started] Vehicle {vehicle_id} -> {topic_name} (every {sleep_delay}s)")

    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for current_idx, row in enumerate(reader):
            # skip rows we already sent before
            if current_idx < start_row:
                continue

            # keep only the columns we want to send
            payload = {col: row[col] for col in columns_filter if col in row}
            if not payload:
                continue

            producer.send(topic=topic_name, key=str(vehicle_id), value=payload)

            # save checkpoint every 50 rows (or every row for the slow stream)
            if current_idx % 50 == 0 or sleep_delay > 10:
                save_checkpoint(file_key, current_idx)

            time.sleep(sleep_delay)

    # file finished -> mark as done with -1 (we do NOT move/delete the file)
    save_checkpoint(file_key, -1)
    print(f"[Finished] Vehicle {vehicle_id} -> {topic_name} reached end of file")


# 6. Main: build the task list and run them in parallel
def main():
    tasks = []

    # one quick task + one slow task per vehicle 0..NUM_CARS-1
    for v in range(NUM_CARS):
        quick_path = os.path.join(QUICK_FOLDER, f"{v}.csv")
        slow_path = os.path.join(SLOW_FOLDER, f"{v}.csv")

        if os.path.exists(quick_path):
            tasks.append((quick_path, QUICK_TOPIC, QUICK_COLUMNS, QUICK_DELAY))
        if os.path.exists(slow_path):
            tasks.append((slow_path, SLOW_TOPIC, SLOW_COLUMNS, SLOW_DELAY))

    if not tasks:
        print("No sensor files found! Check the quick_sensors / slow_sensors folders.")
        return

    print(f"Deploying {len(tasks)} stream workers for {NUM_CARS} vehicles...")

    # 2 streams per car -> need enough threads for all of them at once
    max_workers = NUM_CARS * 2 + 5
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        running = [
            executor.submit(stream_one_file, path, topic, cols, delay)
            for path, topic, cols, delay in tasks
        ]
        # wait until every stream finishes
        futures.wait(running)

    producer.flush()
    print("All sensor streams processed!")


if __name__ == "__main__":
    main()
