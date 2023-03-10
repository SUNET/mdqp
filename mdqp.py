#!/usr/bin/env python

import datetime
import hashlib
import os
import shutil
import sys
import tempfile
import xml.etree.ElementTree as ET

import requests
from persistqueue import SQLiteQueue


def sha1sum(filename):
    h = hashlib.sha1()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def download_signed_metadata(mdq, destination_dir, shasum):
    baseurl = f"{mdq}/entities/" + "%7Bsha1%7D"
    metadata_url = f"{baseurl}{shasum}"
    response = requests.get(metadata_url)
    # Ensure fully downloaded files in signed_metadata_dir
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(response.content)
    shutil.move(tmp.name, destination_dir + "/%7Bsha1%7D" + shasum)


def inspect_file(file):

    entity_id = None
    entity_sha = None

    try:
        tree = ET.parse(file)
    except ET.ParseError:
        print(f"Can't parse {file} - skipping")
        return {}

    root = tree.getroot()

    try:
        entity_id = root.attrib["entityID"]
    except KeyError:
        print(f"No entityID found on {entity} - skipping")

    if entity_id:
        entityid_encoded = hashlib.sha1(entity_id.encode("utf-8"))
        entity_sha = entityid_encoded.hexdigest()

    return {"entity_id": entity_id, "entity_sha": entity_sha}


def main():
    BASEDIR = os.environ["BASEDIR"]
    MDQ_SERVICE = os.environ["MDQ_SERVICE"]
    RPH = int(os.environ["RPH"])

    now = datetime.datetime.now()
    hour = now.hour

    runs_left = (24 - hour) * RPH
    print(f"Runs left today: {runs_left}")

    incoming_dir = f"{BASEDIR}/incoming_metadata"
    seen_metadata_dir = f"{BASEDIR}/seen_metadata"
    signed_metadata_dir = f"{BASEDIR}/signed_metadata/entities"
    queues_dir = f"{BASEDIR}/queue"

    full_sync = False
    if not os.path.isdir(seen_metadata_dir):
        # If seen_metadata_dir doesn't exist we need to handle all entities as
        # unhandled. Remove queues aswell.
        os.makedirs(seen_metadata_dir)
        full_sync = True
        if os.path.exists(queues_dir):
            shutil.rmtree(queues_dir)

    for dir in [incoming_dir, signed_metadata_dir]:
        if not os.path.isdir(dir):
            os.makedirs(dir)
    # Merge queues when or if priority is added to persistqueue?
    queue_daily = SQLiteQueue(f"{queues_dir}/daily_queue", auto_commit=False)
    queue_delta = SQLiteQueue(f"{queues_dir}/delta_queue", auto_commit=False)

    for entity in os.listdir(incoming_dir):

        entity_metadata = inspect_file(incoming_dir + "/" + entity)
        incoming_file = incoming_dir + "/" + entity

        if not entity_metadata:
            print(f"Can go further with {entity} due to parsing errors")
            continue

        message_to_enqueue = dict(
            file=entity,
            entityid=entity_metadata["entity_id"],
            shasum=entity_metadata["entity_sha"],
        )

        if full_sync:
            print(f"Boostrap of {entity}")
            queue_daily.put(message_to_enqueue)
            shutil.copyfile(incoming_file, seen_metadata_dir + "/" + entity)
            continue

        # new file
        if not os.path.isfile(seen_metadata_dir + "/" + entity):
            print(f"New file {entity}")
            queue_delta.put(message_to_enqueue)
            shutil.copyfile(incoming_file, seen_metadata_dir + "/" + entity)
            continue

        # Changed files
        incoming_sha = sha1sum(incoming_dir + "/" + entity)
        published_sha = sha1sum(seen_metadata_dir + "/" + entity)
        if incoming_sha != published_sha:
            print(f"Modified file {entity}")
            queue_delta.put(message_to_enqueue)
            shutil.copyfile(incoming_file, seen_metadata_dir + "/" + entity)
            continue

    # removed files
    for entity in os.listdir(seen_metadata_dir):
        if not os.path.exists(incoming_dir + "/" + entity):
            entity_metadata = inspect_file(seen_metadata_dir + "/" + entity)
            print(f'Removed file {entity}: {entity_metadata["entity_sha"]}')
            os.remove(seen_metadata_dir + "/" + entity)
            if os.path.exists(
                signed_metadata_dir + "/%7Bsha1%7D" + entity_metadata["entity_sha"]
            ):
                os.remove(
                    signed_metadata_dir + "/%7Bsha1%7D" + entity_metadata["entity_sha"]
                )

    total_queue_size = queue_daily.size + queue_delta.size
    print(f"Total queue: {total_queue_size}")

    if total_queue_size == 0:
        print("No updates to fetch")
        sys.exit()

    operations_this_run = int(total_queue_size / runs_left) + 1
    print(f"Updates process this run: {operations_this_run}")
    operations_counter = 0
    while operations_counter < operations_this_run:
        queue_str = ""
        if queue_delta.size != 0:
            queue_str = "delta"
            queue = queue_delta
        elif queue_daily.size != 0:
            queue_str = "daily"
            queue = queue_daily
        else:
            print("Queues are empty!")
            break
        message = queue.get()
        shasum = message["shasum"]
        entityid = message["entityid"]
        print(f"Working on message from the {queue_str} queue: {entityid} - {shasum}")
        download_signed_metadata(MDQ_SERVICE, signed_metadata_dir, shasum)
        queue.task_done()

        operations_counter += 1


if __name__ == "__main__":
    main()
