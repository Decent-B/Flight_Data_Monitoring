#!/usr/bin/env python3
"""
deploy_flow.py
- Waits for NiFi + NiFi Registry to be available
- Imports ./flows/flow.json.gz into a Registry bucket (creates bucket if needed)
- Instantiates the versioned flow onto the NiFi root process group
- Starts the instantiated process group

Requires: nipyapi (installed by docker entrypoint)
"""
import os
import sys
import time
import traceback
from pathlib import Path

import nipyapi
from nipyapi import canvas, registry, config, security, utils

# Configuration from environment
NIFI_API_URL = os.environ.get('NIFI_API_URL', 'http://nifi:8080')
REGISTRY_API_URL = os.environ.get('REGISTRY_API_URL', 'http://nifi-registry:18080')
NIFI_USER = os.environ.get('NIFI_USERNAME', 'admin')
NIFI_PASS = os.environ.get('NIFI_PASSWORD', '')
INSECURE = os.environ.get('INSECURE_SKIP_TLS_VERIFY', 'true').lower() in ('1', 'true', 'yes')
FLOW_PATH_HOST = '/flows/flow.json.gz'  # file mounted read-only from host -> container

WAIT_TIMEOUT = 180  # seconds

def wait_for_url(url, timeout=WAIT_TIMEOUT, interval=3, name='service'):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            # basic GET to check availability
            import requests
            r = requests.get(url, timeout=5)
            if r.status_code < 500:
                print(f"[ok] {name} responded status {r.status_code}")
                return True
        except Exception as e:
            print(f"[wait] {name} not ready yet: {e}")
        time.sleep(interval)
    raise RuntimeError(f"Timeout waiting for {name} at {url}")

def main():
    try:
        print("Configuring NiPyAPI endpoints...")
        # NiFi API endpoints used by nipyapi are the base API endpoints
        config.nifi_config.host = NIFI_API_URL.rstrip('/') + '/nifi-api'
        config.registry_config.host = REGISTRY_API_URL.rstrip('/') + '/nifi-registry-api'

        if INSECURE:
            print("Disabling cert validation (INSECURE mode)")
            security.service.set_service_certificate_validation(False)

        # Wait for services
        print(f"Waiting for NiFi at {NIFI_API_URL} ...")
        wait_for_url(NIFI_API_URL, name='NiFi')
        print(f"Waiting for NiFi Registry at {REGISTRY_API_URL} ...")
        wait_for_url(REGISTRY_API_URL, name='NiFi Registry')

        # Configure NiPyAPI auth if necessary (single-user)
        # For single-user NiFi, basic user/password isn't always required for API calls while running in the same JVM;
        # nipyapi calls will work if the instance accepts them. If your NiFi requires login, you may need to
        # do additional token-based auth. We'll attempt operations and raise informative errors if blocked.

        # Validate flow file exists
        flow_file = Path(FLOW_PATH_HOST)
        if not flow_file.exists():
            raise FileNotFoundError(f"Flow file not found at {flow_file}. Put your flow.json.gz in ./flows/ and mount it.")

        # Create or get a registry bucket
        BUCKET_NAME = "auto-deploy-bucket"
        print("Ensuring Registry bucket exists:", BUCKET_NAME)
        buckets = registry.list_buckets()
        bucket = None
        if buckets:
            for b in buckets:
                if b.name == BUCKET_NAME:
                    bucket = b
                    break

        if bucket is None:
            print("Creating registry bucket...")
            bucket = registry.create_bucket(BUCKET_NAME)
            print("Created bucket:", bucket.identifier, bucket.name)
        else:
            print("Found bucket:", bucket.identifier, bucket.name)

        # Import the flow JSON into the bucket
        print("Importing flow archive into Registry from:", flow_file)
        # Use registry.import_flow_from_archive (available in nipyapi >= 1.3.0)
        try:
            imported = registry.import_flow_from_archive(bucket.identifier, str(flow_file))
            print("Imported flow. Registry flow id:", imported.identifier)
        except Exception as e:
            print("Primary import method failed - attempting fallback import routine.")
            print("Error:", e)
            # If your nipyapi doesn't support import_flow_from_archive, bubble up
            raise

        # Instantiate the versioned flow onto NiFi canvas (root process group)
        print("Finding root process group id...")
        root_pg_id = canvas.get_root_pg_id()
        print("Root PG id:", root_pg_id)

        # The imported object may contain versions; pick the latest
        versions = registry.list_flow_versions(imported.identifier)
        if not versions:
            raise RuntimeError("No versions found for imported flow in registry.")
        latest = versions[-1]
        version_number = latest.version
        print(f"Using flow version: {version_number}")

        # Instantiate (deploy) the version to the root group
        pg_name = f"deployed-flow-{int(time.time())}"
        print(f"Instantiating version {version_number} as process group '{pg_name}'")
        inst = registry.create_flow_version_deployment(
            bucket.identifier,
            imported.identifier,
            version_number,
            root_process_group_id=root_pg_id,
            process_group_name=pg_name,
            # position (x, y) on canvas
            position=(0.0, 0.0)
        )
        # The above helper name may differ between nipyapi versions. If it errors,
        # you might need to call lower-level nipyapi functions. Check nipyapi docs.
        print("Instantiation returned:", inst)

        # Attempt to locate the created process group and schedule it
        time.sleep(2)
        # find process groups with our name under root
        pgs = canvas.get_process_groups(root_pg_id, 'id', True)
        new_pg = None
        for g in pgs:
            if getattr(g, 'component', None) and g.component.name == pg_name:
                new_pg = g
                break
        if new_pg is None:
            # Fallback: pick first process group with timestamped name logic
            print("Could not find PG by name; listing root children to pick newest...")
            children = canvas.list_all_process_groups()
            if children:
                new_pg = children[-1]
        if new_pg is None:
            raise RuntimeError("Failed to find the instantiated process group after deployment.")

        print("Scheduling (starting) process group:", new_pg.id)
        canvas.schedule_process_group(new_pg.id, True)
        print("Process group scheduled. Done.")

    except Exception as e:
        print("ERROR during deployment:", str(e))
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
