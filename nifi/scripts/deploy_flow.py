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
import hashlib

import json
import nipyapi
from nipyapi import canvas, registry, config, security, utils, versioning
import requests
import warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import uuid
from pprint import pprint
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configuration from environment
NIFI_API_BASE_URL = os.environ.get('NIFI_API_URL', 'https://nifi:8443')
REGISTRY_API_URL = os.environ.get('REGISTRY_API_URL', 'http://nifi-registry:18080')
NIFI_USER = os.environ.get('NIFI_USERNAME', 'admin')
NIFI_PASS = os.environ.get('NIFI_PASSWORD', '')
INSECURE = os.environ.get('INSECURE_SKIP_TLS_VERIFY', 'true').lower() in ('1', 'true', 'yes')
FLOW_PATH_HOST = '/flows/OpenSkyAPI.json'  # file mounted read-only from host -> container
PG_NAMES = ["OpenSkyAPI", "AviationWeatherAPI"]
PARAM_CONTEXT_NAME = os.environ.get('NIFI_PARAM_CONTEXT', 'flight-kafka')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker-headless.kafka.svc.cluster.local:9092')
OPENSKY_CLIENT_SECRET = os.environ.get('OPENSKY_CLIENT_SECRET', '')

WAIT_TIMEOUT = 180  # seconds

def wait_for_url(url, timeout=WAIT_TIMEOUT, interval=3, name='service'):
    skip_verify = name == 'NiFi' and INSECURE

    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            # basic GET to check availability
            import requests
            r = requests.get(url, timeout=5, verify=not skip_verify)
            if r.status_code < 500:
                print(f"[ok] {name} responded status {r.status_code}")
                return True
        except Exception as e:
            # Suppress InsecureRequestWarning if running in insecure mode
            if skip_verify and 'SSLCertVerificationError' in str(e):
                
                warnings.filterwarnings('ignore', category=InsecureRequestWarning)

            print(f"[wait] {name} not ready yet: {e}")
        time.sleep(interval)
    raise RuntimeError(f"Timeout waiting for {name} at {url}")

def get_nifi_token(nifi_url, username, password, cert_path):
    """Obtains a Bearer Token (JWT) from the NiFi API."""
    token_url = f"{nifi_url}/access/token"
    
    # Send a POST request with the authentication credentials
    response = requests.post(
        token_url,
        data={'username': username, 'password': password},
        verify=cert_path # Ensure you are using the same verification as your upload request
    )
    
    response.raise_for_status()
    
    # The response body is the raw JWT token
    return response.text

def list_parameter_contexts(nifi_url, token, verify_ssl):
    url = f"{nifi_url}/nifi-api/parameter-contexts"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(url, headers=headers, verify=verify_ssl)
    response.raise_for_status()
    return response.json().get('parameterContexts', [])

def get_parameter_context(nifi_url, token, verify_ssl, context_id):
    url = f"{nifi_url}/nifi-api/parameter-contexts/{context_id}"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(url, headers=headers, verify=verify_ssl)
    response.raise_for_status()
    return response.json()

def upsert_parameter_context(nifi_url, token, verify_ssl, name, parameters):
    param_list = [
        {'parameter': {'name': key, 'value': value, 'sensitive': False}}
        for key, value in parameters.items()
    ]
    for ctx in list_parameter_contexts(nifi_url, token, verify_ssl):
        if ctx.get('component', {}).get('name') == name:
            context_id = ctx.get('id')
            current = get_parameter_context(nifi_url, token, verify_ssl, context_id)
            payload = {
                'revision': current.get('revision'),
                'component': {
                    'id': context_id,
                    'name': name,
                    'parameters': param_list,
                    'inheritedParameterContexts': current.get('component', {}).get('inheritedParameterContexts', [])
                }
            }
            url = f"{nifi_url}/nifi-api/parameter-contexts/{context_id}"
            headers = {
                'Authorization': f'Bearer {token}'
            }
            response = requests.put(url, headers=headers, json=payload, verify=verify_ssl)
            response.raise_for_status()
            return context_id

    payload = {
        'revision': {'version': 0},
        'component': {
            'name': name,
            'parameters': param_list
        }
    }
    url = f"{nifi_url}/nifi-api/parameter-contexts"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.post(url, headers=headers, json=payload, verify=verify_ssl)
    response.raise_for_status()
    return response.json().get('id')

def assign_parameter_context(nifi_url, token, verify_ssl, pg_id, context_id):
    url = f"{nifi_url}/nifi-api/process-groups/{pg_id}"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    current = requests.get(url, headers=headers, verify=verify_ssl)
    current.raise_for_status()
    current_json = current.json()
    current_context = current_json.get('component', {}).get('parameterContext')
    if current_context and current_context.get('id') == context_id:
        return
    payload = {
        'revision': current_json.get('revision'),
        'component': {
            'id': pg_id,
            'parameterContext': {'id': context_id}
        }
    }
    response = requests.put(url, headers=headers, json=payload, verify=verify_ssl)
    response.raise_for_status()

def get_process_group_entity(nifi_url, token, verify_ssl, pg_id):
    url = f"{nifi_url}/nifi-api/process-groups/{pg_id}"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(url, headers=headers, verify=verify_ssl)
    response.raise_for_status()
    return response.json()

def delete_process_group(nifi_url, token, verify_ssl, pg_id, timeout=60):
    headers = {
        'Authorization': f'Bearer {token}'
    }
    entity = get_process_group_entity(nifi_url, token, verify_ssl, pg_id)
    revision = entity.get('revision', {})
    version = revision.get('version', 0)
    client_id = revision.get('clientId', str(uuid.uuid4()))
    params = {
        'version': version,
        'clientId': client_id,
        'disconnectedNodeAcknowledged': 'true'
    }
    url = f"{nifi_url}/nifi-api/process-groups/{pg_id}"
    response = requests.delete(url, headers=headers, params=params, verify=verify_ssl)
    response.raise_for_status()

    start = time.time()
    while time.time() - start < timeout:
        try:
            requests.get(url, headers=headers, verify=verify_ssl, timeout=5).raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for process group {pg_id} deletion.")

def list_connections_in_pg(nifi_url, token, verify_ssl, pg_id):
    url = f"{nifi_url}/nifi-api/flow/process-groups/{pg_id}"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(url, headers=headers, verify=verify_ssl)
    response.raise_for_status()
    connections = response.json().get('processGroupFlow', {}).get('flow', {}).get('connections', [])
    return [conn.get('id') for conn in connections if conn.get('id')]

def drop_connection_queue(nifi_url, token, verify_ssl, connection_id, timeout=120):
    headers = {
        'Authorization': f'Bearer {token}'
    }
    url = f"{nifi_url}/nifi-api/flowfile-queues/{connection_id}/drop-requests"
    response = requests.post(url, headers=headers, json={'acknowledge': True}, verify=verify_ssl)
    response.raise_for_status()
    drop_request = response.json().get('dropRequest', {})
    drop_id = drop_request.get('id')
    if not drop_id:
        return
    status_url = f"{nifi_url}/nifi-api/flowfile-queues/{connection_id}/drop-requests/{drop_id}"
    start = time.time()
    while time.time() - start < timeout:
        status_resp = requests.get(status_url, headers=headers, verify=verify_ssl)
        status_resp.raise_for_status()
        status = status_resp.json().get('dropRequest', {})
        if status.get('finished'):
            break
        time.sleep(2)
    requests.delete(status_url, headers=headers, verify=verify_ssl)

def upload_flow_via_api(nifi_url, parent_pg_id, group_name, client_id, token, file_path, verify_ssl):
    """
    Performs the POST request to upload the flow definition file and associated metadata.
    """
    url = f"{nifi_url}/nifi-api/process-groups/{parent_pg_id}/process-groups/upload"
    
    # 1. Headers (Authorization is the only required header; Content-Type is handled by requests)
    headers = {
        'Authorization': f'Bearer {token}'
    }

    # 2. Form Fields (These are sent as standard form data parts)
    # The NiFi API uses 'groupName' for the name, but the other curl fields are included for completeness.
    data = {
        'positionX': '0.0',
        'clientId': client_id,
        'disconnectNode': 'true',
        'groupName': group_name,
        'positionY': '0.0'
    }

    # 3. File Part (The file content is sent as a multipart file upload)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found at: {file_path}")

    # 'file' is the required field name by the NiFi API
    files = {
        'file': (
            os.path.basename(file_path),  # Filename
            open(file_path, 'rb'),        # File content as bytes
            'application/json'            # Content type
        )
    }

    print(f"Attempting to upload flow to {url}...")
    
    try:
        # Send the POST request
        response = requests.post(
            url, 
            headers=headers, 
            data=data, 
            files=files, 
            verify=verify_ssl
        )
        
        response.raise_for_status() # Raise exception for 4xx or 5xx errors
        
        print(f"✅ Flow successfully uploaded and new Process Group {group_name} created.")
        return response.json()

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e.response.status_code} - {e.response.reason}")
        print(f"NiFi Response Body: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        return None
    finally:
        # Crucial: Close the file handle after the request completes
        if 'file' in files and files['file'][1]:
            files['file'][1].close()

def set_controller_service_run_status(nifi_url, service_id, current_revision, token, verify_ssl, state):
    """
    Sets the Controller Service's state using the /run-status endpoint.
    This is equivalent to clicking the Start/Stop button in the UI.
    """
    url = f"{nifi_url}/nifi-api/controller-services/{service_id}/run-status"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    payload = {
        'revision': current_revision,
        'state': state,
        'disconnectedNodeAcknowledged': 'true',
        'uiOnly': 'true'
    }
    
    print(f"Setting Controller Service {service_id} state to {state} via /run-status...")
    put_response = requests.put(
        url,
        headers=headers,
        json=payload,
        verify=verify_ssl
    )
    put_response.raise_for_status()
    print("✅ Controller Service updated. Check status for initialization.")

def start_controller_service_run_status(nifi_url, service_id, current_revision, token, verify_ssl):
    set_controller_service_run_status(nifi_url, service_id, current_revision, token, verify_ssl, "ENABLED")

def stop_controller_service_run_status(nifi_url, service_id, current_revision, token, verify_ssl):
    set_controller_service_run_status(nifi_url, service_id, current_revision, token, verify_ssl, "DISABLED")

def extract_controller_service_ids(pg_dict):
    # Controller Services are referenced inside the processors' configuration.
    processors = pg_dict.get('component', {}).get('contents', {}).get('processors', [])
    
    if not processors:
        raise RuntimeError("No processors found in the new process group.")

    # Set to store unique IDs
    unique_controller_ids = set()
    
    for processor in processors:
        processor_name = processor.get('name', 'Unnamed Processor')
        
        properties = processor.get('config', {}).get('properties', {})
        
        for prop_name, prop_value in properties.items():
            if prop_name in ["Record Reader", "Record Writer", "Kafka Connection Service"] and prop_value not in ["null", None]:
                    unique_controller_ids.add(prop_value)
    return sorted(list(unique_controller_ids))

def get_controller_service_revision(nifi_url, service_id, token, verify_ssl):
    """
    Fetches the current revision details for a specific Controller Service.
    This is necessary for subsequent PUT requests (like run-status changes).
    """
    url = f"{nifi_url}/nifi-api/controller-services/{service_id}"
    headers = {
        'Authorization': f'Bearer {token}'
    }

    print(f"Fetching current revision for Controller Service {service_id}...")
    try:
        get_response = requests.get(
            url,
            params={"uiOnly":"true"},
            headers=headers,
            verify=verify_ssl
        )
        get_response.raise_for_status()
        service_entity = get_response.json()
        return service_entity.get('revision')
    except requests.exceptions.HTTPError as e:
        print(f"Error fetching revision for {service_id}. Status: {e.response.status_code}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Network error fetching revision for {service_id}: {e}")
        raise

def update_controller_service_properties(nifi_url, service_id, token, verify_ssl, updates, allow_missing=False):
    url = f"{nifi_url}/nifi-api/controller-services/{service_id}"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    current = requests.get(url, headers=headers, verify=verify_ssl)
    current.raise_for_status()
    current_json = current.json()
    component = current_json.get('component', {})
    properties = component.get('properties', {}) or {}
    if not allow_missing and not any(key in properties for key in updates.keys()):
        return False
    properties.update(updates)
    payload = {
        'revision': current_json.get('revision'),
        'component': {
            'id': service_id,
            'properties': properties
        }
    }
    response = requests.put(url, headers=headers, json=payload, verify=verify_ssl)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        detail = response.text.strip()
        print(f"WARNING: Failed to update controller service {service_id}: {e}. {detail}")
        return False
    return True

def update_kafka_controller_service(nifi_url, service_id, token, verify_ssl, bootstrap_servers):
    url = f"{nifi_url}/nifi-api/controller-services/{service_id}"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    current = requests.get(url, headers=headers, verify=verify_ssl)
    current.raise_for_status()
    properties = current.json().get('component', {}).get('properties', {}) or {}
    if 'bootstrap.servers' not in properties:
        return False
    cs_revision = get_controller_service_revision(nifi_url, service_id, token, verify_ssl)
    stop_controller_service_run_status(nifi_url, service_id, cs_revision, token, verify_ssl)
    updated = update_controller_service_properties(
        nifi_url,
        service_id,
        token,
        verify_ssl,
        {'bootstrap.servers': bootstrap_servers}
    )
    cs_revision = get_controller_service_revision(nifi_url, service_id, token, verify_ssl)
    start_controller_service_run_status(nifi_url, service_id, cs_revision, token, verify_ssl)
    return updated

def list_controller_services_in_pg(nifi_url, token, verify_ssl, pg_id):
    url = f"{nifi_url}/nifi-api/flow/process-groups/{pg_id}/controller-services"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(url, headers=headers, verify=verify_ssl)
    response.raise_for_status()
    services = response.json().get('controllerServices', [])
    return [svc.get('id') for svc in services if svc.get('id')]

def list_controller_service_entities_in_pg(nifi_url, token, verify_ssl, pg_id):
    url = f"{nifi_url}/nifi-api/flow/process-groups/{pg_id}/controller-services"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(url, headers=headers, verify=verify_ssl)
    response.raise_for_status()
    return response.json().get('controllerServices', [])

def update_oauth2_controller_services(nifi_url, token, verify_ssl, pg_id, client_secret):
    if not client_secret:
        print("OPENSKY_CLIENT_SECRET not set; skipping OAuth2 controller service update.")
        return
    services = list_controller_service_entities_in_pg(nifi_url, token, verify_ssl, pg_id)
    for svc in services:
        component = svc.get('component', {})
        svc_type = component.get('type')
        svc_name = component.get('name')
        if svc_type == "org.apache.nifi.oauth2.StandardOauth2AccessTokenProvider" or svc_name == "StandardOauth2AccessTokenProvider":
            cs_revision = get_controller_service_revision(nifi_url, svc.get('id'), token, verify_ssl)
            stop_controller_service_run_status(nifi_url, svc.get('id'), cs_revision, token, verify_ssl)
            updated = update_controller_service_properties(
                nifi_url,
                svc.get('id'),
                token,
                verify_ssl,
                {"Client secret": client_secret, "Client Secret": client_secret},
                allow_missing=True
            )
            if updated:
                print(f"Updated OAuth2 client secret for controller service {svc.get('id')}")
            cs_revision = get_controller_service_revision(nifi_url, svc.get('id'), token, verify_ssl)
            start_controller_service_run_status(nifi_url, svc.get('id'), cs_revision, token, verify_ssl)

def normalize_process_groups(pg_lookup):
    if pg_lookup is None:
        return []
    if isinstance(pg_lookup, list):
        return [pg for pg in pg_lookup if pg is not None]
    return [pg_lookup]

def main():
    try:
        print("Configuring NiPyAPI endpoints...")
        # NiFi API endpoints used by nipyapi are the base API endpoints
        config.nifi_config.host = f"{NIFI_API_BASE_URL}/nifi-api"
        config.registry_config.host = F"{REGISTRY_API_URL.rstrip('/')}/nifi-registry-api"
        NIFI_CHECK_URL = f"{NIFI_API_BASE_URL}/nifi"
        REGISTRY_CHECK_URL = f"{REGISTRY_API_URL}/nifi-registry"

        if INSECURE:
            print("Disabling cert validation (INSECURE mode)")
            # security.service.set_service_certificate_validation(False)
            # --- Robustly disable certificate validation in nipyapi (dev/test only) ---
            def disable_cert_validation():
                """
                Attempt multiple ways to disable SSL/cert validation depending on nipyapi version.
                If none work, fall back to setting nipyapi.config.*.verify_ssl = False.
                """
                tried = []
                # 1) Try method accessed via nipyapi.security.service
                try:
                    sec_service = getattr(nipyapi.security, 'service', None)
                    if sec_service and hasattr(sec_service, 'set_service_certificate_validation'):
                        sec_service.set_service_certificate_validation(False)
                        print("[nipyapi] Disabled cert validation via security.service.set_service_certificate_validation")
                        return
                    tried.append("security.service.set_service_certificate_validation")
                except Exception as e:
                    tried.append(f"security.service - error: {e}")

                # 2) Try top-level nipyapi.security.set_service_certificate_validation
                try:
                    if hasattr(nipyapi.security, 'set_service_certificate_validation'):
                        nipyapi.security.set_service_certificate_validation(False)
                        print("[nipyapi] Disabled cert validation via security.set_service_certificate_validation")
                        return
                    tried.append("security.set_service_certificate_validation")
                except Exception as e:
                    tried.append(f"security - error: {e}")

                # 3) Try older/newer helper name (just in case)
                try:
                    if hasattr(nipyapi.security, 'set_service_ssl_verification'):
                        nipyapi.security.set_service_ssl_verification(False)
                        print("[nipyapi] Disabled cert validation via security.set_service_ssl_verification")
                        return
                    tried.append("security.set_service_ssl_verification")
                except Exception as e:
                    tried.append(f"security.set_service_ssl_verification - error: {e}")

                # 4) Fallback: set verify flags on nipyapi config objects directly
                try:
                    # These attributes exist on nipyapi.config objects in many versions
                    if hasattr(nipyapi.config, 'nifi_config'):
                        setattr(nipyapi.config.nifi_config, 'verify_ssl', False)
                    if hasattr(nipyapi.config, 'registry_config'):
                        setattr(nipyapi.config.registry_config, 'verify_ssl', False)
                    print("[nipyapi] Fallback: set nipyapi.config.*.verify_ssl = False")
                    return
                except Exception as e:
                    tried.append(f"config.verify_ssl - error: {e}")

                # If we get here, none of the attempts worked
                print("[nipyapi] WARNING: could not disable cert validation automatically.")
                print("Tried:", tried)
                print("You can inspect available nipyapi.security members with:")
                print("  python -c \"import nipyapi; import inspect; print(dir(nipyapi.security))\"")
                print("Or run an interactive shell inside the flow-deployer container to debug.")
                # Do not raise here; proceed (calls may still succeed if server certs are valid)

            # Call it
            disable_cert_validation()
            # --- end cert disable block ---

        # Wait for services
        print(f"Waiting for NiFi at {NIFI_CHECK_URL} ...")
        wait_for_url(NIFI_CHECK_URL, name='NiFi')
        print(f"Waiting for NiFi Registry at {REGISTRY_CHECK_URL} ...")
        wait_for_url(REGISTRY_CHECK_URL, name='NiFi Registry')

        security.service_login(
            service='nifi', 
            username=NIFI_USER,
            password=NIFI_PASS
        )
        # Mitigate occasional JWT "before use time" errors from clock skew.
        time.sleep(5)

        token = get_nifi_token(f"{NIFI_API_BASE_URL}/nifi-api", NIFI_USER, NIFI_PASS, cert_path=not INSECURE)
        print("Obtained NiFi token for upload.", token[:10] + "...")
        param_values = {
            'kafka.bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
        }

        for PG_NAME in PG_NAMES:
        # Check or create process group to import into
            print("========================================")
            print(f"Checking for existing Process Group named '{PG_NAME}'...")
            pg = canvas.get_process_group(PG_NAME, 'name')
            print("Process group lookup result:", end=' ')
            existing_pgs = normalize_process_groups(pg)
            kept_pgs = []
            if existing_pgs:
                print(existing_pgs[0])
                for p in existing_pgs:
                    print(f" {p.id}", end=',')
                print()
            else:
                print("None")

            for existing_pg in existing_pgs:
                component = existing_pg.component
                if component.parameter_context:
                    print(f"{component.parameter_context}")
                else:
                    print("No parameter context")
                print(f"Replacing existing process group {existing_pg.id} ({component.name})")
                try:
                    canvas.schedule_process_group(existing_pg.id, False)
                except Exception as e:
                    print(f"WARNING: Failed to stop process group {existing_pg.id}: {e}")
                try:
                    delete_process_group(NIFI_API_BASE_URL, token, not INSECURE, existing_pg.id)
                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code == 409:
                        print(f"Conflict deleting process group {existing_pg.id}; dropping queues and retrying.")
                        for connection_id in list_connections_in_pg(NIFI_API_BASE_URL, token, not INSECURE, existing_pg.id):
                            drop_connection_queue(NIFI_API_BASE_URL, token, not INSECURE, connection_id)
                        try:
                            delete_process_group(NIFI_API_BASE_URL, token, not INSECURE, existing_pg.id)
                        except requests.exceptions.HTTPError as retry_error:
                            print(f"WARNING: Unable to delete process group {existing_pg.id} after queue drop: {retry_error}. Keeping existing group.")
                            kept_pgs.append(existing_pg)
                            continue
                    else:
                        raise

            if kept_pgs:
                new_pg = kept_pgs[0]
                use_param_context = True
                try:
                    param_context_id = upsert_parameter_context(
                        NIFI_API_BASE_URL,
                        token,
                        not INSECURE,
                        PARAM_CONTEXT_NAME,
                        param_values
                    )
                    assign_parameter_context(NIFI_API_BASE_URL, token, not INSECURE, new_pg.id, param_context_id)
                except requests.exceptions.HTTPError as e:
                    print(f"WARNING: Parameter context update failed ({e}). Falling back to controller service update.")
                    use_param_context = False
                if not use_param_context:
                    for cs in list_controller_services_in_pg(NIFI_API_BASE_URL, token, not INSECURE, new_pg.id):
                        updated = update_kafka_controller_service(
                            NIFI_API_BASE_URL,
                            cs,
                            token,
                            not INSECURE,
                            KAFKA_BOOTSTRAP_SERVERS
                        )
                        if updated:
                            print(f"Updated bootstrap.servers for controller service {cs}")
            else:
                root_pg_id = canvas.get_root_pg_id()
                print("Root PG id:", root_pg_id)

                # Load flow content from file if exists
                flow_file = Path(FLOW_PATH_HOST)
                flow_content_dict = None
                if flow_file.exists():
                    print(f"Found existing flow content file at {flow_file}. Using it for import.")
                    with open(flow_file, 'r') as f:
                        flow_content_dict = json.load(f)
                else:
                    raise RuntimeError(f"Flow content file not found at {flow_file}")

                # Create or get a registry bucket
                BUCKET_NAME = "auto-deploy-bucket"
                print("Ensuring Registry bucket exists:", BUCKET_NAME)
                bucket_api = registry.apis.buckets_api.BucketsApi()
                buckets = bucket_api.get_buckets()
                bucket = None
                if buckets:
                    for b in buckets:
                        if b.name == BUCKET_NAME:
                            bucket = b
                            break

                if bucket is None:
                    print("Creating registry bucket...")
                    new_bucket = registry.models.Bucket(name=BUCKET_NAME)
                    bucket = bucket_api.create_bucket(new_bucket)
                    print("Created bucket:", bucket.identifier, bucket.name)
                else:
                    print("Found bucket:", bucket.identifier, bucket.name)

                registry_client = versioning.get_registry_client('my-registry-client', 'name')
                if registry_client is None:
                    registry_client = versioning.create_registry_client(
                        name='my-registry-client',
                        uri=config.registry_config.host,
                        description='Client for my flows'
                    )

                # Import the flow JSON into the bucket (using flow content JSON)
                FLOW_NAME = PG_NAME # Name for the versioned flow in the Registry

                # --- FLOW EXISTENCE CHECK & VERSIONING LOGIC (IDEMPOTENT) ---
                # Check if flow container already exists in bucket
                flow_list = registry.apis.bucket_flows_api.BucketFlowsApi().get_flows(bucket.identifier)
                imported = None
                for flow in flow_list:
                    if flow.name == FLOW_NAME:
                        imported = flow
                        break


                if imported is None:
                    # Case 1: Flow container does not exist. Create container and version 1.
                    print(f"Flow '{FLOW_NAME}' not found. Creating new flow container and version 1.")
                    
                    flow_body = registry.models.VersionedFlow(
                        bucket_identifier=bucket.identifier,
                        bucket_name=bucket.name,
                        name=FLOW_NAME,
                        description='Initial import of flow content from NiFi root canvas.',
                        type='Flow'
                    )
                    imported = registry.apis.bucket_flows_api.BucketFlowsApi().create_flow(
                        body=flow_body,
                        bucket_id=bucket.identifier
                    )
                    print("Imported flow container. Registry flow id:", imported.identifier)
                    
                    # Explicitly Save Version 1
                    versioned_flow_snapshot = registry.models.VersionedFlowSnapshot(
                        flow_contents=flow_content_dict,
                        snapshot_metadata=registry.models.VersionedFlowSnapshotMetadata(
                            author="ankhanhtran02",
                            bucket_identifier=bucket.identifier,
                            flow_identifier=imported.identifier,
                        )
                    )

                    versioned_flow_snapshot_json = utils.dump(versioned_flow_snapshot)
                        
                    saved_version = versioning.import_flow_version(
                        bucket_id=bucket.identifier,
                        flow_id=imported.identifier,
                        encoded_flow=versioned_flow_snapshot_json,
                    )
                    version_number = saved_version.snapshot_metadata.version
                    print(f"Saved version {version_number} successfully.")

                else:
                    # Case 2: Flow container exists. Check for change.
                    print(f"Flow '{FLOW_NAME}' found. Checking if new version is required.")
                    

                    versions = registry.apis.flows_api.FlowsApi().get_flow_versions1(imported.identifier)
                    
                    if not versions:
                        # Fallback in case of a corrupted flow container (no versions)
                        print("WARNING: Flow found but has no versions. Saving version 1.")
                        versioned_flow_snapshot = registry.models.VersionedFlowSnapshot(
                            flow_contents=flow_content_dict,
                            snapshot_metadata=registry.models.VersionedFlowSnapshotMetadata(
                                author="ankhanhtran02",
                                bucket_identifier=bucket.identifier,
                                flow_identifier=imported.identifier,
                            )
                        )

                        versioned_flow_snapshot_json = utils.dump(versioned_flow_snapshot)
                        saved_version = versioning.import_flow_version(
                            bucket_id=bucket.identifier,
                            flow_id=imported.identifier,
                            encoded_flow=versioned_flow_snapshot_json,
                        )
                        version_number = saved_version.snapshot_metadata.version
                        print(f"Saved version {version_number} successfully.")

                # print(versioned_flow_snapshot_json)
                pg = upload_flow_via_api(
                    nifi_url=NIFI_API_BASE_URL,
                    parent_pg_id=root_pg_id,
                    client_id=str(uuid.uuid4()),
                    file_path=FLOW_PATH_HOST,
                    group_name=PG_NAME,
                    token=token,
                    verify_ssl=not INSECURE
                )
                use_param_context = True
                try:
                    param_context_id = upsert_parameter_context(
                        NIFI_API_BASE_URL,
                        token,
                        not INSECURE,
                        PARAM_CONTEXT_NAME,
                        param_values
                    )
                    assign_parameter_context(NIFI_API_BASE_URL, token, not INSECURE, pg["id"], param_context_id)
                except requests.exceptions.HTTPError as e:
                    print(f"WARNING: Parameter context update failed ({e}). Falling back to controller service update.")
                    use_param_context = False
                controller_service_ids = extract_controller_service_ids(pg)
                print("Found controller services: ", controller_service_ids)
                for cs in controller_service_ids:
                    if not use_param_context:
                        updated = update_kafka_controller_service(
                            NIFI_API_BASE_URL,
                            cs,
                            token,
                            not INSECURE,
                            KAFKA_BOOTSTRAP_SERVERS
                        )
                        if updated:
                            print(f"Updated bootstrap.servers for controller service {cs}")
                    cs_revision = get_controller_service_revision(NIFI_API_BASE_URL, cs, token, not INSECURE)
                    start_controller_service_run_status(NIFI_API_BASE_URL, cs, cs_revision, token, not INSECURE)

                # Attempt to locate the created process group and schedule it
                time.sleep(2)
                # find process groups with our name under root
                new_pg = canvas.get_process_group(pg["id"], 'id', False)
                if new_pg is None:
                    raise RuntimeError("Failed to find the instantiated process group after deployment.")
                else:
                    print("Found instantiated process group:", new_pg.id, new_pg.component.name)
                
            update_oauth2_controller_services(NIFI_API_BASE_URL, token, not INSECURE, new_pg.id, OPENSKY_CLIENT_SECRET)
            print("Scheduling (starting) process group:", new_pg.id)
            canvas.schedule_process_group(new_pg.id, True)
            print("Process group scheduled. Done.")

    except Exception as e:
        print("ERROR during deployment:", str(e))
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
