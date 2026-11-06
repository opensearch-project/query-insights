#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Reproduce #510/#673 with real 3.4/3.8 nodes, then verify a patched plugin.

Build first:
  ./gradlew bundlePlugin -Dopensearch.version=3.8.0 -Dbuild.snapshot=false
Run:
  python3 scripts/mixed_version_attribute_test.py --plugin build/distributions/query-insights-3.8.0.0.zip

Requires Docker and Python 3 (standard library only). Creates isolated disposable
clusters, binds HTTP to loopback on random ports, and preserves evidence under
build/reports/mixed-version-attributes. Existing containers are never modified.
"""

import argparse
import json
import re
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import uuid


def docker(*args, check=True):
    result = subprocess.run(["docker", *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
    if check and result.returncode:
        raise RuntimeError(f"docker {' '.join(args)}: {result.stdout}")
    return result.stdout


def request(endpoint, method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(endpoint + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as response:
        return json.load(response)


def wait_for(description, operation, timeout=180):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = operation()
            if value:
                return value
        except (OSError, ValueError) as error:
            last_error = error
        time.sleep(2)
    raise AssertionError(f"Timed out waiting for {description}: {last_error}")


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def run_phase(output, plugin):
    phase = "patched" if plugin else "baseline"
    evidence = output / phase
    evidence.mkdir(parents=True, exist_ok=False)
    prefix = f"qi-bwc-{uuid.uuid4().hex[:10]}"
    containers = []
    endpoints = {}
    network_created = False
    print(f"[{phase}] Starting isolated 3.4.0 + 3.8.0 cluster", flush=True)
    try:
        docker("network", "create", prefix)
        network_created = True
        for role, version in (("old", "3.4.0"), ("new", "3.8.0")):
            name = f"{prefix}-{role}"
            args = ["create", "--name", name, "--network", prefix, "--network-alias", role,
                    "--publish", "127.0.0.1::9200", "--memory", "2g",
                    "--env", "DISABLE_INSTALL_DEMO_CONFIG=true", "--env", "DISABLE_SECURITY_PLUGIN=true",
                    "--env", "DISABLE_PERFORMANCE_ANALYZER_AGENT_CLI=true",
                    "--env", "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m",
                    "--env", f"cluster.name={prefix}", "--env", f"node.name={role}",
                    "--env", "node.roles=cluster_manager,data,ingest" if role == "old" else "node.roles=data,ingest",
                    "--env", "discovery.seed_hosts=old,new", "--env", "cluster.initial_cluster_manager_nodes=old",
                    "--env", "node.store.allow_mmap=false",
                    "--env", "search.insights.top_queries.latency.enabled=false",
                    "--env", "search.insights.top_queries.cpu.enabled=false",
                    "--env", "search.insights.top_queries.memory.enabled=false"]
            if role == "new" and plugin:
                args += ["--entrypoint", "/bin/bash"]
            args += [f"opensearchproject/opensearch:{version}"]
            if role == "new" and plugin:
                args += ["-c", "bin/opensearch-plugin remove query-insights && "
                         "bin/opensearch-plugin install --batch file:///tmp/query-insights.zip && "
                         "exec ./opensearch-docker-entrypoint.sh opensearch"]
            docker(*args)
            containers.append(name)
            if role == "new" and plugin:
                docker("cp", str(plugin), f"{name}:/tmp/query-insights.zip")
            docker("start", name)
            port = docker("port", name, "9200/tcp").strip()
            endpoints[role] = "http://" + port

        old, new = endpoints["old"], endpoints["new"]
        for role in ("old", "new"):
            info = wait_for(f"{role} HTTP", lambda: request(endpoints[role], "GET", "/"))
            assert info["version"]["number"] == ("3.4.0" if role == "old" else "3.8.0"), info
            save(evidence / f"{role}-info.json", info)

        nodes = wait_for("both nodes joining", lambda: two_nodes(old))
        save(evidence / "nodes.json", nodes)
        new_id = next(node_id for node_id, node in nodes["nodes"].items() if node["name"] == "new")
        save(evidence / "plugins.json", request(old, "GET", "/_cat/plugins?format=json"))
        request(old, "PUT", "/_cluster/settings", {"persistent": {
            "search.insights.top_queries.latency.enabled": True,
            "search.insights.top_queries.latency.top_n_size": 100,
            "search.insights.top_queries.latency.window_size": "60m",
            "search.insights.top_queries.exporter.type": "none",
            "search.insights.top_queries.excluded_indices": ["^(?!attribute-bwc-probe$).*"],
        }})
        index = "attribute-bwc-probe"
        request(old, "PUT", "/" + index, {"settings": {
            "number_of_shards": 1, "number_of_replicas": 0,
            "index.routing.allocation.require._name": "new",
        }})
        wait_for("probe index allocation", lambda: request(old, "GET", f"/_cluster/health/{index}")["status"] == "green")
        request(new, "PUT", f"/{index}/_doc/1?refresh=true", {"marker": "attribute-bwc-probe"})
        # Poll by searching until the new node's asynchronous collector exposes our record.
        def collect_query():
            # A query-free source avoids an unrelated legacy SOURCE conversion issue
            # with named query parsers; the dedicated index identifies our probe.
            request(new, "POST", f"/{index}/_search", {"size": 1})
            response = request(new, "GET", "/_insights/top_queries?type=latency&verbose=true")
            records = probe_records(response, new_id, index)
            return (response, records) if records else None

        new_response, new_records = wait_for("new-node query collection", collect_query)
        save(evidence / "new-coordinator-response.json", new_response)
        assert all("source_truncated" in record for record in new_records), new_records
        print(f"[{phase}] New node collected {len(new_records)} probe records with SOURCE_TRUNCATED", flush=True)

        old_response = request(old, "GET", "/_insights/top_queries?type=latency&verbose=true")
        save(evidence / "old-coordinator-response.json", old_response)
        old_records = probe_records(old_response, new_id, index)
        old_logs = docker("logs", containers[0])
        if not plugin:
            assert not old_records, "Baseline unexpectedly returned the new node's query records"
            unknown = re.search(r"No enum constant org\.opensearch\.plugin\.insights\.rules\.model\.Attribute\.(SOURCE_TRUNCATED|FAILED)", old_logs)
            assert unknown, old_logs[-6000:]
            print(f"[baseline] Confirmed: old coordinator rejects {unknown.group(1)} and loses the new node's records", flush=True)
        else:
            assert old_records, "HTTP succeeded but the new node's records were lost"
            by_id = {record["id"]: record for record in old_records}
            for expected in new_records:
                assert expected["id"] in by_id, f"Missing query {expected['id']}"
                actual = by_id[expected["id"]]
                for field in ("id", "timestamp", "node_id", "indices", "source", "total_shards", "measurements"):
                    assert field in expected and field in actual, f"Missing known field {field}"
                    actual_value, expected_value = actual[field], expected[field]
                    if field == "source":
                        # 3.4 renders SOURCE as an object; 3.8 renders it as a JSON string.
                        actual_value = json.loads(actual_value) if isinstance(actual_value, str) else actual_value
                        expected_value = json.loads(expected_value) if isinstance(expected_value, str) else expected_value
                    assert actual_value == expected_value, f"Changed field {field}: {actual_value} != {expected_value}"
                assert not {"source_truncated", "failed", "username", "user_roles", "backend_roles"}.intersection(actual)
            assert "Failed to deserialize response" not in old_logs, old_logs[-6000:]
            assert "No enum constant org.opensearch.plugin.insights.rules.model.Attribute" not in old_logs, old_logs[-6000:]
            print(f"[patched] PASS: old coordinator preserved {len(new_records)} new-node records and known fields", flush=True)

        summary = {"phase": phase, "old_version": "3.4.0", "new_version": "3.8.0",
                   "new_node_id": new_id, "new_record_ids": [record["id"] for record in new_records],
                   "old_record_ids": [record["id"] for record in old_records], "passed": True}
        save(evidence / "result.json", summary)
        return summary
    finally:
        for name in containers:
            (evidence / f"{name.rsplit('-', 1)[-1]}.log").write_text(docker("logs", name, check=False))
            # Remove only this run's explicitly tracked disposable containers and anonymous volumes.
            docker("rm", "--force", "--volumes", name, check=False)
        if network_created:
            docker("network", "rm", prefix, check=False)


def two_nodes(endpoint):
    nodes = request(endpoint, "GET", "/_nodes")
    return nodes if len(nodes["nodes"]) == 2 else None


def probe_records(response, node_id, index):
    return [record for record in response.get("top_queries", [])
            if record.get("node_id") == node_id and index in record.get("indices", [])]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--plugin", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path("build/reports/mixed-version-attributes") / time.strftime("%Y%m%d-%H%M%S"))
    args = parser.parse_args()
    plugin = args.plugin.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=False)
    summaries = [run_phase(args.output, None), run_phase(args.output, plugin)]
    save(args.output / "result.json", summaries)
    print(f"Evidence: {args.output.resolve()}", flush=True)


if __name__ == "__main__":
    main()
