#!/usr/bin/env python3

"""Distributed Elasticsearch Query Benchmarker
Tested with Python 3.10.12

## High Level Structure:
- Arrange
  - Parse arguments 
    - see `--help`
  - load json-based query description 
    - see input format below and examples in repo
  - Let every rank calculate its elasticsearch server to request against:
    - see description of `select_es_server`
- Act
  - For each disjunct benchmark step:
    - root waits until the cluster health is green
      - Rests waits with barrier
      - Docs: <https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html>
    - Create Random permutation of all queries for this step in each rank
    - In order to create more load variety between the load generators
    - If warmup run time > 0 sec
      - Until the warmup time is done
        - send the next query, throw away the result
        - If configured, sleep between requests
    - Until the execution time is done
      - choose the next request
      - track time right before the request
      - do the request
        - seach API endpoint, Cache explicitly disabled
          - <https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-request-cache.html#_enabling_and_disabling_caching_per_request>
      - track time right after the receiving
        - even before the processing to keep our time overhead as low as possible and to maximize our likelyhood 
          of it being more atomic-y
      - If successful HTTP response code
        - count the number of returned documents
        - save the (latency, docs) for the current query and rank
      - else:
          save the http code for current query and rank
      - If configured, sleep between requests
- Assert
  - Merge results of all ranks using MPI gather and some data transformation
    - See output format below
  - Root dumps it into JSON specified via CLI parameter

## Test Mode:
We also have a test mode for debugging the query configurations when run with `--test-mode`.
It only runs on root rank, and just requests each query once and pretty prints both the query and its result.
It is mainly used to see that the output is actually what is expected so that one doesn't accidentally benchmark 
invalid and/or broken queries

## Input Format:
(The comments are just for documentations sake and not part of the json)
```json
[
  {
    "search_queries": [
      {
        /* everything in here just gets sent to ES */
        "body": {
          /* The raw ES query sent to the server */
        }
      }
    ],
    "warmup_time_secs": 30, /* optional */
    "execution_time_secs": 120, /* optional */
  },
  {
    "search_queries": [
      {
        "body": {
          /* The first of two queries sent iteratively (random permutation) */
        }
      },
      {
        "body": {
          /* The second of two queries sent iteratively (random permutation) */
        }
      }
    ],
    "warmup_time_secs": 30, /* optional */
    "execution_time_secs": 180, /* optional */
    "sleep_between_requests_secs": 0.25 /* optional */
  }
]
```

## Output Format:
(The comments are just for documentations sake and not part of the json)
```json
[
  {
    "query_result": [
      {
        "body": {
          /* The inside of the first "query" in the input */
        },
        "responses": [ 
          [ 
            /* load generator 1 */
            {"latency": 0.35, "docs": 2500},
            {"latency": 0.32, "docs": 2500},
            {"error_code": 501}
            {"latency": 0.33, "docs": 2500},
          ],
          [ 
            /* load generator 2 */
            {"latency": 0.35, "docs": 2500},
            {"latency": 0.32, "docs": 2500},
            {"error_code": 501}
            {"latency": 0.33, "docs": 2500},
          ]
        ]
      },
      {
        "body": {
          /* The inside of the second "query" in the input */
        },
        "responses": [ 
          [ 
            /* load generator 1 */
            {"latency": 0.35, "docs": 2500},
            {"latency": 0.32, "docs": 2500},
            {"error_code": 501}
            {"latency": 0.33, "docs": 2500},
          ],
          [ 
            /* load generator 2 */
            {"latency": 0.35, "docs": 2500},
            {"latency": 0.32, "docs": 2500},
            {"error_code": "501"}
            {"latency": 0.33, "docs": 2500},
          ]
        ]
      }
    ]
    /* same as input */
    "warmup_time_secs": 30,
    "execution_time_secs": 120,
    "sleep_between_requests_secs": 0.25
  },
  {
    /* ... */
  }
]
```
"""

import argparse
import itertools
import json
import random
import os
import time

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Union

import requests

from mpi4py import MPI

COMM = MPI.COMM_WORLD

@dataclass
class BenchmarkStep:
    """Dataclass to represent each benchmark step.

    Attributes:
        search_queries (List[Dict]): A list of dictionaries representing search queries.
        warmup_time_secs (Optional[int]): Optional warm-up time in seconds.
        execution_time_secs (Optional[int]): Optional execution time in seconds.
        sleep_between_requests_secs (Optional[float]): Optional sleep time between requests in seconds.
    """
    search_queries: List[Dict]
    warmup_time_secs: Optional[int] = None
    execution_time_secs: Optional[int] = None
    sleep_between_requests_secs: Optional[float] = None

@dataclass
class Success:
    """Metadata about a successful response from elasticsearch.

    So when we get a lower bound, we just choose that as a value, since elasticsearch
    also just lazily computed until this bound.

    Attributes:
        latency (float): The time from sending the request to receiving the response, 
                         without the overhead of generating or parsing the reults.
        docs (int): The document count **we got**. Useful for error checking and computing docs/sec.
    """
    latency: float
    docs: int

@dataclass
class Failure:
    """Metadata about an unsuccessful response from elasticsearch.

    Attributes:
        error_code (int): The HTTP error code returned from elasticsearch.
            If -1, the request timed out.
    """
    error_code: int

@dataclass
class QueryResult:
    """The result of a single query type, part of a `BenchmarkStepResult`.

    See the description of `BenchmarkStepResult` and the high level description for more information.

    Attributes:
        query (Dict): The query as it was sent to elasticsearch after being converted to JSON
        responses (List[Union[Success, Failure]]): Information about each response to this type of query.
                The outer list represents each load generator. 
                The inner list represents metadata about each response the n-th generator got
    """
    body: Dict
    responses: List[List[Union[Success, Failure]]]

@dataclass
class BenchmarkStepResult:
    """The result of a BenchmarkStep.

    Note that all QueryResults in a BenchmarkStep are executed alternatingly, thus the deltas do not
    imply that two deltas next to each other were recorded next to each other.
    Furthermore, since the benchmark is executed by many different load generators, it is to be expected
    that if multiple Queries exist that all are probably processed concurrently by Elasticsearch/Lucene.

    Attributes:
        search_results (List[QueryResult]): A list of QueryResults, containing the 
        warmup_time_secs (Optional[int]): Optional warm-up time in seconds.
        execution_time_secs (Optional[int]): Optional execution time in seconds.
        sleep_between_requests_secs (Optional[float]): Optional sleep time between requests in seconds.
    """
    search_results: List[QueryResult]
    warmup_time_secs: Optional[int] = None
    execution_time_secs: Optional[int] = None
    sleep_between_requests_secs: Optional[float] = None


def is_root() -> bool:
    """
    Check if the current MPI process is the root process.

    Returns:
        bool: True if the current MPI process rank is 0 (root), else False.
    """
    return COMM.Get_rank() == 0

def parse_arguments() -> argparse.Namespace:
    """
    Parse and validate command line arguments.

    Returns:
        argparse.Namespace: An object containing attributes for each command line argument.
        Includes:
        - json_file: Path to the JSON file with query configurations.
        - hosts: Comma-separated list of Elasticsearch hosts.
        - indexname: Name of the Elasticsearch index to query.
        - output: Filepath for saving output results.
        - warmup: Default warmup time in seconds.
        - execution: Default execution time in seconds.
        - test-mode: If set, runs the script in test mode for debugging.
    """
    parser = argparse.ArgumentParser(description="Elasticsearch Query Benchmarker.")

    # Positional argument for the JSON file path
    parser.add_argument(
        "json_file",
        type=str,
        help="Path to the JSON file containing the query configurations."
    )

    # Same parameters as in the ingest benchmarker
    parser.add_argument(
        "--hosts",
        type=str,
        required=True,
        help="Comma separated hosts. Example: \"--hosts host1:9200,10.0.0.2:9200,localhost:9200\""
    )
    parser.add_argument(
        "--indexname",
        type=str,
        default="benchmark_index",
        help="Name of the Elasticsearch index to query."
    )

    # New parameters specific to the query benchmarker
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Filepath for the output results."
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=60,
        help="Default warmup time in seconds (defaults to 60)."
    )
    parser.add_argument(
        "--execution",
        type=int,
        default=300,
        help="Default execution time in seconds (defaults to 300)."
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="How long to wait until the next request (defaults to 0.0)."
    )
    parser.add_argument(
        "--testmode",
        action="store_true",
        help="Run the script in test mode. Executes only on root, once per query."
    )
    parser.add_argument(
        "--allowbad",
        action="store_true",
        help="Allow yellow and red clusters (useful for specific debugging)"
    )

    args = parser.parse_args()

    # Validation checks
    # Check if input and output file paths are valid
    if not os.path.isfile(args.json_file):
        parser.error(f"JSON file path \"{args.json_file}\" is not a valid file!")
    if os.path.exists(args.output):
        parser.error(f"Output file path \"{args.output}\" already exists!")

    # Check if the files are readable
    if not os.access(args.json_file, os.R_OK):
        parser.error(f"JSON file path \"{args.json_file}\" is not readable!")

    # Hosts validation (similar to the ingest benchmarker)
    all_hosts = [x.strip() for x in args.hosts.split(",") if x.strip() != ""]
    for host in all_hosts:
        colon_index = host.rfind(":")
        if colon_index == -1:
            parser.error(f"Host \"{host}\" does not contain a port!")
        port = host[colon_index+1:]
        if not port.isdigit() or int(port) > 65535:
            parser.error(f"Host \"{host}\" does not contain a valid port!")

    return args

def load_json_queries(json_file_path) -> List[BenchmarkStep]:
    """
    Load and parse a JSON file into a list of BenchmarkStep dataclasses.

    Args:
        json_file_path (str): Path to the JSON file to be parsed.

    Returns:
        List[BenchmarkStep]: A list of BenchmarkStep instances parsed from the JSON file.
    """
    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing JSON file: {e}")
    except Exception as e:
        raise ValueError(f"Error reading file: {e}")

    benchmark_steps = []
    for item in data:
        benchmark_step = BenchmarkStep(
            search_queries=item.get('search_queries', []),
            warmup_time_secs=item.get('warmup_time_secs'),
            execution_time_secs=item.get('execution_time_secs'),
            sleep_between_requests_secs=item.get('sleep_between_requests_secs')
        )
        benchmark_steps.append(benchmark_step)

    return benchmark_steps

def select_es_server(hosts: str) -> str:
    """
    We have to distribute the requests between all ES cluster nodes...
    I'd expect people to understand this is somehow a problem (as mentioned in the README).
    Thus, we assume that the MPI World Size is a multiple of the number of ES cluster nodes.
    With that in mind, we do the following load distribution (assuming 3 ES nodes)
    
    rank 0 requests against ES 0
    rank 1 requests against ES 1
    rank 2 requests against ES 2
    rank 3 requests against ES 0
    rank 4 requests against ES 1
    rank 5 requests against ES 2
    rank 6 requests against ES 0
    ...

    Args:
        hosts (str): A comma seperated list of "<SOMETHING_RESOLVABLE>:<PORT>". Example: "1.1.1.1:9200,node3.local:9200"

    Returns:
        str: The selected host found as described above
    """
    all_hosts = [x.strip() for x in hosts.split(",") if x.strip() != ""]
    return all_hosts[COMM.Get_rank() % len(all_hosts)]

def wait_for_green_cluster(es_server: str, wait: int = 10):
    """
    Wait for the Elasticsearch cluster to reach the 'green' health status.

    This function repeatedly queries the Elasticsearch cluster health API until the cluster health
    status is 'green'. If the cluster does not reach 'green' status within the specified timeout,
    it continues waiting.

    Args:
        es_server (str): The address of the Elasticsearch server.
        timeout (str): Timeout duration for waiting for the 'green' status. The default is '10s'.
    """
    health_endpoint = f"http://{es_server}/_cluster/health?wait_for_status=green"
    
    # we really make sure not to spam too much
    if not is_root():
        return

    while True:
        try:
            response = requests.get(health_endpoint)
            response.raise_for_status()

            health_data = response.json()
            if health_data.get("status") == "green":
                print("Cluster health is green.")
                break
            else:
                print(f"Waiting for cluster to turn green. Current status: {health_data.get('status')}")

        except requests.RequestException as e:
            print(f"Error checking cluster health: {e}. Trying again...")

        time.sleep(wait)

def get_docs_from_response(response: requests.Response) -> int:
    """
    Returns the number of documetns within a elasticsearch response.

    Note that we do count the number of docs **we got**, not the number/lower bound elasticsearch tells us
    at `["hits"]["total"]["value"]`.

    See: <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html#search-api-response-body>

    Args:
        response (requests.Response): The response to the previously sent search API request

    Returns:
        int: Total number of hits from the elasticsearch query.
    """
    assert 200 <= response.status_code < 300
    result = response.json()
    total_hits = len(result['hits']['hits'])
    return total_hits

def run_step(step: BenchmarkStep, es_server: str, indexname: str, default_warmup_secs: int, default_execution_secs: int, default_sleep_secs: float) -> BenchmarkStepResult:
    """
    Execute a benchmark step against an Elasticsearch server.

    Args:
        step (BenchmarkStep): The benchmark step to execute.
        es_server (str): The Elasticsearch server address.
        indexname (str): The name of the Elasticsearch index to query.
        default_warmup_secs (int): Default warmup time in seconds if not specified in the step.
        default_execution_secs (int): Default execution time in seconds if not specified in the step.

    Returns:
        BenchmarkStepResult: The result based on the responses that single rank got.
    """
    # Prepraration
    # Permutate so that we have different load from different load generators if multiple queries exist
    query_permutation = random.sample(step.search_queries, len(step.search_queries))
    warmup_time_secs = step.warmup_time_secs if step.warmup_time_secs is not None else default_warmup_secs
    execution_time_secs = step.execution_time_secs if step.execution_time_secs is not None else default_execution_secs
    sleep_between_requests_secs = step.sleep_between_requests_secs if step.sleep_between_requests_secs is not None else default_sleep_secs
    server_endpoint = f"http://{es_server}/{indexname}/_search?request_cache=false"
    
    # warmup runs
    if warmup_time_secs > 0:
        start_time = time.time()
        for query in itertools.cycle(query_permutation):
            requests.post(server_endpoint, json=query)
            if time.time() - start_time >= warmup_time_secs:
                break
            if sleep_between_requests_secs > 0:
                time.sleep(sleep_between_requests_secs)

    # Create empty result structure
    query_results = [QueryResult(body=q, responses=[[]]) for q in query_permutation]


    # timed execution runs
    start_time = time.time()
    for idx, query in enumerate(itertools.cycle(query_permutation)):
        before = time.time()
        response = requests.post(server_endpoint, json=query["body"])
        after = time.time()

        if 200 <= response.status_code < 300:
            response = Success(latency=after-before, docs=get_docs_from_response(response))
        else:
            response = Failure(error_code=response.status_code)
        # Fill in for our current load generator (merging later)
        query_results[idx % len(query_permutation)].responses[0].append(response)
        
        if time.time() - start_time >= execution_time_secs:
            break

        if sleep_between_requests_secs > 0:
            time.sleep(sleep_between_requests_secs)

    benchmark_step_result = BenchmarkStepResult(
        search_results=query_results,
        warmup_time_secs=warmup_time_secs,
        execution_time_secs=execution_time_secs,
        sleep_between_requests_secs=sleep_between_requests_secs
    )

    return benchmark_step_result

def merge_step_results_mpi(all_step_results: List[BenchmarkStepResult]) -> List[BenchmarkStepResult]:
    """Merges the steps from all ranks to one unified one containing the results of all ranks combined.

    In the `run_step` function, each rank generates its own `BenchmarkStepResult` based on the responses
    this single rank got. In particular, for each query, the BenchmarkStepResult.search_results[i].body
    only contains a single list of responses, that of the current rank.

    Thus, each rank creates a List[BenchmarkStepResult], resulting in a global List[List[BenchmarkStepResult]].
    This function tries to flatten it into a List[BenchmarkStepResult].

    First, note the following:
      - While the query order within a single step is randomly permutated (see `run_step`), the step order stays
        the same between all ranks since this benchmark generally follows the fork-join model.
      - Each benchmarkstep has the same warmup time, execution time, and sleep time between requests between all
        ranks.
      - Thus, for each query within a step, only the responses have to be merged.

    We want to have the following structure in the end:
    ```
    [
      /* This is the list of all non-overlapping benchmark steps */
      {
        "query_result": [
          /* this is the list of all queries run alternatingly within a single step */
          {
            "body": {
              /* The inside of the first "query" for the current step */
            },
            "responses": [
              [ 
                /* load generator 1 */
                {"latency": 0.35, "docs": 2500},
                {"latency": 0.32, "docs": 2500},
                {"error_code": 501}
                {"latency": 0.33, "docs": 2500},
              ],
              [ 
                /* load generator 2 */
                {"latency": 0.35, "docs": 2500},
                {"latency": 0.32, "docs": 2500},
                {"error_code": 501}
                {"latency": 0.33, "docs": 2500},
              ],
              [...]
            ],
          }
          {
            /* Second query... */
          }
        ]
        /* The parameter that stay the same for all queries within a step */
        "warmup_time_secs": 30,
        "execution_time_secs": 120,
        "sleep_between_requests_secs": 0.25
      },
      {
        /* Second step, runs after the first one finished */
      }
    ]
    ```

    The function archives this as follows:
    1. Gather all results at root. non root ranks can exit afterwards.
    2. For each step:
      - Create a lookup table (dictionary) for all queries in that step
      - create a list with the responses of each rank as the items to each query
      - create new query objects based on this lookup table
      - create a BenchmarkStepResult based on those query objects
    3. Return the list of all merged steps

    Args:
        all_step_results (List[BenchmarkStepResult]) The results of all steps **from the current rank**, which get merged into the root rank using MPI.

    Returns:
        List[BenchmarkStepResult]: The results of all steps **from all ranks** merged.
    """
    gathered_results: List[List[BenchmarkStepResult]] = COMM.gather(all_step_results, root=0)

    if not is_root():
        return []

    merged_steps = []
    
    # Note that, although the query order gets randomly permutated in the `run_step` function, the Steps itself
    # obviously have the same order (since the whole benchmarker follows a fork-join model)
    # So we can use any List[BenchmarkStepResult] for iterating over the steps
    all_steps_single_node: List[BenchmarkStepResult] = gathered_results[0]

    for i in range(len(all_steps_single_node)):
        current_step: BenchmarkStepResult = all_steps_single_node[i]

        # We use a dict so that we do not have to search
        # We need that since, in order to have a more variadic load, each rank randomly permutates the queries **within a single step**.
        all_queries_current_step: Dict[str, List[List[Union[Success, Failure]]]] = {json.dumps(result.body): [] for result in current_step.search_results}

        # Now, we have to accumulate this step from all ranks
        for rank in gathered_results:
            i_th_step: BenchmarkStepResult = rank[i]

            # Now the merging part
            for query_result in i_th_step.search_results:
                # Since we had it locally, the `responses` list had just one element, the one from that local rank
                responses: List[Union[Success, Failure]] = query_result.responses[0]
                all_queries_current_step[json.dumps(query_result.body)].append(responses)

        # Transform them back into the old structure
        search_results: List[QueryResult] = [
            QueryResult(
                body=json.loads(query_json),
                responses=all_responses
            )
            for query_json, all_responses in all_queries_current_step.items()
        ]

        # The rest stays the same since it is of the same step in our fork-join model.
        merged_step = BenchmarkStepResult(
            search_results=search_results,
            warmup_time_secs=current_step.warmup_time_secs,
            execution_time_secs=current_step.execution_time_secs,
            sleep_between_requests_secs=current_step.sleep_between_requests_secs
        )
        merged_steps.append(merged_step)

    return merged_steps


def dump_result(steps: List[BenchmarkStepResult], output_path: str):
    """Dump the benchmark results into a JSON file.

    This function takes the previously merged benchmark results and writes them into a JSON file.
    The JSON structure can be seen in the top level documentation.
    
    Args:
        steps (List[BenchmarkStepResult]): The merged benchmark results from all ranks.
        output_path (str): The file path where the JSON output will be saved.
    """
    # Convert into something serializable
    output_data = []
    for step in steps:
        step_data = {
            "search_results": [],
            "warmup_time_secs": step.warmup_time_secs,
            "execution_time_secs": step.execution_time_secs,
            "sleep_between_requests_secs": step.sleep_between_requests_secs
        }

        for query_result in step.search_results:
            query_data = {
                "query": query_result.body,
                "responses": []
            }

            for responses in query_result.responses:
                serialized_responses = []
                for response in responses:
                    if isinstance(response, Success):
                        serialized_responses.append({
                            "latency": response.latency,
                            "docs": response.docs
                        })
                    elif isinstance(response, Failure):
                        serialized_responses.append({
                            "error_code": response.error_code
                        })
                query_data["responses"].append(serialized_responses)

            step_data["search_results"].append(query_data)

        output_data.append(step_data)

    with open(output_path, "w") as fp:
        json.dump(output_data, fp, indent=2)

def print_root(*args, **kwargs):
    """Print messages only if executed by the root MPI process.

    This function behaves like the built-in print function but restricts output
    to the root process in an MPI setting. It's useful for avoiding redundant output 
    in a parallel execution environment.

    Args:
        *args: passed to `print`
        **kwargs: passed to `print`
    """
    if is_root():
        print(*args, **kwargs)

def test_main(args: argparse.Namespace):
    """
    Execute the script in test mode. This mode runs only on the root node and processes each query once.
    It prints both input and output JSON for each query for debugging purposes.
    
    Args:
        args (argparse.Namespace): Parsed command line arguments.
    """
    print_root(f"Loading queries from JSON file: {args.json_file}")
    benchmark_data = load_json_queries(args.json_file)

    print_root("Executing test mode for each query...")
    for i, benchmark_step in enumerate(benchmark_data):
        print_root(f"Executing benchmark step {i+1}/{len(benchmark_data)}")

        es_server = select_es_server(args.hosts)
        server_endpoint = f"http://{es_server}/{args.indexname}/_search?request_cache=false"

        for query in benchmark_step.search_queries:
            print_root("--------------------------------------------------")
            print_root("Input Query:", json.dumps(query, indent=2))
            response = requests.post(server_endpoint, json=query["body"])
            print_root("Result:", json.dumps(response.json(), indent=2))
            print_root("--------------------------------------------------")


    print_root("Test Mode execution completed.")

def main() -> None:
    """
    Main function to execute the script logic.
    """
    print_root("Starting Elasticsearch Query Benchmarking...")
    args = parse_arguments()
    print_root("Command line arguments parsed successfully.")


    if args.testmode:
        if is_root():
            print_root("Running in test mode")
            test_main(args)
        return

    print_root(f"Loading queries from JSON file: {args.json_file}")
    benchmark_data = load_json_queries(args.json_file)

    es_server = select_es_server(args.hosts)

    # The actual runs
    print_root("Starting benchmark steps...")
    all_step_results = []
    for i, benchmark_step in enumerate(benchmark_data):
        # Make sure that the server survived the last one before we benchmark again
        if is_root() and not args.allowbad:
            wait_for_green_cluster(es_server)

        print_root(f"Running benchmark step {i+1}/{len(benchmark_data)}")

        COMM.Barrier()
        step_result = run_step(
            benchmark_step, 
            es_server,
            args.indexname,
            args.warmup,
            args.execution,
            args.sleep
        )
        all_step_results.append(step_result)

        print_root(f"Benchmark step {i+1} completed.")

    COMM.Barrier()

    print_root("Merging results from all ranks...")
    combined_results = merge_step_results_mpi(all_step_results)
    print_root("Results merged successfully.")
    COMM.Barrier()

    if is_root():
        print_root(f"Writing final results to {args.output}")
        dump_result(combined_results, args.output)

    print_root("Elasticsearch Query Benchmarking completed.")

    
if __name__ == "__main__":
    main()

