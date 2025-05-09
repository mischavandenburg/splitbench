#!/usr/bin/env python3
"""
Diskspd Benchmark Results Parser

This script parses diskspd benchmark results from multiple nodes and converts them to CSV format
for easy import into spreadsheets like Google Sheets.
"""

import os
import re
import csv
from collections import defaultdict

# Base directory containing the benchmark results
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results")

# Output directory structure
OUTPUT_DIR = os.path.join(BASE_DIR, "results")
VM_SIZE_DIR = os.path.join(OUTPUT_DIR, "Standard_D4s_v3")

def parse_command_line(content):
    """Extract the command line parameters from the result file"""
    match = re.search(r"Command Line: (.*)", content)
    if match:
        return match.group(1)
    return "N/A"

def parse_system_info(content):
    """Extract system info from the result file"""
    processor_count = re.search(r"processor count: (\d+)", content)
    caching_options = re.search(r"caching options: (.*)", content)
    
    return {
        "processor_count": processor_count.group(1) if processor_count else "N/A",
        "caching_options": caching_options.group(1) if caching_options else "N/A"
    }

def parse_input_parameters(content):
    """Extract key input parameters from the result file"""
    params = {}
    
    # Extract duration
    duration_match = re.search(r"duration: (\d+)s", content)
    if duration_match:
        params["duration"] = duration_match.group(1)
    
    # Extract block size
    block_size_match = re.search(r"block size: (\d+)", content)
    if block_size_match:
        # Convert from bytes to K
        block_size_bytes = int(block_size_match.group(1))
        params["block_size"] = str(block_size_bytes // 1024)
    
    # Extract outstanding I/O operations
    io_ops_match = re.search(r"number of outstanding I/O operations: (\d+)", content)
    if io_ops_match:
        params["outstanding_io"] = io_ops_match.group(1)
    
    # Extract threads
    threads_match = re.search(r"total threads: (\d+)", content)
    if threads_match:
        params["threads"] = threads_match.group(1)
    
    # Extract file size
    file_size_match = re.search(r"size: (\d+)B", content)
    if file_size_match:
        file_size_bytes = int(file_size_match.group(1))
        file_size_gb = file_size_bytes / (1024 * 1024 * 1024)
        params["file_size_gb"] = f"{file_size_gb:.2f}"
    
    # Extract I/O type (random or sequential)
    io_type_match = re.search(r"using (random|sequential) I/O", content)
    if io_type_match:
        params["io_type"] = io_type_match.group(1)
    
    return params

def parse_cpu_usage(content):
    """Extract CPU usage information"""
    # Find the CPU usage section
    cpu_section = re.search(r"CPU\s+\|\s+Usage.*?avg:\s+([\d.]+)%\s+\|\s+([\d.]+)%\s+\|\s+([\d.]+)%\s+\|\s+([\d.]+)%\s+\|\s+([\d.]+)%", 
                           content, re.DOTALL)
    
    if cpu_section:
        return {
            "cpu_total_usage": cpu_section.group(1),
            "cpu_user": cpu_section.group(2),
            "cpu_kernel": cpu_section.group(3),
            "cpu_io_wait": cpu_section.group(4),
            "cpu_idle": cpu_section.group(5)
        }
    return {}

def parse_io_stats(content, io_type="Total"):
    """Extract IO statistics for a specific IO type (Total, Read, Write)"""
    # Find the section for the specified IO type
    section_pattern = rf"{io_type} IO.*?total:\s+([\d]+)\s+\|\s+([\d]+)\s+\|\s+([\d.]+)\s+\|\s+([\d.]+)\s+\|\s+([\d.]+)\s+\|"
    io_section = re.search(section_pattern, content, re.DOTALL)
    
    if io_section:
        return {
            f"{io_type.lower()}_bytes": io_section.group(1),
            f"{io_type.lower()}_ios": io_section.group(2),
            f"{io_type.lower()}_mbps": io_section.group(3),
            f"{io_type.lower()}_iops": io_section.group(4),
            f"{io_type.lower()}_avg_latency": io_section.group(5)
        }
    return {}

def parse_latency_percentiles(content):
    """Extract latency percentiles"""
    percentiles = {}
    
    # Find the latency percentiles section
    latency_section = re.search(r"%-ile.*?max \|.*?\n", content, re.DOTALL)
    if latency_section:
        section_text = latency_section.group(0)
        
        # Extract specific percentiles
        for percentile in ["min", "50th", "95th", "99th", "max"]:
            pattern = rf"{percentile}\s+\|\s+([\d.]+)\s+\|"
            match = re.search(pattern, section_text)
            if match:
                percentiles[f"latency_{percentile}"] = match.group(1)
    
    return percentiles

def parse_result_file(file_path, benchmark_id, node_id):
    """Parse a single result file and extract all relevant metrics"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Initialize result dictionary with benchmark and node identifiers
    result = {
        "benchmark_id": benchmark_id,
        "node_id": node_id
    }
    
    # Parse different sections of the result file
    result["command_line"] = parse_command_line(content)
    result.update(parse_system_info(content))
    result.update(parse_input_parameters(content))
    result.update(parse_cpu_usage(content))
    result.update(parse_io_stats(content, "Total"))
    result.update(parse_io_stats(content, "Read"))
    result.update(parse_io_stats(content, "Write"))
    result.update(parse_latency_percentiles(content))
    
    return result

def process_benchmark_results():
    """Process all benchmark results and generate CSV files"""
    all_results = []
    
    # Get all node result files directly from the results directory
    node_files = [f for f in os.listdir(RESULTS_DIR) if f.startswith("node-") and f.endswith(".txt")]
    
    # Parse each node result file
    for node_file in node_files:
        node_id = node_file.replace("node-", "").replace(".txt", "")
        file_path = os.path.join(RESULTS_DIR, node_file)
        
        try:
            # Use a single benchmark ID since all files are from the same run
            result = parse_result_file(file_path, "benchmark-01", node_id)
            all_results.append(result)
            print(f"Processed node {node_id}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    return all_results

def ensure_directory_exists(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def get_block_size_dir(results):
    """Determine the block size directory name from results"""
    # Get block size from the first result
    if results and "block_size" in results[0]:
        return f"{results[0]['block_size']}K"
    return "unknown_block_size"

def write_summary_csv(results, output_file=None):
    """Write a summary CSV with key metrics from all nodes and benchmarks"""
    if not results:
        print("No results to write to CSV")
        return
    
    # Create the output directory structure
    ensure_directory_exists(OUTPUT_DIR)
    ensure_directory_exists(VM_SIZE_DIR)
    
    # Get block size directory
    block_size_dir_name = get_block_size_dir(results)
    block_size_dir = os.path.join(VM_SIZE_DIR, block_size_dir_name)
    ensure_directory_exists(block_size_dir)
    
    # Set output file path if not provided
    if output_file is None:
        output_file = os.path.join(block_size_dir, f"benchmark_summary_{block_size_dir_name}.csv")
    
    # Determine all fields from the first result
    fieldnames = list(results[0].keys())
    
    # Write CSV file
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"Summary CSV written to {output_file}")

def write_benchmark_specific_csvs(results):
    """Write separate CSV files for each benchmark"""
    if not results:
        return
    
    # Create the output directory structure
    ensure_directory_exists(OUTPUT_DIR)
    ensure_directory_exists(VM_SIZE_DIR)
    
    # Get block size directory
    block_size_dir_name = get_block_size_dir(results)
    block_size_dir = os.path.join(VM_SIZE_DIR, block_size_dir_name)
    ensure_directory_exists(block_size_dir)
    
    # Group results by benchmark ID
    benchmark_results = defaultdict(list)
    for result in results:
        benchmark_results[result["benchmark_id"]].append(result)
    
    # Write a CSV for each benchmark
    for benchmark_id, benchmark_data in benchmark_results.items():
        if not benchmark_data:
            continue
        
        output_file = os.path.join(block_size_dir, f"benchmark_{benchmark_id}_{block_size_dir_name}.csv")
        fieldnames = list(benchmark_data[0].keys())
        
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(benchmark_data)
        
        print(f"Benchmark {benchmark_id} CSV written to {output_file}")

def main():
    """Main function to process all benchmark results and generate CSV files"""
    print("Starting diskspd benchmark results processing...")
    
    # Process all benchmark results
    results = process_benchmark_results()
    
    if results:
        # Write a summary CSV with all results
        write_summary_csv(results)
        
        # Write separate CSVs for each benchmark
        write_benchmark_specific_csvs(results)
        
        print(f"Successfully processed {len(results)} result files")
    else:
        print("No results were processed")

if __name__ == "__main__":
    main()
