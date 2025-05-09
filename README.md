# SplitBench Kubernetes Benchmarking Tool

A CLI tool to run DiskSPD benchmarks on Kubernetes clusters. This tool supports running benchmarks on both single and multiple nodes.

## Installation

Clone the repository and run with `uv`:

```bash
# Clone the repository
git clone https://github.com/yourusername/splitbench.git
cd splitbench

# Run directly with uv
uv run main.py --help
```

Or install the package:

```bash
# Install with uv
uv pip install .

# Run the installed package
splitbench --help
```

## Usage

The tool provides two main commands:

### Single Node Benchmark

Run a benchmark on a single Kubernetes node:

```bash
uv run main.py single --storage-class "your-storage-class" --duration 60
```

### Multi-Node Benchmark

Run benchmarks across multiple Kubernetes nodes:

```bash
uv run main.py multi --nodes 3 --storage-class "your-storage-class" --duration 60
```

## Options

### Single Node Command

- `--storage-class`: Storage class for the PVC (default: "default")
- `--duration`: Test duration in seconds (default: 60)
- `--output-dir`: Directory to store results (default: "results")

### Multi-Node Command

- `--nodes`: Number of worker nodes to run benchmarks on (default: 3)
- `--storage-class`: Storage class for the PVCs (default: "default")
- `--duration`: Test duration in seconds (default: 60)
- `--output-dir`: Directory to store results (default: "results")

## Requirements

- Python 3.9+
- Kubernetes cluster with `kubectl` configured
- Storage classes set up in the Kubernetes cluster