"""
SplitBrain DiskSPD Kubernetes Benchmarking Tool

A CLI tool to run DiskSPD benchmarks on Kubernetes clusters.
"""

import os
import asyncio
import subprocess
from pathlib import Path

import typer
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)

app = typer.Typer(help="Run DiskSPD benchmarks on Kubernetes clusters")
console = Console()

# Get the directory where templates are stored
TEMPLATE_DIR = Path(__file__).parent / "templates"


def read_template(template_name: str) -> str:
    """Read a template file and return its contents."""
    template_path = TEMPLATE_DIR / template_name
    with open(template_path, "r") as f:
        return f.read()


def run_kubectl_command(command: list[str]) -> str:
    """Run a kubectl command and return its output."""
    try:
        result = subprocess.run(
            ["kubectl"] + command, check=True, text=True, capture_output=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        console.print(f"[bold red]Error running kubectl command:[/bold red] {e}")
        console.print(f"[bold red]Error output:[/bold red] {e.stderr}")
        raise typer.Exit(code=1)


async def wait_for_job_completion(job_name: str, timeout_seconds: int = 7200) -> bool:
    """Wait for a Kubernetes job to complete."""
    try:
        subprocess.run(
            [
                "kubectl",
                "wait",
                "--for=condition=complete",
                f"job/{job_name}",
                f"--timeout={timeout_seconds}s",
            ],
            check=True,
            text=True,
            capture_output=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False


async def capture_logs(node_num: int, output_dir: Path) -> None:
    """Capture logs from a completed job."""
    output_file = output_dir / f"node-{node_num}.txt"
    try:
        # First try to get logs using the label selector
        result = subprocess.run(
            ["kubectl", "logs", "-l", f"node-test=node-{node_num}", "--tail=500"],
            check=True,
            text=True,
            capture_output=True,
        )
        
        # If no logs found using label, try getting logs directly from the pod
        if not result.stdout.strip():
            # Find the pod name for this job
            pod_list = subprocess.run(
                ["kubectl", "get", "pods", "-l", f"node-test=node-{node_num}", "-o", "name"],
                check=True,
                text=True,
                capture_output=True,
            )
            
            if pod_list.stdout.strip():
                pod_name = pod_list.stdout.strip().split("\n")[0]
                result = subprocess.run(
                    ["kubectl", "logs", pod_name, "--tail=500"],
                    check=True,
                    text=True,
                    capture_output=True,
                )
        
        # Write the logs to file
        with open(output_file, "w") as f:
            f.write(result.stdout)
            
        console.print(f"[bold green]Logs for node {node_num} saved to {output_file}[/bold green]")
    except subprocess.CalledProcessError as e:
        console.print(
            f"[bold yellow]Warning: Could not capture logs for node {node_num}[/bold yellow]"
        )
        console.print(f"[bold yellow]Error: {e.stderr}[/bold yellow]")
        
        # Try another approach to get logs - get all pods for this job and get logs from first one
        try:
            # Get the pod name for this job
            job_pods = subprocess.run(
                ["kubectl", "get", "pods", "--selector=job-name=diskspd-node-" + str(node_num), "-o", "name"],
                check=True,
                text=True,
                capture_output=True,
            )
            
            if job_pods.stdout.strip():
                pod_name = job_pods.stdout.strip().split("\n")[0]
                logs = subprocess.run(
                    ["kubectl", "logs", pod_name],
                    check=True,
                    text=True,
                    capture_output=True,
                )
                with open(output_file, "w") as f:
                    f.write(logs.stdout)
                console.print(f"[bold green]Logs for node {node_num} saved to {output_file}[/bold green]")
        except subprocess.CalledProcessError as e2:
            console.print(f"[bold red]All attempts to get logs for node {node_num} failed[/bold red]")
            # Create an empty file with error message so we know this node was processed
            with open(output_file, "w") as f:
                f.write(f"Failed to capture logs for node {node_num}.\nError: {e.stderr}\nSecond error: {e2.stderr}")


@app.command()
def single(
    storage_class: str = typer.Option("default", help="Storage class for the PVC"),
    duration: int = typer.Option(60, help="Test duration in seconds"),
    output_dir: str = typer.Option("results", help="Directory to store results"),
):
    """Run DiskSPD benchmark on a single node."""
    console.print(
        "[bold green]Running DiskSPD benchmark on a single node...[/bold green]"
    )

    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)

    # Read and modify the template
    template = read_template("single-node.yaml")
    template = template.replace("default", storage_class)
    template = template.replace('"-d60"', f'"-d{duration}"')

    # Apply the template
    with console.status(
        "[bold green]Deploying single-node benchmark job...[/bold green]"
    ):
        with open("temp_single_node.yaml", "w") as f:
            f.write(template)
        run_kubectl_command(["apply", "-f", "temp_single_node.yaml"])
        os.remove("temp_single_node.yaml")

    # Wait for job completion
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold green]Waiting for job completion...[/bold green]"),
        BarColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        progress.add_task("Running benchmark...", total=None)
        result = asyncio.run(wait_for_job_completion("diskspd-single-node"))

    if result:
        console.print("[bold green]Job completed successfully![/bold green]")

        # Capture logs
        with console.status("[bold green]Capturing logs...[/bold green]"):
            try:
                logs = run_kubectl_command(
                    ["logs", "-l", "node-test=single-node", "--tail=500"]
                )
                output_file = output_path / "single-node.txt"
                with open(output_file, "w") as f:
                    f.write(logs)
                console.print(f"[bold green]Logs saved to {output_file}[/bold green]")
            except Exception as e:
                console.print(f"[bold red]Error capturing logs: {e}[/bold red]")
    else:
        console.print("[bold red]Job failed or timed out![/bold red]")


@app.command()
def multi(
    nodes: int = typer.Option(3, help="Number of worker nodes to run benchmarks on"),
    storage_class: str = typer.Option("default", help="Storage class for the PVCs"),
    duration: int = typer.Option(60, help="Test duration in seconds"),
    output_dir: str = typer.Option("results", help="Directory to store results"),
):
    """Run DiskSPD benchmark across multiple nodes."""
    console.print(
        f"[bold green]Running DiskSPD benchmark across {nodes} nodes...[/bold green]"
    )

    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)

    # Read and modify the template
    template = read_template("multi-node.yaml")
    template = template.replace("${STORAGE_CLASS}", storage_class)
    template = template.replace('"-d60"', f'"-d{duration}"')

    # Deploy jobs for all nodes
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold green]Deploying benchmark jobs...[/bold green]"),
        BarColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Deploying...", total=nodes)

        for i in range(1, nodes + 1):
            node_template = template.replace("${NODE_NUM}", str(i))
            with open(f"temp_node_{i}.yaml", "w") as f:
                f.write(node_template)
            run_kubectl_command(["apply", "-f", f"temp_node_{i}.yaml"])
            os.remove(f"temp_node_{i}.yaml")
            progress.update(task, advance=1)

    console.print(
        "[bold green]All jobs deployed. Waiting for completion and capturing logs...[/bold green]"
    )

    # Wait for all jobs to complete and capture logs in parallel
    async def process_all_nodes():
        tasks = []
        for i in range(1, nodes + 1):
            tasks.append(asyncio.create_task(process_node(i)))
        await asyncio.gather(*tasks)

    async def process_node(node_num: int):
        job_name = f"diskspd-node-{node_num}"
        result = await wait_for_job_completion(job_name)
        if result:
            await capture_logs(node_num, output_path)

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold green]Processing nodes...[/bold green]"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Running benchmarks...", total=None)
        asyncio.run(process_all_nodes())

    console.print("[bold green]All jobs completed and logs collected![/bold green]")


if __name__ == "__main__":
    app()
