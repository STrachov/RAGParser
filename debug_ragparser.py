 #!/usr/bin/env python3
"""
Debug script for RAGParser - helps diagnose why tasks are stuck in 'waiting' state.
"""

import asyncio
import json
import requests
import subprocess
import sys
import time
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

# Configuration
API_BASE_URL = "http://localhost:8001"  # Adjust if different
TASK_ID = "1d9ee652-75c5-4532-8973-527a5a86ac55"  # Your stuck task ID


async def check_system_components():
    """Check if all system components are running."""
    console.print("[bold blue]🔧 System Components Check[/bold blue]")
    
    components = {
        "API Server": f"{API_BASE_URL}/health",
        "Redis": None,  # We'll check via API
        "Celery Worker": None,  # We'll check via monitoring
    }
    
    # Check API
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            console.print("✅ API Server: [green]Running[/green]")
            api_data = response.json()
            if api_data.get("redis"):
                console.print("✅ Redis: [green]Connected[/green]")
            else:
                console.print("❌ Redis: [red]Not connected[/red]")
        else:
            console.print(f"❌ API Server: [red]Error {response.status_code}[/red]")
            return False
    except Exception as e:
        console.print(f"❌ API Server: [red]Cannot connect - {e}[/red]")
        console.print("\n[yellow]Make sure the API is running:[/yellow]")
        console.print("  cd src && python -m app.main")
        return False
    
    # Check monitoring endpoint for worker info
    try:
        response = requests.get(f"{API_BASE_URL}/monitoring/health", timeout=5)
        if response.status_code == 200:
            health_data = response.json()
            workers_health = None
            
            for component in health_data.get("components", []):
                if component.get("component") == "workers":
                    workers_health = component
                    break
            
            if workers_health and workers_health.get("healthy"):
                worker_count = workers_health.get("details", {}).get("total_workers", 0)
                console.print(f"✅ Celery Workers: [green]{worker_count} active[/green]")
            else:
                console.print("❌ Celery Workers: [red]No workers active[/red]")
                console.print("\n[yellow]Start a worker with:[/yellow]")
                console.print("  cd src && celery -A worker.tasks worker --loglevel=info --queues=cpu")
                return False
        else:
            console.print("⚠️  Cannot check worker status via monitoring endpoint")
    except Exception as e:
        console.print(f"⚠️  Cannot check workers: {e}")
    
    return True


def check_task_status(task_id: str):
    """Check detailed status of a specific task."""
    console.print(f"\n[bold blue]📋 Task Status: {task_id[:8]}...[/bold blue]")
    
    try:
        # Basic status check
        response = requests.get(f"{API_BASE_URL}/status/{task_id}")
        if response.status_code == 200:
            status_data = response.json()
            
            # Create status table
            table = Table(title="Task Status")
            table.add_column("Field", style="cyan")
            table.add_column("Value", style="white")
            
            for key, value in status_data.items():
                if value is not None:
                    table.add_row(key, str(value))
            
            console.print(table)
            
            if status_data.get("state") == "waiting":
                console.print("\n[red]❌ Task is stuck in 'waiting' state[/red]")
                console.print("This means the task is queued but no worker is processing it.")
                return False
            else:
                console.print(f"\n✅ Task state: {status_data.get('state')}")
                return True
                
        elif response.status_code == 404:
            console.print("[red]❌ Task not found - it may have expired[/red]")
            return False
        else:
            console.print(f"[red]❌ Error checking task: {response.status_code}[/red]")
            return False
            
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        return False


def check_celery_processes():
    """Check if Celery processes are running on the system."""
    console.print(f"\n[bold blue]🔍 Process Check[/bold blue]")
    
    try:
        # Check for celery processes
        result = subprocess.run(
            ["ps", "aux"], 
            capture_output=True, 
            text=True
        )
        
        celery_processes = []
        for line in result.stdout.split('\n'):
            if 'celery' in line and 'worker' in line:
                celery_processes.append(line.strip())
        
        if celery_processes:
            console.print(f"✅ Found {len(celery_processes)} Celery process(es)")
            for process in celery_processes:
                # Show abbreviated process info
                parts = process.split()
                if len(parts) > 10:
                    abbreviated = ' '.join(parts[:3]) + ' ... ' + ' '.join(parts[-3:])
                    console.print(f"  {abbreviated}")
        else:
            console.print("❌ No Celery worker processes found")
            console.print("\n[yellow]Start a worker with one of these commands:[/yellow]")
            console.print("  # Using the run script:")
            console.print("  cd src && python scripts/run.py worker")
            console.print("  # Or directly:")
            console.print("  cd src && celery -A worker.tasks worker --loglevel=info")
            
    except Exception as e:
        console.print(f"⚠️  Cannot check processes: {e}")


def check_queue_status():
    """Check Celery queue status."""
    console.print(f"\n[bold blue]📊 Queue Status[/bold blue]")
    
    try:
        # Try to get queue info from monitoring endpoint
        response = requests.get(f"{API_BASE_URL}/monitoring/tasks", timeout=5)
        if response.status_code == 200:
            data = response.json()
            tasks = data.get("tasks", [])
            
            # Count tasks by state
            state_counts = {}
            for task in tasks:
                state = task.get("state", "unknown")
                state_counts[state] = state_counts.get(state, 0) + 1
            
            if state_counts:
                table = Table(title="Tasks by State")
                table.add_column("State", style="cyan")
                table.add_column("Count", style="white")
                
                for state, count in state_counts.items():
                    table.add_row(state, str(count))
                
                console.print(table)
                
                waiting_count = state_counts.get("waiting", 0)
                if waiting_count > 0:
                    console.print(f"\n[yellow]⚠️  {waiting_count} task(s) waiting for processing[/yellow]")
            else:
                console.print("ℹ️  No tasks in monitoring system")
        else:
            console.print("⚠️  Cannot check queue status")
            
    except Exception as e:
        console.print(f"⚠️  Error checking queue: {e}")


def suggest_solutions():
    """Suggest solutions based on common issues."""
    console.print(f"\n[bold green]💡 Troubleshooting Steps[/bold green]")
    
    solutions = [
        "1. **Start the Celery Worker** (most common issue)",
        "   cd src && celery -A worker.tasks worker --loglevel=info --queues=cpu",
        "",
        "2. **Check if Redis is running**",
        "   docker run -d -p 6379:6379 redis",
        "   # Or if using local Redis: redis-server",
        "",
        "3. **Check worker logs for errors**",
        "   Look for error messages in the worker console output",
        "",
        "4. **Restart all components**",
        "   - Stop the API and worker",
        "   - Restart Redis",
        "   - Start the API: cd src && python -m app.main",
        "   - Start the worker: cd src && celery -A worker.tasks worker --loglevel=info",
        "",
        "5. **Check for port conflicts**",
        "   - API should be on port 8001",
        "   - Redis should be on port 6379",
        "   - Make sure no other services are using these ports",
        "",
        "6. **Clear Redis cache** (if tasks are really stuck)",
        "   redis-cli FLUSHDB",
    ]
    
    for solution in solutions:
        if solution.startswith("   "):
            console.print(f"[dim]{solution}[/dim]")
        elif solution == "":
            console.print()
        else:
            console.print(solution)


def main():
    """Main debug function."""
    console.print("[bold green]🔧 RAGParser Debug Tool[/bold green]")
    console.print("This tool will help diagnose why your task is stuck in 'waiting' state.\n")
    
    # Step 1: Check system components
    system_ok = asyncio.run(check_system_components())
    
    # Step 2: Check specific task
    if TASK_ID:
        task_ok = check_task_status(TASK_ID)
    else:
        console.print("\n[yellow]No specific task ID provided, skipping task check[/yellow]")
        task_ok = False
    
    # Step 3: Check processes
    check_celery_processes()
    
    # Step 4: Check queue status
    check_queue_status()
    
    # Step 5: Provide solutions
    suggest_solutions()
    
    # Summary
    console.print(f"\n[bold blue]📋 Summary[/bold blue]")
    if system_ok and task_ok:
        console.print("✅ System appears to be working correctly")
    elif not system_ok:
        console.print("❌ System components have issues - focus on starting missing services")
    else:
        console.print("⚠️  System is running but task is stuck - check worker logs")
    
    console.print(f"\n[bold]Next Steps:[/bold]")
    console.print("1. Follow the troubleshooting steps above")
    console.print("2. If worker is not running, start it with the commands shown")
    console.print("3. Monitor worker logs for any error messages")
    console.print("4. Try submitting a new test task after fixing issues")


if __name__ == "__main__":
    main()