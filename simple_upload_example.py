#!/usr/bin/env python3
"""
Simple example: Upload an arXiv PDF and trace its processing state.

This script demonstrates:
1. How to upload a PDF for processing
2. How to monitor the task state
3. How to use the monitoring system to diagnose issues

Prerequisites:
- Docker containers running: Redis, RabbitMQ
- API running: python src/app/main.py
- Worker running: cd src && celery -A worker.tasks worker --loglevel=info
- (Optional) Celery Flower: cd src && celery -A worker.tasks flower
"""

import asyncio
import httpx
import json
import time
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

base_url = "http://localhost:8001"

async def upload_and_monitor_pdf():
    """Upload a PDF and monitor its processing state."""
    console = Console()
    
    # Example arXiv PDF - a small paper about AI
    #pdf_url = "https://arxiv.org/pdf/2301.07041.pdf"
    pdf_url = "https://arxiv.org/pdf/1706.03762"
    paper_title = "GPT-3.5 Turbo Fine-tuning and API Updates"
    
    console.print(f"[bold blue]📄 PDF Processing Example[/bold blue]")
    console.print(f"Paper: {paper_title}")
    console.print(f"URL: {pdf_url}")
    console.print("=" * 60)
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        
        # Step 1: Upload the PDF
        console.print("[cyan]Step 1: Uploading PDF for processing...[/cyan]")
        
        try:
            # Enhanced upload request with parse configuration
            upload_response = await client.post(
                f"{base_url}/upload",
                json={
                    "url": pdf_url,
                    "parser_hint": "docling",  # You can try "marker" or "unstructured" too
                    "parse_config": {
                        "do_ocr": True,
                        "do_table_structure": True,
                        "ocr_language": "en"
                    }
                }
            )
            
            if upload_response.status_code != 200:
                console.print(f"[red]❌ Upload failed: {upload_response.status_code}[/red]")
                console.print(f"Response: {upload_response.text}")
                return None
            
            upload_data = upload_response.json()
            task_id = upload_data["task_id"]
            queue_position = upload_data.get("queue_position", 0)
            
            console.print(f"[green]✅ Upload successful![/green]")
            console.print(f"Task ID: [bold]{task_id}[/bold]")
            console.print(f"Queue position: {queue_position}")
            
        except Exception as e:
            console.print(f"[red]❌ Error uploading: {e}[/red]")
            return None
        
        # Step 2: Show different ways to monitor the task
        console.print(f"\n[cyan]Step 2: Monitoring task {task_id[:8]}...[/cyan]")
        
        # Method 1: Basic status endpoint
        console.print("\n[bold]Method 1: Basic Status Check[/bold]")
        try:
            status_response = await client.get(f"{base_url}/status/{task_id}")
            if status_response.status_code == 200:
                status_data = status_response.json()
                console.print(f"State: {status_data.get('state', 'unknown')}")
                console.print(f"Progress: {status_data.get('progress', 0)}%")
                
                # Show new enhanced fields
                if status_data.get('started_at'):
                    console.print(f"Started at: {status_data['started_at']}")
                if status_data.get('parser_used'):
                    console.print(f"Parser used: {status_data['parser_used']}")
                if status_data.get('pages_processed'):
                    console.print(f"Pages processed: {status_data['pages_processed']}")
            else:
                console.print(f"[red]Status check failed: {status_response.status_code}[/red]")
        except Exception as e:
            console.print(f"[red]Error checking status: {e}[/red]")
        
        # Method 2: Detailed monitoring info
        console.print("\n[bold]Method 2: Detailed Monitoring Info[/bold]")
        try:
            detailed_response = await client.get(f"{base_url}/monitoring/tasks?task_id={task_id}")
            if detailed_response.status_code == 200:
                detailed_data = detailed_response.json()
                
                # Show basic info from the new unified endpoint
                if detailed_data.get("task"):
                    task_info = detailed_data["task"]
                    if task_info.get("status"):
                        status = task_info["status"]
                        console.print(f"State: {status.get('state', 'unknown')}")
                        console.print(f"Progress: {status.get('progress', 0)}%")
                        console.print(f"Created: {status.get('created_at', 'unknown')}")
                        console.print(f"Updated: {status.get('updated_at', 'unknown')}")
                    
                    # Show progress log if available
                    if task_info.get("progress_log"):
                        console.print("\nProgress Log:")
                        for entry in task_info["progress_log"][-3:]:  # Last 3 entries
                            timestamp = entry["timestamp"][:19].replace("T", " ")
                            step = entry["step"]
                            console.print(f"  {timestamp}: {step}")
            else:
                console.print(f"[red]Detailed check failed: {detailed_response.status_code}[/red]")
        except Exception as e:
            console.print(f"[red]Error checking detailed status: {e}[/red]")
        
        # Method 3: Task diagnosis (useful if stuck)
        console.print("\n[bold]Method 3: Task Diagnosis[/bold]")
        try:
            diagnosis_response = await client.get(f"{base_url}/monitoring/tasks?task_id={task_id}&diagnose=true")
            if diagnosis_response.status_code == 200:
                diagnosis_data = diagnosis_response.json()
                
                if diagnosis_data.get("diagnosis"):
                    diag = diagnosis_data["diagnosis"]
                    console.print(f"Task ID: {diag['task_id'][:8]}...")
                    
                    if diag.get("recommendations"):
                        console.print("Recommendations:")
                        for rec in diag["recommendations"]:
                            console.print(f"  • {rec}")
                    else:
                        console.print("[green]No issues detected[/green]")
                else:
                    console.print("[green]No diagnosis needed[/green]")
            else:
                console.print(f"[red]Diagnosis failed: {diagnosis_response.status_code}[/red]")
        except Exception as e:
            console.print(f"[red]Error running diagnosis: {e}[/red]")
        
        # Step 3: Real-time monitoring
        console.print(f"\n[cyan]Step 3: Real-time monitoring (30 seconds)...[/cyan]")
        console.print("Press Ctrl+C to stop early")
        
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                monitor_task = progress.add_task("Monitoring...", total=100)
                
                for i in range(30):  # Monitor for 30 seconds
                    try:
                        status_response = await client.get(f"{base_url}/status/{task_id}")
                        
                        if status_response.status_code == 200:
                            status_data = status_response.json()
                            state = status_data.get("state", "unknown")
                            prog = status_data.get("progress", 0)
                            
                            progress.update(
                                monitor_task, 
                                completed=prog,
                                description=f"State: {state} ({prog}%) - Check {i+1}/30"
                            )
                            
                            # If task completed, break (updated state names)
                            if state in ["completed", "failed"]:
                                progress.update(
                                    monitor_task,
                                    description=f"Task completed: {state}"
                                )
                                break
                        
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        progress.update(monitor_task, description=f"Error: {e}")
                        break
        
        except KeyboardInterrupt:
            console.print("\n[yellow]Monitoring stopped by user[/yellow]")
        
        # Step 4: Final status and results
        console.print(f"\n[cyan]Step 4: Final Status and Results[/cyan]")
        
        try:
            final_response = await client.get(f"{base_url}/status/{task_id}")
            if final_response.status_code == 200:
                final_data = final_response.json()
                state = final_data.get("state", "unknown")
                progress_pct = final_data.get("progress", 0)
                
                # Create status panel (updated state names and enhanced info)
                if state == "completed":
                    status_color = "green"
                    status_icon = "✅"
                    result_key = final_data.get("result_key")
                    table_keys = final_data.get("table_keys", [])
                    started_at = final_data.get("started_at")
                    parser_used = final_data.get("parser_used")
                    pages_processed = final_data.get("pages_processed")
                    
                    panel_content = f"{status_icon} Task completed successfully!\n"
                    panel_content += f"Progress: {progress_pct}%\n"
                    if started_at:
                        panel_content += f"Started at: {started_at}\n"
                    if parser_used:
                        panel_content += f"Parser used: {parser_used}\n"
                    if pages_processed:
                        panel_content += f"Pages processed: {pages_processed}\n"
                    if result_key:
                        panel_content += f"Result stored at: {result_key}\n"
                    if table_keys:
                        panel_content += f"Tables extracted: {len(table_keys)} files\n"
                        panel_content += f"Table files: {', '.join(table_keys[:3])}{'...' if len(table_keys) > 3 else ''}"
                    
                elif state == "failed":
                    status_color = "red"
                    status_icon = "❌"
                    error_msg = final_data.get("error", "Unknown error")
                    
                    panel_content = f"{status_icon} Task failed\n"
                    panel_content += f"Progress: {progress_pct}%\n"
                    panel_content += f"Error: {error_msg}"
                    
                elif state == "running":
                    status_color = "yellow"
                    status_icon = "🔄"
                    panel_content = f"{status_icon} Task still running\nProgress: {progress_pct}%"
                    
                else:  # waiting
                    status_color = "blue"
                    status_icon = "⏳"
                    panel_content = f"{status_icon} Task waiting\nProgress: {progress_pct}%"
                
                panel = Panel(panel_content, title=f"Task {task_id[:8]}", border_style=status_color)
                console.print(panel)
            
        except Exception as e:
            console.print(f"[red]Error getting final status: {e}[/red]")
        
        return task_id


async def demonstrate_parse_configs():
    """Demonstrate different parse configuration options."""
    console = Console()
    
    console.print(f"\n[bold cyan]🔧 Parse Configuration Examples[/bold cyan]")
    
    # Different configuration examples
    config_examples = [
        {
            "name": "Default Configuration",
            "config": {
                "do_ocr": True,
                "do_table_structure": True,
                "ocr_language": "en"
            },
            "description": "Full processing with OCR and table extraction"
        },
        {
            "name": "OCR Disabled",
            "config": {
                "do_ocr": False,
                "do_table_structure": True,
                "ocr_language": "en"
            },
            "description": "Skip OCR for text-based PDFs (faster)"
        },
        {
            "name": "Tables Only",
            "config": {
                "do_ocr": False,
                "do_table_structure": True,
                "ocr_language": "en"
            },
            "description": "Focus on table extraction only"
        },
        {
            "name": "Spanish OCR",
            "config": {
                "do_ocr": True,
                "do_table_structure": True,
                "ocr_language": "es"
            },
            "description": "OCR optimized for Spanish text"
        },
        {
            "name": "Minimal Processing",
            "config": {
                "do_ocr": False,
                "do_table_structure": False,
                "ocr_language": "en"
            },
            "description": "Basic text extraction only (fastest)"
        }
    ]
    
    config_table = Table(title="Parse Configuration Options")
    config_table.add_column("Configuration", style="cyan")
    config_table.add_column("Settings", style="white")
    config_table.add_column("Use Case", style="green")
    
    for example in config_examples:
        config_str = json.dumps(example["config"], indent=2)
        config_table.add_row(
            example["name"],
            config_str,
            example["description"]
        )
    
    console.print(config_table)
    
    console.print(f"\n[bold]Example Upload Request with Configuration:[/bold]")
    console.print("""
[cyan]curl -X POST http://localhost:8001/upload \\
  -H "Content-Type: application/json" \\
  -d '{
    "url": "https://example.com/document.pdf",
    "parser_hint": "docling",
    "parse_config": {
      "do_ocr": true,
      "do_table_structure": true,
      "ocr_language": "en"
    }
  }'[/cyan]
    """)


async def show_monitoring_commands(task_id: str):
    """Show CLI commands for monitoring this specific task."""
    console = Console()
    
    console.print(f"\n[bold blue]🔧 Monitoring Commands for Task {task_id[:8]}[/bold blue]")
    
    commands_table = Table(title="Useful Monitoring Commands")
    commands_table.add_column("Command", style="cyan")
    commands_table.add_column("Description", style="white")
    
    commands = [
        (f"python monitor_system.py diagnose {task_id}", "Diagnose this specific task"),
        ("python monitor_system.py health", "Check overall system health"),
        ("python monitor_system.py pending", "Analyze all pending tasks"),
        ("python monitor_system.py stuck", "Find stuck tasks"),
        ("python monitor_system.py tasks --limit 10", "List recent tasks"),
        ("python monitor_system.py watch", "Watch system in real-time"),
    ]
    
    for cmd, desc in commands:
        commands_table.add_row(cmd, desc)
    
    console.print(commands_table)
    
    # Celery Flower info
    console.print(f"\n[bold]🌸 Celery Flower Dashboard[/bold]")
    console.print("If you have Flower running, you can also monitor at:")
    console.print("• [cyan]http://localhost:5555[/cyan] - Celery Flower web interface")
    console.print("• View workers, queues, and individual task details")
    
    # API endpoints
    console.print(f"\n[bold]🌐 Direct API Endpoints[/bold]")
    api_endpoints = [
        (f"/status/{task_id}", "Basic task status"),
        (f"/monitoring/tasks?task_id={task_id}", "Detailed task info"),
        (f"/monitoring/tasks?task_id={task_id}&diagnose=true", "Task diagnosis"),
        ("/monitoring/health", "System health"),
        ("/monitoring/tasks", "All tasks"),
    ]
    
    for endpoint, desc in api_endpoints:
        console.print(f"• [cyan]curl {base_url}{endpoint}[/cyan] - {desc}")


async def main():
    """Main example function."""
    console = Console()
    
    console.print("[bold green]🚀 PDF Parser Enhanced Upload Example[/bold green]")
    console.print()
    console.print("This example will:")
    console.print("1. Show enhanced parse configuration options")
    console.print("2. Upload an arXiv PDF with parse config")
    console.print("3. Demonstrate enhanced status monitoring")
    console.print("4. Display detailed results with new fields")
    console.print("5. Show useful monitoring commands")
    console.print()
    
    # Check if system is running
    console.print("[cyan]Checking if system is running...[/cyan]")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            health_response = await client.get(f"{base_url}/health")
            if health_response.status_code == 200:
                console.print("[green]✅ System is running![/green]")
            else:
                console.print(f"[red]❌ System health check failed: {health_response.status_code}[/red]")
                return
    except Exception as e:
        console.print(f"[red]❌ Cannot connect to system: {e}[/red]")
        console.print("\nMake sure you have:")
        console.print("1. Redis running: docker run -d -p 6379:6379 redis")
        console.print("2. RabbitMQ running: docker run -d -p 5672:5672 rabbitmq")
        console.print("3. API running: python src/app/main.py")
        console.print("4. Worker running: cd src && celery -A worker.tasks worker --loglevel=info")
        return
    
    # Show parse configuration examples
    await demonstrate_parse_configs()
    
    # Run the example
    task_id = await upload_and_monitor_pdf()
    
    if task_id:
        await show_monitoring_commands(task_id)
        
        console.print(f"\n[bold green]🎉 Enhanced Example completed![/bold green]")
        console.print(f"Your task ID is: [bold]{task_id}[/bold]")
        console.print("\n✨ [bold]New Features Demonstrated:[/bold]")
        console.print("  • Enhanced upload with parse_config")
        console.print("  • Detailed status with started_at, parser_used, pages_processed")
        console.print("  • Updated state names (waiting/running/completed/failed)")
        console.print("  • Multiple parse configuration examples")
        console.print("\n🔍 You can continue monitoring this task using the commands shown above.")


if __name__ == "__main__":
    asyncio.run(main()) 