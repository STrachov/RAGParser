import json
from typing import Dict, List, Any, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import BaseModel

from monitoring.system_monitor import (
    SystemMonitor,
    diagnose_stuck_task,
    get_monitoring_report
)
from worker.enhanced_tasks import get_comprehensive_task_info


router = APIRouter(prefix="/monitoring", tags=["monitoring"])


class SystemOverviewResponse(BaseModel):
    """Response model for system overview."""
    timestamp: str
    collection_time_ms: float
    overall_healthy: bool
    critical_issues: List[str]
    system_health: Dict[str, Any]
    task_statistics: Dict[str, Any]
    pending_analysis: Dict[str, Any]


class TaskDetailsResponse(BaseModel):
    """Response model for detailed task information."""
    task_id: str
    status: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    start_info: Optional[Dict[str, Any]] = None
    completion_info: Optional[Dict[str, Any]] = None
    progress_log: Optional[List[Dict[str, Any]]] = None
    retry_history: Optional[List[Dict[str, Any]]] = None
    error_details: Optional[Dict[str, Any]] = None


class TaskDiagnosisResponse(BaseModel):
    """Response model for task diagnosis."""
    task_id: str
    task_info: Dict[str, Any]
    system_health: Dict[str, Any]
    recommendations: List[str]


@router.get("/health", response_model=SystemOverviewResponse)
async def get_system_health(
    include_report: bool = Query(False, description="Include human-readable report")
):
    """Get comprehensive system health overview with optional report."""
    monitor = SystemMonitor()
    try:
        overview = await monitor.get_system_overview()
        
        if include_report:
            report = await get_monitoring_report()
            overview["report"] = report
            
        return SystemOverviewResponse(**overview)
    finally:
        await monitor.cleanup()


@router.get("/tasks")
async def get_tasks(
    task_id: Optional[str] = Query(None, description="Get specific task by ID"),
    limit: int = Query(50, ge=1, le=500),
    state: Optional[str] = Query(None, description="Filter by task state"),
    stuck_only: bool = Query(False, description="Show only stuck tasks"),
    diagnose: bool = Query(False, description="Include diagnosis information"),
    pending_analysis: bool = Query(False, description="Include pending task analysis"),
    sync_celery: bool = Query(False, description="Sync with Celery before returning")
):
    """Unified endpoint for task information and analysis."""
    monitor = SystemMonitor()
    try:
        # Handle specific task request
        if task_id:
            # Try comprehensive task info first
            task_info = get_comprehensive_task_info(task_id)
            
            if not task_info:
                # Fallback to monitor task details
                task_details = await monitor.get_task_details(task_id)
                if not task_details:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Task {task_id} not found"
                    )
                task_info = {"task_id": task_id, "status": {"state": task_details.state}}
            
            result = {"task": TaskDetailsResponse(task_id=task_id, **task_info)}
            
            if diagnose:
                diagnosis = await diagnose_stuck_task(task_id)
                if "error" not in diagnosis:
                    result["diagnosis"] = TaskDiagnosisResponse(**diagnosis)
                else:
                    result["diagnosis_error"] = diagnosis["error"]
            
            return result
        
        # Handle bulk task operations
        if sync_celery:
            sync_report = await _sync_with_celery(monitor)
            
        if stuck_only:
            tasks = await monitor.get_stuck_tasks()
        else:
            tasks = await monitor.get_all_tasks(limit=limit)
        
        # Filter by state if specified
        if state:
            tasks = [task for task in tasks if task.state == state]
        
        # Convert to dict format for JSON response
        task_list = []
        for task in tasks:
            task_dict = {
                "task_id": task.task_id,
                "state": task.state,
                "progress": task.progress,
                "created_at": task.created_at.isoformat(),
                "updated_at": task.updated_at.isoformat(),
                "worker": task.worker,
                "queue": task.queue,
                "runtime": task.runtime,
                "retries": task.retries,
                "error_message": task.error_message,
                "result_key": task.result_key,
                "table_keys": task.table_keys,
            }
            task_list.append(task_dict)
        
        result = {
            "tasks": task_list,
            "count": len(task_list),
            "filters_applied": {
                "limit": limit,
                "state": state,
                "stuck_only": stuck_only
            }
        }
        
        # Add pending analysis if requested
        if pending_analysis:
            analysis = await monitor.analyze_pending_tasks()
            result["pending_analysis"] = analysis
        
        # Add sync report if performed
        if sync_celery:
            result["sync_report"] = sync_report
        
        return result
        
    finally:
        await monitor.cleanup()


@router.get("/components/{component}")
async def check_component_health(component: str):
    """Check health of a specific component (redis, celery, s3)."""
    monitor = SystemMonitor()
    try:
        if component == "redis":
            health = await monitor.check_redis_health()
        elif component == "celery":
            health = await monitor.check_celery_health()
        elif component == "s3":
            health = await monitor.check_s3_health()
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid component '{component}'. Valid options: redis, celery, s3"
            )
        
        return {
            "component": health.component,
            "healthy": health.healthy,
            "details": health.details,
            "last_check": health.last_check.isoformat(),
            "error_message": health.error_message
        }
    finally:
        await monitor.cleanup()


@router.post("/celery/sync")
async def sync_with_celery():
    """Manually sync task states with Celery and report differences."""
    monitor = SystemMonitor()
    try:
        return await _sync_with_celery(monitor)
    finally:
        await monitor.cleanup()


@router.get("/celery/comparison")
async def compare_with_celery():
    """Compare our task tracking with Celery's view."""
    monitor = SystemMonitor()
    try:
        # Get comprehensive comparison
        our_tasks = await monitor.get_all_tasks(limit=100)
        celery_task_ids = await monitor._discover_celery_tasks()
        
        comparison = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "our_tracking": len(our_tasks),
                "celery_active_reserved": len(celery_task_ids),
                "overlap": len(set(task.task_id for task in our_tasks) & celery_task_ids)
            },
            "our_tasks_by_state": {},
            "celery_tasks": [],
            "recommendations": []
        }
        
        # Analyze our tasks by state
        for task in our_tasks:
            state = task.state
            if state not in comparison["our_tasks_by_state"]:
                comparison["our_tasks_by_state"][state] = []
            
            comparison["our_tasks_by_state"][state].append({
                "task_id": task.task_id,
                "progress": task.progress,
                "age_seconds": (datetime.now() - task.updated_at).total_seconds()
            })
        
        # Get info about Celery tasks
        for task_id in list(celery_task_ids)[:20]:  # Limit to first 20 for performance
            celery_info = await monitor._get_celery_task_info(task_id)
            if celery_info:
                comparison["celery_tasks"].append({
                    "task_id": task_id,
                    "state": celery_info.state,
                    "progress": celery_info.progress,
                    "in_our_tracking": task_id in {task.task_id for task in our_tasks}
                })
        
        # Generate recommendations
        if len(celery_task_ids) > len(our_tasks):
            comparison["recommendations"].append(
                f"Celery has {len(celery_task_ids) - len(our_tasks)} more tasks than our tracking. Consider running /monitoring/celery/sync"
            )
        
        pending_tasks = comparison["our_tasks_by_state"].get("pending", [])
        if len(pending_tasks) > 5:
            comparison["recommendations"].append(
                f"You have {len(pending_tasks)} pending tasks. Check if workers are running."
            )
        
        return comparison
        
    finally:
        await monitor.cleanup()


@router.get("/metrics/prometheus")
async def get_prometheus_metrics():
    """Get current Prometheus metrics (if available)."""
    try:
        from prometheus_client import REGISTRY, generate_latest
        
        # Generate metrics in Prometheus format
        metrics_data = generate_latest(REGISTRY)
        
        return {
            "metrics": metrics_data.decode('utf-8'),
            "format": "prometheus",
            "timestamp": datetime.now().isoformat()
        }
    except ImportError:
        raise HTTPException(
            status_code=501,
            detail="Prometheus client not available"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error generating metrics: {str(e)}"
        )


@router.post("/tasks/{task_id}/retry")
async def retry_failed_task(task_id: str, request: Request):
    """Retry a failed task (if supported)."""
    try:
        # Get task details first
        task_info = get_comprehensive_task_info(task_id)
        
        if not task_info or not task_info.get("status"):
            raise HTTPException(
                status_code=404,
                detail="Task not found"
            )
        
        task_state = task_info["status"].get("state")
        
        if task_state != "failed":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot retry task in state '{task_state}'. Only failed tasks can be retried."
            )
        
        # Try to get original task parameters and retry
        # This would require storing the original task parameters
        # For now, return information about how to manually retry
        
        return {
            "message": "Task retry information",
            "task_id": task_id,
            "current_state": task_state,
            "retry_instructions": [
                "To retry this task, you need to resubmit it with the original parameters",
                "Check the start_info for original parameters",
                "Use the /upload endpoint with the same PDF URL and parameters"
            ],
            "original_params": task_info.get("start_info", {}).get("kwargs", {})
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing retry request: {str(e)}"
        )


@router.delete("/tasks/{task_id}")
async def cancel_task(task_id: str):
    """Cancel a pending or running task (if supported)."""
    try:
        # Get task details first
        task_info = get_comprehensive_task_info(task_id)
        
        if not task_info or not task_info.get("status"):
            raise HTTPException(
                status_code=404,
                detail="Task not found"
            )
        
        task_state = task_info["status"].get("state")
        
        if task_state in ["succeeded", "failed"]:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot cancel task in state '{task_state}'. Only pending or running tasks can be cancelled."
            )
        
        # Try to revoke the task using Celery
        try:
            from celery import Celery
            from pdf_parser.settings import get_settings
            
            settings = get_settings()
            celery_app = Celery("pdf_parser")
            celery_app.conf.update(
                broker_url=settings.CELERY_BROKER_URL,
                result_backend=settings.CELERY_RESULT_BACKEND,
                task_serializer="msgpack",
                accept_content=["msgpack", "json"],
                result_serializer="msgpack",
                task_compression="gzip",
            )
            
            # Revoke the task
            celery_app.control.revoke(task_id, terminate=True)
            
            # Update task status to cancelled
            import redis
            redis_client = redis.Redis.from_url(settings.REDIS_URL)
            
            cancel_data = {
                "state": "failed",
                "error": "Task cancelled by user request",
                "cancelled_at": datetime.now().isoformat(),
                "progress": task_info["status"].get("progress", 0)
            }
            
            redis_client.set(
                f"task:{task_id}",
                json.dumps(cancel_data),
                ex=settings.CACHE_TTL
            )
            
            return {
                "message": f"Task {task_id} has been cancelled",
                "task_id": task_id,
                "previous_state": task_state,
                "new_state": "failed",
                "cancelled_at": cancel_data["cancelled_at"]
            }
            
        except Exception as e:
            return {
                "message": f"Task cancellation requested but may not be effective: {str(e)}",
                "task_id": task_id,
                "state": task_state,
                "note": "The task may continue running if the worker has already started processing it"
            }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing cancellation request: {str(e)}"
        )


# Helper function for Celery sync
async def _sync_with_celery(monitor: SystemMonitor) -> Dict[str, Any]:
    """Internal helper to sync with Celery and return report."""
    # Get tasks from both sources
    our_tasks = await monitor.get_all_tasks(limit=200)
    celery_task_ids = await monitor._discover_celery_tasks()
    
    sync_report = {
        "timestamp": datetime.now().isoformat(),
        "our_task_count": len(our_tasks),
        "celery_task_count": len(celery_task_ids),
        "synced_tasks": 0,
        "missing_from_tracking": [],
        "state_differences": [],
        "orphaned_tasks": []
    }
    
    # Find tasks in Celery but not in our tracking
    our_task_ids = {task.task_id for task in our_tasks}
    missing_tasks = celery_task_ids - our_task_ids
    
    for task_id in missing_tasks:
        celery_info = await monitor._get_celery_task_info(task_id)
        if celery_info:
            sync_report["missing_from_tracking"].append({
                "task_id": task_id,
                "celery_state": celery_info.state,
                "progress": celery_info.progress
            })
    
    # Check for state differences in tracked tasks
    for task in our_tasks:
        celery_info = await monitor._get_celery_task_info(task.task_id)
        if celery_info and celery_info.state != task.state:
            sync_report["state_differences"].append({
                "task_id": task.task_id,
                "our_state": task.state,
                "celery_state": celery_info.state,
                "our_progress": task.progress,
                "celery_progress": celery_info.progress
            })
            sync_report["synced_tasks"] += 1
    
    # Find tasks in our tracking but not in Celery (potentially orphaned)
    for task in our_tasks:
        if task.task_id not in celery_task_ids and task.state in ["pending", "running"]:
            sync_report["orphaned_tasks"].append({
                "task_id": task.task_id,
                "state": task.state,
                "age_seconds": (datetime.now() - task.updated_at).total_seconds()
            })
    
    return sync_report 