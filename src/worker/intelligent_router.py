 #!/usr/bin/env python3
"""
Intelligent task router for CPU/GPU optimization.

Routes PDF parsing tasks to optimal workers based on document characteristics.
"""

import logging
from typing import Dict, Literal, Optional, Tuple
from dataclasses import dataclass
from celery import Celery

from utils.pdf_analyzer import get_optimal_queue_for_pdf, PDFCharacteristics

logger = logging.getLogger(__name__)


@dataclass
class QueueStatus:
    """Status of a processing queue."""
    name: str
    active_tasks: int
    pending_tasks: int
    available_workers: int
    avg_processing_time: float  # seconds
    estimated_wait_time: float  # seconds


class IntelligentRouter:
    """Routes tasks to optimal queues based on content analysis and queue status."""
    
    def __init__(self, celery_app: Celery):
        self.celery_app = celery_app
        self.queue_stats = {}
        
    def route_pdf_task(
        self, 
        pdf_url: str, 
        force_queue: Optional[str] = None,
        enable_analysis: bool = True
    ) -> Tuple[str, str, Dict]:
        """
        Route PDF task to optimal queue.
        
        Args:
            pdf_url: URL of PDF to process
            force_queue: Force specific queue (overrides analysis)
            enable_analysis: Whether to analyze PDF characteristics
            
        Returns:
            Tuple of (queue_name, routing_reason, analysis_data)
        """
        
        analysis_data = {}
        
        try:
            # Check if queue is forced
            if force_queue:
                return force_queue, f"Forced to {force_queue} queue", analysis_data
            
            # Get current queue status
            queue_status = self._get_queue_status()
            
            # If analysis is disabled, use simple load balancing
            if not enable_analysis:
                optimal_queue = self._simple_load_balance(queue_status)
                return optimal_queue, "Load balancing (analysis disabled)", analysis_data
            
            # Analyze PDF characteristics
            logger.info(f"Analyzing PDF for intelligent routing: {pdf_url}")
            
            try:
                from utils.pdf_analyzer import analyze_pdf_from_url
                characteristics = analyze_pdf_from_url(pdf_url, max_analysis_pages=3)
                
                analysis_data = {
                    "page_count": characteristics.page_count,
                    "file_size_mb": characteristics.file_size_mb,
                    "recommended_queue": characteristics.recommended_queue,
                    "confidence": characteristics.confidence,
                    "reasoning": characteristics.reasoning,
                    "needs_ocr": characteristics.needs_ocr,
                    "has_complex_layout": characteristics.has_complex_layout
                }
                
                # Make routing decision considering both analysis and queue status
                final_queue, routing_reason = self._make_final_routing_decision(
                    characteristics, queue_status
                )
                
                logger.info(f"Routing decision: {final_queue} queue - {routing_reason}")
                return final_queue, routing_reason, analysis_data
                
            except Exception as e:
                logger.warning(f"PDF analysis failed: {e}, falling back to load balancing")
                optimal_queue = self._simple_load_balance(queue_status)
                return optimal_queue, f"Analysis failed, load balancing: {str(e)}", analysis_data
                
        except Exception as e:
            logger.error(f"Routing failed: {e}, using default GPU queue")
            return "gpu", f"Routing error: {str(e)}", analysis_data
    
    def _get_queue_status(self) -> Dict[str, QueueStatus]:
        """Get current status of all queues."""
        status = {}
        
        try:
            # Get queue information from Celery
            inspect = self.celery_app.control.inspect()
            
            # Get active tasks
            active = inspect.active() or {}
            
            # Get queue lengths (this is approximate)
            reserved = inspect.reserved() or {}
            
            # Get worker stats
            stats = inspect.stats() or {}
            
            # Process each queue
            for queue_name in ["cpu", "gpu"]:
                active_count = sum(
                    len([task for task in tasks if task.get("routing_key", "").endswith(queue_name)])
                    for tasks in active.values()
                )
                
                pending_count = sum(
                    len([task for task in tasks if task.get("routing_key", "").endswith(queue_name)])
                    for tasks in reserved.values()
                )
                
                # Count available workers for this queue
                available_workers = len([
                    worker for worker, worker_stats in stats.items()
                    if queue_name in worker_stats.get("pool", {}).get("processes", [])
                ])
                
                # Estimate processing times (these would be better as historical averages)
                avg_processing_time = 45.0 if queue_name == "gpu" else 120.0  # seconds
                
                # Calculate estimated wait time
                if available_workers > 0:
                    estimated_wait = (active_count + pending_count) * avg_processing_time / available_workers
                else:
                    estimated_wait = float('inf')
                
                status[queue_name] = QueueStatus(
                    name=queue_name,
                    active_tasks=active_count,
                    pending_tasks=pending_count,
                    available_workers=available_workers,
                    avg_processing_time=avg_processing_time,
                    estimated_wait_time=estimated_wait
                )
                
        except Exception as e:
            logger.warning(f"Could not get accurate queue status: {e}")
            # Fallback status
            status = {
                "cpu": QueueStatus("cpu", 0, 0, 1, 120.0, 0.0),
                "gpu": QueueStatus("gpu", 0, 0, 1, 45.0, 0.0)
            }
        
        return status
    
    def _simple_load_balance(self, queue_status: Dict[str, QueueStatus]) -> str:
        """Simple load balancing when analysis is not available."""
        
        # Choose queue with shortest estimated wait time
        if not queue_status:
            return "gpu"  # Default fallback
        
        optimal_queue = min(
            queue_status.keys(),
            key=lambda q: queue_status[q].estimated_wait_time
        )
        
        return optimal_queue
    
    def _make_final_routing_decision(
        self, 
        characteristics: PDFCharacteristics, 
        queue_status: Dict[str, QueueStatus]
    ) -> Tuple[str, str]:
        """
        Make final routing decision considering both analysis and queue status.
        """
        
        recommended = characteristics.recommended_queue
        confidence = characteristics.confidence
        
        # High confidence recommendations
        if confidence > 0.8:
            target_queue = recommended if recommended != "hybrid" else "gpu"
            
            # Check if target queue is available and not overloaded
            if target_queue in queue_status:
                queue_info = queue_status[target_queue]
                
                # If queue is severely overloaded, consider alternative
                if queue_info.estimated_wait_time > 300:  # 5 minutes
                    alternative = "cpu" if target_queue == "gpu" else "gpu"
                    if alternative in queue_status and queue_status[alternative].estimated_wait_time < queue_info.estimated_wait_time / 2:
                        return alternative, f"High confidence {target_queue} but overloaded, using {alternative}"
                
                return target_queue, f"High confidence recommendation: {characteristics.reasoning}"
        
        # Medium confidence - consider queue load more heavily
        elif confidence > 0.5:
            target_queue = recommended if recommended != "hybrid" else "gpu"
            
            if target_queue in queue_status:
                queue_info = queue_status[target_queue]
                
                # More aggressive load balancing for medium confidence
                if queue_info.estimated_wait_time > 120:  # 2 minutes
                    alternative = "cpu" if target_queue == "gpu" else "gpu"
                    if alternative in queue_status and queue_status[alternative].estimated_wait_time < queue_info.estimated_wait_time:
                        return alternative, f"Medium confidence {target_queue} but load balancing to {alternative}"
                
                return target_queue, f"Medium confidence: {characteristics.reasoning}"
        
        # Low confidence - use load balancing
        else:
            optimal_queue = self._simple_load_balance(queue_status)
            return optimal_queue, f"Low confidence analysis, load balancing to {optimal_queue}"
    
    def get_routing_stats(self) -> Dict:
        """Get routing statistics for monitoring."""
        queue_status = self._get_queue_status()
        
        return {
            "queues": {
                name: {
                    "active_tasks": status.active_tasks,
                    "pending_tasks": status.pending_tasks,
                    "available_workers": status.available_workers,
                    "estimated_wait_minutes": status.estimated_wait_time / 60
                }
                for name, status in queue_status.items()
            },
            "recommendations": {
                "cpu_suitable_for": "Simple text documents, small files, high text density",
                "gpu_suitable_for": "Complex layouts, images, tables, OCR needed, large files"
            }
        }


# Global router instance
_router_instance = None

def get_router(celery_app: Celery) -> IntelligentRouter:
    """Get global router instance."""
    global _router_instance
    if _router_instance is None:
        _router_instance = IntelligentRouter(celery_app)
    return _router_instance


if __name__ == "__main__":
    # Test the router
    from worker.tasks import app
    
    router = IntelligentRouter(app)
    
    # Test with a sample PDF
    test_url = "https://arxiv.org/pdf/2301.07041.pdf"
    queue, reason, analysis = router.route_pdf_task(test_url)
    
    print(f"Routing result for {test_url}:")
    print(f"Queue: {queue}")
    print(f"Reason: {reason}")
    print(f"Analysis: {analysis}")
    
    # Show routing stats
    stats = router.get_routing_stats()
    print(f"\nCurrent routing stats: {stats}")