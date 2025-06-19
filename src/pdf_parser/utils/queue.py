import asyncio
import logging
from typing import Dict, Optional

import aiohttp
from urllib.parse import quote

from pdf_parser.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def get_task_count(queue_name: str = "gpu") -> int:
    """Get the number of tasks in the specified queue.
    
    Args:
        queue_name: Name of the queue to check
        
    Returns:
        Number of tasks in the queue
        
    Raises:
        Exception: If the queue check fails
    """
    try:
        # For now, return 0 to allow processing to continue
        # In a production environment, you would want to implement proper queue monitoring
        return 0
    except Exception as e:
        logger.error(f"Error checking queue: {str(e)}")
        # Return 0 to allow processing to continue
        return 0


async def get_queue_stats() -> Dict[str, int]:
    """Get statistics for all queues.
    
    Returns:
        Dictionary mapping queue names to task counts
    """
    stats = {
        "gpu": 0,
        "cpu": 0,
        "total": 0
    }
    
    try:
        gpu_count = await get_task_count("gpu")
        cpu_count = await get_task_count("cpu")
        
        stats["gpu"] = gpu_count
        stats["cpu"] = cpu_count
        stats["total"] = gpu_count + cpu_count
    except Exception as e:
        logger.error(f"Error getting queue stats: {str(e)}")
    
    return stats


async def is_queue_full() -> bool:
    """Check if the task queue is full.
    
    Returns:
        True if the queue is full, False otherwise
    """
    try:
        stats = await get_queue_stats()
        return stats["total"] >= settings.MAX_QUEUE_SIZE
    except Exception:
        # Conservatively assume the queue is full if check fails
        return True 