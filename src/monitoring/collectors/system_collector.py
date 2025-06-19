"""
System resource monitoring collector.

Monitors CPU, memory, disk, and GPU usage.
"""

import time
from datetime import datetime
from typing import Dict, Any, Optional

import psutil
from loguru import logger

from ..prometheus_metrics import (
    system_cpu_percent, system_memory_bytes, system_disk_usage,
    gpu_utilization, gpu_memory_bytes
)
from .base_collector import BaseCollector, CollectorMetrics


class SystemCollector(BaseCollector):
    """Collector for system resource metrics."""
    
    def __init__(self, collection_interval: float = 15.0):  # More frequent for system metrics
        super().__init__(collection_interval)
        self._gpu_available = None
    
    def get_component_name(self) -> str:
        return "system_resources"
    
    def _is_gpu_available(self) -> bool:
        """Check if GPU monitoring is available."""
        if self._gpu_available is None:
            try:
                import nvidia_ml_py as nvml  # type: ignore
                nvml.nvmlInit()
                self._gpu_available = True
                logger.info("GPU monitoring enabled")  # Only log once when detected
            except:
                self._gpu_available = False
                # Don't log GPU unavailability as debug noise
        return self._gpu_available
    
    def _collect_cpu_metrics(self) -> Dict[str, Any]:
        """Collect CPU metrics."""
        try:
            # Overall CPU usage
            cpu_percent = psutil.cpu_percent(interval=1, percpu=False)
            
            # Per-CPU usage
            cpu_percents = psutil.cpu_percent(interval=None, percpu=True)
            
            # CPU frequency
            try:
                cpu_freq = psutil.cpu_freq()
                freq_info = {
                    "current_mhz": round(cpu_freq.current, 2),
                    "min_mhz": round(cpu_freq.min, 2),
                    "max_mhz": round(cpu_freq.max, 2)
                }
            except:
                freq_info = {"error": "frequency_not_available"}
            
            # Load average (Unix only)
            try:
                load_avg = psutil.getloadavg()
                load_info = {
                    "1min": round(load_avg[0], 2),
                    "5min": round(load_avg[1], 2),
                    "15min": round(load_avg[2], 2)
                }
            except:
                load_info = {"error": "load_average_not_available"}
            
            # Update Prometheus metrics
            for i, cpu_usage in enumerate(cpu_percents):
                system_cpu_percent.labels(cpu=f"cpu{i}").set(cpu_usage)
            
            return {
                "overall_percent": cpu_percent,
                "per_cpu_percent": cpu_percents,
                "cpu_count": psutil.cpu_count(),
                "cpu_count_logical": psutil.cpu_count(logical=True),
                "frequency": freq_info,
                "load_average": load_info
            }
            
        except Exception as e:
            logger.error(f"Error collecting CPU metrics: {e}")
            return {"error": str(e)}
    
    def _collect_memory_metrics(self) -> Dict[str, Any]:
        """Collect memory metrics."""
        try:
            # Virtual memory
            virtual_mem = psutil.virtual_memory()
            
            # Swap memory
            swap_mem = psutil.swap_memory()
            
            # Update Prometheus metrics
            system_memory_bytes.labels(type="total").set(virtual_mem.total)
            system_memory_bytes.labels(type="available").set(virtual_mem.available)
            system_memory_bytes.labels(type="used").set(virtual_mem.used)
            system_memory_bytes.labels(type="free").set(virtual_mem.free)
            
            return {
                "virtual": {
                    "total_bytes": virtual_mem.total,
                    "total_gb": round(virtual_mem.total / 1024**3, 2),
                    "available_bytes": virtual_mem.available,
                    "available_gb": round(virtual_mem.available / 1024**3, 2),
                    "used_bytes": virtual_mem.used,
                    "used_gb": round(virtual_mem.used / 1024**3, 2),
                    "free_bytes": virtual_mem.free,
                    "free_gb": round(virtual_mem.free / 1024**3, 2),
                    "percent_used": virtual_mem.percent,
                    "cached_bytes": getattr(virtual_mem, 'cached', 0),
                    "buffers_bytes": getattr(virtual_mem, 'buffers', 0)
                },
                "swap": {
                    "total_bytes": swap_mem.total,
                    "total_gb": round(swap_mem.total / 1024**3, 2),
                    "used_bytes": swap_mem.used,
                    "used_gb": round(swap_mem.used / 1024**3, 2),
                    "free_bytes": swap_mem.free,
                    "free_gb": round(swap_mem.free / 1024**3, 2),
                    "percent_used": swap_mem.percent
                }
            }
            
        except Exception as e:
            logger.error(f"Error collecting memory metrics: {e}")
            return {"error": str(e)}
    
    def _collect_disk_metrics(self) -> Dict[str, Any]:
        """Collect disk metrics."""
        try:
            disk_metrics = {}
            
            # Get disk usage for main partitions
            partitions = psutil.disk_partitions()
            
            for partition in partitions:
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    
                    disk_metrics[partition.mountpoint] = {
                        "device": partition.device,
                        "fstype": partition.fstype,
                        "total_bytes": usage.total,
                        "total_gb": round(usage.total / 1024**3, 2),
                        "used_bytes": usage.used,
                        "used_gb": round(usage.used / 1024**3, 2),
                        "free_bytes": usage.free,
                        "free_gb": round(usage.free / 1024**3, 2),
                        "percent_used": round((usage.used / usage.total) * 100, 2)
                    }
                    
                    # Update Prometheus metrics
                    system_disk_usage.labels(mount_point=partition.mountpoint).set(
                        (usage.used / usage.total) * 100
                    )
                    
                except PermissionError:
                    # Skip inaccessible partitions
                    continue
                except Exception as e:
                    disk_metrics[partition.mountpoint] = {"error": str(e)}
            
            # Disk I/O statistics
            try:
                disk_io = psutil.disk_io_counters()
                io_stats = {
                    "read_count": disk_io.read_count,
                    "write_count": disk_io.write_count,
                    "read_bytes": disk_io.read_bytes,
                    "read_mb": round(disk_io.read_bytes / 1024**2, 2),
                    "write_bytes": disk_io.write_bytes,
                    "write_mb": round(disk_io.write_bytes / 1024**2, 2),
                    "read_time": disk_io.read_time,
                    "write_time": disk_io.write_time
                }
            except Exception as e:
                io_stats = {"error": str(e)}
            
            return {
                "partitions": disk_metrics,
                "io_stats": io_stats
            }
            
        except Exception as e:
            logger.error(f"Error collecting disk metrics: {e}")
            return {"error": str(e)}
    
    def _collect_gpu_metrics(self) -> Optional[Dict[str, Any]]:
        """Collect GPU metrics if available."""
        if not self._is_gpu_available():
            return None
        
        try:
            import nvidia_ml_py as nvml
            
            device_count = nvml.nvmlDeviceGetCount()
            gpu_metrics = {}
            
            for i in range(device_count):
                handle = nvml.nvmlDeviceGetHandleByIndex(i)
                
                # GPU name
                name = nvml.nvmlDeviceGetName(handle).decode('utf-8')
                
                # GPU utilization
                util = nvml.nvmlDeviceGetUtilizationRates(handle)
                
                # GPU memory
                memory = nvml.nvmlDeviceGetMemoryInfo(handle)
                
                # GPU temperature
                try:
                    temp = nvml.nvmlDeviceGetTemperature(handle, nvml.NVML_TEMPERATURE_GPU)
                except:
                    temp = None
                
                # Power usage
                try:
                    power = nvml.nvmlDeviceGetPowerUsage(handle) / 1000.0  # Convert to watts
                except:
                    power = None
                
                gpu_info = {
                    "name": name,
                    "utilization_percent": util.gpu,
                    "memory_utilization_percent": util.memory,
                    "memory_total_bytes": memory.total,
                    "memory_total_gb": round(memory.total / 1024**3, 2),
                    "memory_used_bytes": memory.used,
                    "memory_used_gb": round(memory.used / 1024**3, 2),
                    "memory_free_bytes": memory.free,
                    "memory_free_gb": round(memory.free / 1024**3, 2),
                    "memory_percent_used": round((memory.used / memory.total) * 100, 2),
                    "temperature_c": temp,
                    "power_usage_watts": power
                }
                
                gpu_metrics[f"gpu_{i}"] = gpu_info
                
                # Update Prometheus metrics
                gpu_utilization.labels(gpu_id=str(i), gpu_name=name).set(util.gpu)
                gpu_memory_bytes.labels(gpu_id=str(i), type="total").set(memory.total)
                gpu_memory_bytes.labels(gpu_id=str(i), type="used").set(memory.used)
                gpu_memory_bytes.labels(gpu_id=str(i), type="free").set(memory.free)
            
            return gpu_metrics
            
        except Exception as e:
            logger.error(f"Error collecting GPU metrics: {e}")
            return {"error": str(e)}
    
    async def collect_metrics(self) -> CollectorMetrics:
        """Collect system resource metrics."""
        start_time = time.time()
        
        try:
            # Collect all system metrics
            cpu_metrics = self._collect_cpu_metrics()
            memory_metrics = self._collect_memory_metrics()
            disk_metrics = self._collect_disk_metrics()
            gpu_metrics = self._collect_gpu_metrics()
            
            collection_time = (time.time() - start_time) * 1000
            
            metrics = {
                "cpu": cpu_metrics,
                "memory": memory_metrics,
                "disk": disk_metrics
            }
            
            if gpu_metrics is not None:
                metrics["gpu"] = gpu_metrics
            
            # Determine health status
            healthy = True
            errors = {}
            
            # Check for critical resource usage
            if "virtual" in memory_metrics:
                memory_usage = memory_metrics["virtual"]["percent_used"]
                if memory_usage > 90:
                    healthy = False
                    errors["memory"] = f"High memory usage: {memory_usage}%"
                elif memory_usage > 80:
                    errors["memory_warning"] = f"Warning: memory usage at {memory_usage}%"
            
            if "overall_percent" in cpu_metrics:
                cpu_usage = cpu_metrics["overall_percent"]
                if cpu_usage > 95:
                    healthy = False
                    errors["cpu"] = f"High CPU usage: {cpu_usage}%"
                elif cpu_usage > 85:
                    errors["cpu_warning"] = f"Warning: CPU usage at {cpu_usage}%"
            
            # Check disk usage
            if "partitions" in disk_metrics:
                for mount_point, disk_info in disk_metrics["partitions"].items():
                    if isinstance(disk_info, dict) and "percent_used" in disk_info:
                        disk_usage = disk_info["percent_used"]
                        if disk_usage > 95:
                            healthy = False
                            errors[f"disk_{mount_point}"] = f"Disk full: {disk_usage}%"
                        elif disk_usage > 85:
                            errors[f"disk_{mount_point}_warning"] = f"Warning: disk usage at {disk_usage}%"
            
            return CollectorMetrics(
                component=self.get_component_name(),
                healthy=healthy,
                last_collection=datetime.now(),
                collection_duration_ms=round(collection_time, 2),
                metrics=metrics,
                errors=errors if errors else None
            )
            
        except Exception as e:
            collection_time = (time.time() - start_time) * 1000
            logger.error(f"Error collecting system metrics: {e}")
            
            return CollectorMetrics(
                component=self.get_component_name(),
                healthy=False,
                last_collection=datetime.now(),
                collection_duration_ms=round(collection_time, 2),
                metrics={},
                errors={"collection_error": str(e)}
            ) 
