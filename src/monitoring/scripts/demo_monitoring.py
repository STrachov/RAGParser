#!/usr/bin/env python3
"""
RAGParser Monitoring System Demo

This script demonstrates how to use the comprehensive monitoring system.
"""

import asyncio
from datetime import datetime

from monitoring import (
    setup_collectors, health_checker, collector_manager,
    get_comprehensive_status, record_task_start, record_task_completion
)
from monitoring.collectors import SystemCollector, CacheCollector


async def demo_health_checks():
    """Demonstrate health check capabilities."""
    print("🏥 Health Check Demo")
    print("=" * 50)
    
    # Individual component health checks
    print("\n📊 Individual Component Health:")
    
    redis_health = await health_checker.check_redis_health()
    print(f"Redis: {'✅ Healthy' if redis_health.healthy else '❌ Unhealthy'}")
    print(f"  Response time: {redis_health.response_time_ms}ms")
    print(f"  Memory used: {redis_health.details.get('memory_used_mb', 'N/A')}MB")
    
    rabbitmq_health = await health_checker.check_rabbitmq_health()
    print(f"RabbitMQ: {'✅ Healthy' if rabbitmq_health.healthy else '❌ Unhealthy'}")
    print(f"  Active workers: {rabbitmq_health.details.get('active_workers', 'N/A')}")
    
    s3_health = await health_checker.check_s3_health()
    print(f"S3 Storage: {'✅ Healthy' if s3_health.healthy else '❌ Unhealthy'}")
    print(f"  Response time: {s3_health.response_time_ms}ms")
    
    # System overview
    print("\n🌐 System Overview:")
    system_health = await health_checker.get_system_health()
    print(f"Overall Status: {'✅ HEALTHY' if system_health.overall_healthy else '❌ UNHEALTHY'}")
    print(f"Check Time: {system_health.checked_at}")
    
    if system_health.critical_issues:
        print(f"Critical Issues: {', '.join(system_health.critical_issues)}")


async def demo_collectors():
    """Demonstrate metric collectors."""
    print("\n🔧 Metric Collectors Demo")
    print("=" * 50)
    
    # Initialize collectors manually (normally done in setup_collectors())
    system_collector = SystemCollector(collection_interval=5.0)
    cache_collector = CacheCollector(collection_interval=10.0)
    
    print("\n📈 Collecting System Metrics:")
    system_metrics = await system_collector.collect_metrics()
    print(f"Collection took: {system_metrics.collection_duration_ms}ms")
    print(f"CPU usage: {system_metrics.metrics.get('cpu', {}).get('overall_percent', 'N/A')}%")
    print(f"Memory usage: {system_metrics.metrics.get('memory', {}).get('virtual', {}).get('percent_used', 'N/A')}%")
    
    print("\n💾 Collecting Cache Metrics:")
    cache_metrics = await cache_collector.collect_metrics()
    print(f"Collection took: {cache_metrics.collection_duration_ms}ms")
    print(f"Cache healthy: {cache_metrics.healthy}")
    connectivity = cache_metrics.metrics.get('connectivity', {})
    print(f"Ping time: {connectivity.get('ping_time_ms', 'N/A')}ms")
    
    # Clean up
    await system_collector.stop_collection()
    await cache_collector.stop_collection()


async def demo_metrics_recording():
    """Demonstrate manual metrics recording."""
    print("\n📊 Metrics Recording Demo")
    print("=" * 50)
    
    # Simulate task processing
    task_name = "parse_pdf"
    parser_type = "docling"
    queue = "gpu_queue"
    
    print(f"\n🚀 Recording task start: {task_name}")
    record_task_start(task_name, parser_type, queue)
    
    # Simulate processing time
    await asyncio.sleep(1)
    
    print(f"✅ Recording task completion")
    record_task_completion(task_name, parser_type, queue, 1.0, "success")
    
    print("Metrics recorded successfully!")


async def demo_comprehensive_monitoring():
    """Demonstrate comprehensive monitoring status."""
    print("\n🔍 Comprehensive Monitoring Demo")
    print("=" * 50)
    
    # This would normally be called after setup_collectors()
    try:
        status = await get_comprehensive_status()
        
        print(f"\n📊 System Status:")
        print(f"Metrics available: {status['metrics_available']}")
        
        health = status['health']
        print(f"Overall healthy: {health.overall_healthy}")
        print(f"Components checked: {len(health.components)}")
        
        for component_name, component_health in health.components.items():
            print(f"  {component_name}: {'✅' if component_health.healthy else '❌'}")
        
    except Exception as e:
        print(f"Error getting comprehensive status: {e}")


async def main():
    """Main demo function."""
    print("🚀 RAGParser Monitoring System Demo")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    
    try:
        # Run individual demos
        await demo_health_checks()
        await demo_collectors()
        await demo_metrics_recording()
        await demo_comprehensive_monitoring()
        
        print("\n" + "=" * 60)
        print("✅ All demos completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\nFinished at: {datetime.now()}")


if __name__ == "__main__":
    # Run the demo
    asyncio.run(main()) 