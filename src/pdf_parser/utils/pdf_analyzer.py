#!/usr/bin/env python3
"""
PDF Analyzer for intelligent CPU/GPU task routing.

Analyzes PDF characteristics to determine optimal processing strategy.
"""

import io
import tempfile
from pathlib import Path
from typing import Dict, Literal, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse
import logging

import httpx
import fitz  # PyMuPDF for fast PDF analysis

logger = logging.getLogger(__name__)


@dataclass
class PDFCharacteristics:
    """PDF document characteristics for routing decisions."""
    
    # Basic properties
    page_count: int
    file_size_mb: float
    
    # Content analysis
    text_density: float  # Text characters per page
    image_count: int
    image_density: float  # Images per page
    table_count: int
    
    # Complexity indicators
    has_complex_layout: bool
    has_forms: bool
    has_annotations: bool
    has_embedded_fonts: bool
    
    # OCR requirements
    needs_ocr: bool
    ocr_confidence: float  # 0-1, estimated
    
    # Processing recommendations
    recommended_queue: Literal["cpu", "gpu", "hybrid"]
    confidence: float  # 0-1, confidence in recommendation
    reasoning: str  # Human-readable explanation


def analyze_pdf_from_url(pdf_url: str, max_analysis_pages: int = 5) -> PDFCharacteristics:
    """Analyze PDF from URL to determine processing characteristics."""
    
    logger.info(f"Analyzing PDF from URL: {pdf_url}")
    
    # Download first few pages for analysis
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
        try:
            # Download PDF (or first part for large files)
            _download_pdf_sample(pdf_url, temp_file.name, max_size_mb=10)
            
            # Analyze the sample
            characteristics = analyze_pdf_file(temp_file.name, max_analysis_pages)
            
            logger.info(f"PDF analysis complete: {characteristics.recommended_queue} queue recommended")
            return characteristics
            
        finally:
            # Cleanup
            temp_path = Path(temp_file.name)
            if temp_path.exists():
                temp_path.unlink()


def analyze_pdf_file(pdf_path: str, max_pages: int = 5) -> PDFCharacteristics:
    """Analyze local PDF file to determine processing characteristics."""
    
    try:
        doc = fitz.open(pdf_path)
        
        # Basic properties
        page_count = len(doc)
        file_size_mb = Path(pdf_path).stat().st_size / (1024 * 1024)
        
        # Sample pages for analysis (don't analyze huge documents fully)
        pages_to_analyze = min(max_pages, page_count)
        
        # Initialize counters
        total_text_chars = 0
        total_images = 0
        total_tables = 0
        has_complex_layout = False
        has_forms = False
        has_annotations = False
        has_embedded_fonts = len(doc.getPageFontList(0)) > 0 if page_count > 0 else False
        
        # Analyze sample pages
        for page_num in range(pages_to_analyze):
            page = doc[page_num]
            
            # Text analysis
            text = page.get_text()
            total_text_chars += len(text.strip())
            
            # Image analysis
            image_list = page.get_images()
            total_images += len(image_list)
            
            # Layout complexity detection
            blocks = page.get_text("dict")["blocks"]
            text_blocks = [b for b in blocks if "lines" in b]
            
            # Check for complex layout indicators
            if len(text_blocks) > 10:  # Many text blocks = complex layout
                has_complex_layout = True
            
            # Check for multi-column layout
            if _detect_multi_column_layout(text_blocks):
                has_complex_layout = True
            
            # Forms and annotations
            if page.first_widget:  # Has form fields
                has_forms = True
            
            if page.first_annot:  # Has annotations
                has_annotations = True
            
            # Table detection (basic heuristic)
            tables_on_page = _detect_tables_on_page(page)
            total_tables += tables_on_page
        
        doc.close()
        
        # Calculate metrics
        text_density = total_text_chars / page_count if page_count > 0 else 0
        image_density = total_images / page_count if page_count > 0 else 0
        
        # OCR assessment
        needs_ocr, ocr_confidence = _assess_ocr_needs(total_text_chars, total_images, page_count)
        
        # Make routing recommendation
        recommendation = _make_routing_recommendation(
            page_count=page_count,
            file_size_mb=file_size_mb,
            text_density=text_density,
            image_density=image_density,
            table_count=total_tables,
            has_complex_layout=has_complex_layout,
            needs_ocr=needs_ocr,
            has_forms=has_forms
        )
        
        return PDFCharacteristics(
            page_count=page_count,
            file_size_mb=file_size_mb,
            text_density=text_density,
            image_count=total_images,
            image_density=image_density,
            table_count=total_tables,
            has_complex_layout=has_complex_layout,
            has_forms=has_forms,
            has_annotations=has_annotations,
            has_embedded_fonts=has_embedded_fonts,
            needs_ocr=needs_ocr,
            ocr_confidence=ocr_confidence,
            **recommendation
        )
        
    except Exception as e:
        logger.error(f"Error analyzing PDF: {e}")
        # Fallback to conservative GPU recommendation for unknown PDFs
        return PDFCharacteristics(
            page_count=1,
            file_size_mb=1.0,
            text_density=0,
            image_count=0,
            image_density=0,
            table_count=0,
            has_complex_layout=True,
            has_forms=False,
            has_annotations=False,
            has_embedded_fonts=False,
            needs_ocr=True,
            ocr_confidence=0.5,
            recommended_queue="gpu",
            confidence=0.3,
            reasoning="Analysis failed, using conservative GPU recommendation"
        )


def _download_pdf_sample(pdf_url: str, local_path: str, max_size_mb: int = 10) -> None:
    """Download PDF or sample for analysis."""
    max_bytes = max_size_mb * 1024 * 1024
    
    try:
        with httpx.stream("GET", pdf_url, timeout=30.0) as response:
            response.raise_for_status()
            
            downloaded = 0
            with open(local_path, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    if downloaded + len(chunk) > max_bytes:
                        # Write partial chunk to reach max_bytes
                        remaining = max_bytes - downloaded
                        f.write(chunk[:remaining])
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    
    except Exception as e:
        logger.error(f"Error downloading PDF sample: {e}")
        raise


def _detect_multi_column_layout(text_blocks: list) -> bool:
    """Detect if page has multi-column layout."""
    if len(text_blocks) < 4:
        return False
    
    # Simple heuristic: check if text blocks are arranged in columns
    x_positions = [block["bbox"][0] for block in text_blocks]
    unique_x_positions = len(set(round(x) for x in x_positions))
    
    # If we have multiple distinct X positions, likely multi-column
    return unique_x_positions >= 2


def _detect_tables_on_page(page) -> int:
    """Detect tables on a page (basic heuristic)."""
    # This is a simplified detection - production would use more sophisticated methods
    text = page.get_text()
    
    # Count tab-separated values and aligned text patterns
    lines = text.split('\n')
    table_indicators = 0
    
    for line in lines:
        # Look for table-like patterns
        if '\t' in line and len(line.split('\t')) > 2:
            table_indicators += 1
        elif '|' in line and len(line.split('|')) > 2:
            table_indicators += 1
    
    # Rough estimate: every 10 table-like lines = 1 table
    return max(0, table_indicators // 10)


def _assess_ocr_needs(text_chars: int, images: int, pages: int) -> Tuple[bool, float]:
    """Assess if document needs OCR and confidence level."""
    
    if pages == 0:
        return True, 0.5
    
    # Calculate text density
    text_density = text_chars / pages
    image_density = images / pages
    
    # Heuristics for OCR needs
    if text_density < 100:  # Very little text
        needs_ocr = True
        confidence = 0.8 if image_density > 0.5 else 0.6
    elif text_density < 500:  # Some text but might be image-based
        needs_ocr = image_density > 0.3
        confidence = 0.7
    else:  # Plenty of text, probably extractable
        needs_ocr = False
        confidence = 0.9
    
    return needs_ocr, confidence


def _make_routing_recommendation(
    page_count: int,
    file_size_mb: float,
    text_density: float,
    image_density: float,
    table_count: int,
    has_complex_layout: bool,
    needs_ocr: bool,
    has_forms: bool
) -> Dict[str, any]:
    """Make intelligent routing recommendation based on PDF characteristics."""
    
    reasons = []
    gpu_score = 0
    cpu_score = 0
    
    # Page count influence
    if page_count > 50:
        gpu_score += 2
        reasons.append(f"Large document ({page_count} pages)")
    elif page_count < 5:
        cpu_score += 1
        reasons.append(f"Small document ({page_count} pages)")
    
    # File size influence
    if file_size_mb > 10:
        gpu_score += 2
        reasons.append(f"Large file ({file_size_mb:.1f}MB)")
    elif file_size_mb < 1:
        cpu_score += 1
        reasons.append(f"Small file ({file_size_mb:.1f}MB)")
    
    # Text density influence
    if text_density > 1000:
        cpu_score += 2
        reasons.append("High text density (good for CPU)")
    elif text_density < 200:
        gpu_score += 2
        reasons.append("Low text density (may need OCR)")
    
    # Image density influence
    if image_density > 1:
        gpu_score += 3
        reasons.append(f"Image-heavy document ({image_density:.1f} images/page)")
    elif image_density < 0.1:
        cpu_score += 1
        reasons.append("Minimal images")
    
    # Table influence
    if table_count > 0:
        gpu_score += 2
        reasons.append(f"Contains tables ({table_count})")
    
    # Layout complexity
    if has_complex_layout:
        gpu_score += 2
        reasons.append("Complex layout detected")
    else:
        cpu_score += 1
        reasons.append("Simple layout")
    
    # OCR needs
    if needs_ocr:
        gpu_score += 3
        reasons.append("Requires OCR processing")
    else:
        cpu_score += 2
        reasons.append("Text is extractable")
    
    # Forms
    if has_forms:
        gpu_score += 1
        reasons.append("Contains forms")
    
    # Make decision
    if gpu_score > cpu_score + 2:
        queue = "gpu"
        confidence = min(0.9, 0.5 + (gpu_score - cpu_score) * 0.1)
    elif cpu_score > gpu_score + 1:
        queue = "cpu"
        confidence = min(0.9, 0.5 + (cpu_score - gpu_score) * 0.1)
    else:
        queue = "hybrid"  # Could go either way
        confidence = 0.5
    
    reasoning = "; ".join(reasons[:3])  # Top 3 reasons
    
    return {
        "recommended_queue": queue,
        "confidence": confidence,
        "reasoning": reasoning
    }


def get_optimal_queue_for_pdf(pdf_url: str, fallback_queue: str = "gpu") -> Tuple[str, str]:
    """
    Get optimal queue for a PDF with fallback.
    
    Returns:
        Tuple of (queue_name, reasoning)
    """
    try:
        characteristics = analyze_pdf_from_url(pdf_url)
        
        if characteristics.confidence > 0.7:
            return characteristics.recommended_queue, characteristics.reasoning
        else:
            return fallback_queue, f"Low confidence ({characteristics.confidence:.2f}), using fallback"
            
    except Exception as e:
        logger.warning(f"PDF analysis failed: {e}")
        return fallback_queue, f"Analysis failed: {str(e)}"


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python pdf_analyzer.py <pdf_url_or_path>")
        sys.exit(1)
    
    pdf_input = sys.argv[1]
    
    if pdf_input.startswith(('http://', 'https://')):
        characteristics = analyze_pdf_from_url(pdf_input)
    else:
        characteristics = analyze_pdf_file(pdf_input)
    
    print(f"📄 PDF Analysis Results:")
    print(f"   Pages: {characteristics.page_count}")
    print(f"   Size: {characteristics.file_size_mb:.1f}MB")
    print(f"   Text density: {characteristics.text_density:.0f} chars/page")
    print(f"   Images: {characteristics.image_count} ({characteristics.image_density:.1f}/page)")
    print(f"   Tables: {characteristics.table_count}")
    print(f"   Complex layout: {characteristics.has_complex_layout}")
    print(f"   Needs OCR: {characteristics.needs_ocr}")
    print(f"")
    print(f"🎯 Recommendation: {characteristics.recommended_queue.upper()} queue")
    print(f"   Confidence: {characteristics.confidence:.1%}")
    print(f"   Reasoning: {characteristics.reasoning}")