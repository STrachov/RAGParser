import os
import logging
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import torch
from tabulate import tabulate

# Conditionally import docling
try:
    from docling.document_converter import DocumentConverter
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    DOCLING_AVAILABLE = True
    logging.info("Docling is available")
except ImportError:
    DOCLING_AVAILABLE = False
    logging.warning("Docling is not available. Install with 'pip install docling'")

from pdf_parser.parsers.base import BaseParser
from pdf_parser.settings import get_settings

# Get settings
settings = get_settings()
logger = logging.getLogger(__name__)
# Set logger level to DEBUG for development
logger.setLevel(logging.DEBUG)


class DoclingParser(BaseParser):
    """Parser using Docling for layout-aware PDF parsing."""
    
    def __init__(self, pdf_path: Path, **kwargs):
        """Initialize the Docling parser.
        
        Args:
            pdf_path: Path to the PDF file to parse
            **kwargs: Additional arguments for Docling
        """
        super().__init__(pdf_path)
        
        logger.info(f"Initializing DoclingParser for file: {pdf_path}")
        
        if not DOCLING_AVAILABLE:
            error_msg = "Docling is required for this parser but not available. Install with 'pip install docling'"
            logger.error(error_msg)
            raise ImportError(error_msg)
        
        # Fix for torch.get_default_device compatibility issue
        # Force CPU mode to avoid GPU detection problems
        os.environ["CUDA_VISIBLE_DEVICES"] = ""
        os.environ["TORCH_USE_CUDA_DSA"] = "0"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"  # Prevent online model downloads during inference
        
        logger.info("Forcing CPU mode to avoid torch.get_default_device compatibility issues")
        
        # For multi-threaded CPU acceleration
        os.environ["OMP_NUM_THREADS"] = str(os.cpu_count() or 1)
        logger.debug(f"Using {os.cpu_count() or 1} CPU threads")
        
        # Add a monkey patch for the missing function if needed
        if not hasattr(torch, 'get_default_device'):
            def get_default_device():
                return torch.device('cpu')
            torch.get_default_device = get_default_device
            logger.info("Applied torch.get_default_device monkey patch")
        
        # Initialize the converter with CPU-only configuration
        try:
            # Simple initialization without complex pipeline options
            self.converter = DocumentConverter()
            logger.info("DocumentConverter initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize DocumentConverter: {e}")
            raise
    
    def parse(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Parse the PDF file using Docling.
        
        Returns:
            Tuple containing:
            - Processed document data
            - List of extracted tables
            
        Raises:
            Exception: If parsing fails
        """
        try:
            logger.info(f"Starting to parse PDF: {self.pdf_path}")
            
            # Use Docling to parse the PDF
            logger.debug("Converting document with Docling")
            result = self.converter.convert(str(self.pdf_path))
            
            logger.debug("Successfully converted document with Docling")
            
            # Process the document
            logger.debug("Processing document into internal format")
            processed_result = self._process_document(result.document)
            
            logger.info(f"Successfully processed document with {len(processed_result[1])} tables")
            return processed_result
            
        except Exception as e:
            logger.exception(f"Error parsing PDF with Docling: {str(e)}")
            raise
    
    def _process_document(self, document: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Process Docling document into our format.
        
        Args:
            document: Docling document
            
        Returns:
            Tuple containing:
            - Processed document data
            - List of extracted tables
        """
        # Initialize document structure
        title = getattr(document, "title", None) or self.pdf_path.stem
        page_count = len(document.pages) if hasattr(document, "pages") else 1
        
        logger.debug(f"Processing document: {title} with {page_count} pages")
        
        processed_document = {
            "metadata": {
                "title": title,
                "pages": page_count,
            },
            "content": []
        }
        
        # Get text content
        text_content = document.export_to_text()
        
        # Split content by pages (simple approach)
        page_content = {
            "page": 1,
            "text": text_content,
            "page_dimensions": {
                "width": 612,  # Default PDF width
                "height": 792,  # Default PDF height
            }
        }
        processed_document["content"].append(page_content)
        
        # Extract tables
        tables = []
        if hasattr(document, 'tables') and document.tables:
            logger.debug(f"Extracting {len(document.tables)} tables")
            for i, table in enumerate(document.tables):
                table_data = self._process_table(table, 1, i)
                tables.append(table_data)
        
        logger.debug(f"Completed document processing with {len(tables)} total tables")
        return processed_document, tables
    
    def _process_table(self, table: Any, page_num: int, table_idx: int) -> Dict[str, Any]:
        """Process a table from Docling.
        
        Args:
            table: Docling table object
            page_num: Page number
            table_idx: Table index
            
        Returns:
            Processed table data
        """
        logger.debug(f"Processing table {table_idx} on page {page_num}")
        
        # Extract table data
        table_data = {
            "page": page_num,
            "table_index": table_idx,
            "data": [],
            "markdown": "",
        }
        
        try:
            # Debug: log what methods are available on the table object
            logger.debug(f"Table type: {type(table)}")
            logger.debug(f"Table attributes: {dir(table)}")
            
            # Try different approaches to extract table data
            if hasattr(table, 'export_to_dataframe'):
                logger.debug("Using export_to_dataframe method")
                df = table.export_to_dataframe()
                if not df.empty:
                    table_data["data"] = df.to_dict('records')
                    table_data["markdown"] = df.to_markdown(index=False)
                else:
                    logger.warning(f"Table {table_idx} DataFrame is empty")
                    table_data["data"] = [{"error": "Empty table"}]
                    table_data["markdown"] = "| Empty Table |\n|---|"
                    
            elif hasattr(table, 'export_to_dict'):
                logger.debug("Using export_to_dict method")
                table_dict = table.export_to_dict()
                table_data["data"] = [table_dict] if isinstance(table_dict, dict) else table_dict
                # Convert dict to markdown
                if isinstance(table_dict, dict):
                    table_data["markdown"] = "| Key | Value |\n|---|---|\n" + "\n".join([f"| {k} | {v} |" for k, v in table_dict.items()])
                else:
                    table_data["markdown"] = str(table_dict)
                    
            elif hasattr(table, 'text') or hasattr(table, 'get_text'):
                # Try to get text content
                text_content = getattr(table, 'text', None) or getattr(table, 'get_text', lambda: '')()
                logger.debug(f"Using text content: {text_content[:100]}...")
                if text_content and text_content.strip():
                    # Parse simple table from text (basic approach)
                    lines = [line.strip() for line in text_content.split('\n') if line.strip()]
                    if lines:
                        table_data["data"] = [{"row": i, "content": line} for i, line in enumerate(lines)]
                        table_data["markdown"] = "| Row | Content |\n|---|---|\n" + "\n".join([f"| {i} | {line} |" for i, line in enumerate(lines)])
                    else:
                        table_data["data"] = [{"error": "No table content found"}]
                        table_data["markdown"] = "| No Content |\n|---|"
                else:
                    table_data["data"] = [{"error": "Empty table text"}]
                    table_data["markdown"] = "| Empty Table |\n|---|"
                    
            elif hasattr(table, 'bbox') and hasattr(table, 'text'):
                # Table with bounding box and text
                bbox = getattr(table, 'bbox', None)
                text = getattr(table, 'text', '')
                logger.debug(f"Table with bbox {bbox} and text: {text[:100]}...")
                table_data["data"] = [{"bbox": str(bbox), "text": text}]
                table_data["markdown"] = f"| Table Content |\n|---|\n| {text} |"
                
            else:
                # Last resort: convert to string and parse
                table_str = str(table)
                logger.debug(f"Fallback to string representation: {table_str[:100]}...")
                
                # Check if it's a placeholder text or actual content
                if "download CSV" in table_str or "Table data" in table_str:
                    # This is the problematic placeholder text
                    logger.warning(f"Table {table_idx} contains placeholder text, marking as unavailable")
                    table_data["data"] = [{"error": "Table data extraction not available", "raw": table_str}]
                    table_data["markdown"] = "| Table Extraction Failed |\n|---|\n| Data not available in current parser version |"
                else:
                    # Assume it's actual content
                    table_data["data"] = [{"content": table_str}]
                    table_data["markdown"] = f"| Table Content |\n|---|\n| {table_str} |"
                
        except Exception as e:
            logger.error(f"Error processing table {table_idx}: {e}")
            table_data["data"] = [{"error": f"Table processing failed: {str(e)}"}]
            table_data["markdown"] = f"| Error |\n|---|\n| {str(e)} |"
        
        logger.debug(f"Table {table_idx} processed with {len(table_data['data'])} data entries")
        return table_data