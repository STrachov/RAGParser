import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

# Conditionally import unstructured
try:
    from unstructured.partition.pdf import partition_pdf
    from unstructured.partition.html import partition_html
    from unstructured.documents.elements import (
        Title, NarrativeText, Table, ListItem, HeaderText, FooterText
    )
    UNSTRUCTURED_AVAILABLE = True
except ImportError:
    UNSTRUCTURED_AVAILABLE = False
    logging.warning("Unstructured is not available. Install with 'pip install unstructured[pdf]'")

import pandas as pd
from tabulate import tabulate

from pdf_parser.parsers.base import BaseParser
from pdf_parser.settings import get_settings

# Get settings
settings = get_settings()
logger = logging.getLogger(__name__)


class UnstructuredParser(BaseParser):
    """Parser using Unstructured.io for OCR and fallback processing."""
    
    def __init__(self, pdf_path: Path, **kwargs):
        """Initialize the Unstructured parser.
        
        Args:
            pdf_path: Path to the PDF file to parse
            **kwargs: Additional arguments for Unstructured
        """
        super().__init__(pdf_path)
        
        if not UNSTRUCTURED_AVAILABLE:
            raise ImportError(
                "Unstructured is required for this parser but not available. "
                "Install with 'pip install unstructured[pdf]'"
            )
        
        self.strategy = kwargs.get("strategy", "hi_res")
        self.ocr_languages = kwargs.get("ocr_languages", ["eng"])
    
    def parse(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Parse the PDF file using Unstructured.
        
        Returns:
            Tuple containing:
            - Processed document data
            - List of extracted tables
        """
        try:
            # Use Unstructured to partition the PDF
            elements = partition_pdf(
                filename=str(self.pdf_path),
                strategy=self.strategy,
                ocr_languages=self.ocr_languages,
                extract_images_in_pdf=True,
                extract_image_block_types=["Image", "Table"],
                infer_table_structure=True,
                chunking_strategy="by_title"
            )
            
            # Process the elements into our document structure
            return self._process_elements(elements)
            
        except Exception as e:
            logger.exception(f"Error parsing PDF with Unstructured: {str(e)}")
            raise
    
    def _process_elements(self, elements: List[Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Process Unstructured elements into document structure.
        
        Args:
            elements: List of Unstructured elements
            
        Returns:
            Tuple containing:
            - Processed document data
            - List of extracted tables
        """
        # Organize elements by page
        pages = {}
        tables = []
        
        for element in elements:
            page_num = getattr(element, "metadata", {}).get("page_number", 1)
            
            if page_num not in pages:
                pages[page_num] = []
            
            # Process the element based on its type
            if isinstance(element, Table):
                table_data = self._process_table(element, page_num, len(tables))
                tables.append(table_data)
                pages[page_num].append({
                    "type": "table",
                    "table_id": table_data["table_id"]
                })
            else:
                # Process text element
                element_content = self._process_text_element(element)
                if element_content:
                    pages[page_num].append(element_content)
        
        # Create document structure
        document = {
            "metadata": {
                "title": self.pdf_path.stem,
                "pages": len(pages),
            },
            "content": []
        }
        
        # Combine elements on each page into text
        for page_num in sorted(pages.keys()):
            page_content = {
                "page": page_num,
                "text": self._combine_page_elements(pages[page_num]),
                "page_dimensions": {}  # Unstructured doesn't provide this
            }
            document["content"].append(page_content)
        
        return document, tables
    
    def _process_text_element(self, element: Any) -> Dict[str, Any]:
        """Process a text element from Unstructured.
        
        Args:
            element: Unstructured text element
            
        Returns:
            Processed text element
        """
        text = str(element)
        element_type = "text"
        
        # Map element types
        if isinstance(element, Title) or isinstance(element, HeaderText):
            element_type = "section_header" if len(text) < 100 else "paragraph"
        elif isinstance(element, NarrativeText):
            element_type = "paragraph"
        elif isinstance(element, ListItem):
            element_type = "list_item"
        elif isinstance(element, FooterText):
            element_type = "footnote"
        
        return {
            "type": element_type,
            "text": text
        }
    
    def _process_table(self, table_element: Any, page_num: int, table_idx: int) -> Dict[str, Any]:
        """Process a table element from Unstructured.
        
        Args:
            table_element: Unstructured table element
            page_num: Page number
            table_idx: Table index
            
        Returns:
            Processed table data
        """
        # Convert to pandas DataFrame
        df = pd.DataFrame(table_element.metadata.get("text_as_html", table_element.metadata.get("text")))
        
        # Generate markdown and HTML
        try:
            markdown = tabulate(df.values.tolist(), headers=df.columns.tolist(), tablefmt="github")
        except Exception:
            markdown = str(table_element)
        
        # Create table data structure
        table_data = {
            "table_id": table_idx,
            "page": page_num,
            "bbox": [0, 0, 0, 0],  # Unstructured doesn't always provide coordinates
            "rows": len(df),
            "cols": len(df.columns),
            "markdown": markdown,
            "data": {
                "headers": df.columns.tolist(),
                "rows": df.values.tolist()
            }
        }
        
        return table_data
    
    def _combine_page_elements(self, elements: List[Dict[str, Any]]) -> str:
        """Combine elements on a page into a single text string.
        
        Args:
            elements: List of elements on the page
            
        Returns:
            Combined text
        """
        texts = []
        
        for element in elements:
            # Skip tables, they're handled separately
            if element["type"] == "table":
                continue
                
            # Format based on element type
            element_text = element.get("text", "").strip()
            
            if not element_text:
                continue
                
            if element["type"] == "section_header":
                texts.append(f"\n## {element_text}\n")
            elif element["type"] == "paragraph":
                texts.append(f"\n{element_text}\n")
            elif element["type"] == "list_item":
                texts.append(f"- {element_text}")
            elif element["type"] == "footnote":
                texts.append(f"[^{element_text}]")
            else:
                texts.append(element_text)
        
        return "\n".join(texts) 