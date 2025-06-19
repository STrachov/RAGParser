import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

# Conditionally import marker
try:
    import marker
    from marker.convert import convert_pdf_to_document
    MARKER_AVAILABLE = True
except ImportError:
    MARKER_AVAILABLE = False
    logging.warning("Marker is not available. Install with 'pip install marker'")

from tabulate import tabulate

from pdf_parser.parsers.base import BaseParser
from pdf_parser.settings import get_settings

# Get settings
settings = get_settings()
logger = logging.getLogger(__name__)


class MarkerParser(BaseParser):
    """Parser using Marker for scientific document processing."""
    
    def __init__(self, pdf_path: Path, **kwargs):
        """Initialize the Marker parser.
        
        Args:
            pdf_path: Path to the PDF file to parse
            **kwargs: Additional arguments for Marker
        """
        super().__init__(pdf_path)
        
        if not MARKER_AVAILABLE:
            raise ImportError(
                "Marker is required for this parser but not available. "
                "Install with 'pip install marker'"
            )
        
        self.model = kwargs.get("model", "marker-content")
    
    def parse(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Parse the PDF file using Marker.
        
        Returns:
            Tuple containing:
            - Processed document data
            - List of extracted tables
            
        Raises:
            Exception: If parsing fails
        """
        try:
            # Use Marker to parse the PDF
            document = convert_pdf_to_document(
                self.pdf_path,
                model=self.model,
            )
            
            # Process the document into our structure
            return self._process_document(document)
            
        except Exception as e:
            logger.exception(f"Error parsing PDF with Marker: {str(e)}")
            raise
    
    def _process_document(self, document: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Process Marker document into our structure.
        
        Args:
            document: Marker document
            
        Returns:
            Tuple containing:
            - Processed document data
            - List of extracted tables
        """
        # Initialize document structure
        processed_document = {
            "metadata": {
                "title": document.title if hasattr(document, "title") else self.pdf_path.stem,
                "pages": len(document.pages) if hasattr(document, "pages") else 0,
            },
            "content": []
        }
        
        # Process pages
        tables = []
        
        for i, page in enumerate(document.pages):
            page_content = {
                "page": i + 1,
                "text": self._process_page_content(page),
                "page_dimensions": {
                    "width": page.width if hasattr(page, "width") else 0,
                    "height": page.height if hasattr(page, "height") else 0,
                }
            }
            processed_document["content"].append(page_content)
            
            # Process tables on page
            page_tables = self._extract_tables_from_page(page, i + 1)
            tables.extend(page_tables)
        
        return processed_document, tables
    
    def _process_page_content(self, page: Any) -> str:
        """Process page content into a formatted string.
        
        Args:
            page: Marker page
            
        Returns:
            Formatted page text
        """
        sections = []
        
        # Process sections
        for element in page.elements:
            element_type = getattr(element, "type", "text")
            text = getattr(element, "text", "").strip()
            
            if not text:
                continue
            
            # Format based on element type
            if element_type == "heading":
                level = getattr(element, "level", 1)
                prefix = "#" * min(level, 6)
                sections.append(f"\n{prefix} {text}\n")
            elif element_type == "paragraph":
                sections.append(f"\n{text}\n")
            elif element_type == "list_item":
                sections.append(f"- {text}")
            elif element_type == "footnote" or element_type == "citation":
                sections.append(f"[^{text}]")
            else:
                sections.append(text)
        
        return "\n".join(sections)
    
    def _extract_tables_from_page(self, page: Any, page_num: int) -> List[Dict[str, Any]]:
        """Extract tables from a page.
        
        Args:
            page: Marker page
            page_num: Page number
            
        Returns:
            List of extracted tables
        """
        tables = []
        
        # Marker doesn't have a standard table extraction API
        # This is a simplified implementation
        for i, element in enumerate(page.elements):
            if getattr(element, "type", "") == "table":
                table_data = self._process_table(element, page_num, len(tables))
                tables.append(table_data)
        
        return tables
    
    def _process_table(self, table_element: Any, page_num: int, table_idx: int) -> Dict[str, Any]:
        """Process a table element.
        
        Args:
            table_element: Table element
            page_num: Page number
            table_idx: Table index
            
        Returns:
            Processed table data
        """
        # Access table data
        rows = getattr(table_element, "rows", [])
        table_data = []
        
        # Extract headers and data
        headers = []
        if rows and len(rows) > 0:
            headers = [cell.text for cell in rows[0].cells]
            for row in rows[1:]:
                table_data.append([cell.text for cell in row.cells])
        
        # Generate markdown
        markdown = tabulate(table_data, headers=headers, tablefmt="github")
        
        # Create table data structure
        return {
            "table_id": table_idx,
            "page": page_num,
            "bbox": [
                getattr(table_element, "x", 0),
                getattr(table_element, "y", 0),
                getattr(table_element, "x", 0) + getattr(table_element, "width", 0),
                getattr(table_element, "y", 0) + getattr(table_element, "height", 0),
            ],
            "rows": len(rows) - 1 if rows else 0,
            "cols": len(headers),
            "markdown": markdown,
            "data": {
                "headers": headers,
                "rows": table_data
            }
        } 