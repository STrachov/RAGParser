from pathlib import Path
from typing import Type, Dict, Literal, List, Optional
import logging

from pdf_parser.parsers.base import BaseParser

# Conditionally import parsers
try:
    from pdf_parser.parsers.docling_parser import DoclingParser
    DOCLING_AVAILABLE = True
except ImportError:
    DOCLING_AVAILABLE = False
    logging.warning("Docling is not available. Install with 'pip install docling'")

try:
    from pdf_parser.parsers.marker_parser import MarkerParser
    MARKER_AVAILABLE = True
except ImportError:
    MARKER_AVAILABLE = False
    logging.warning("Marker is not available. Install with 'pip install marker'")

try:
    from pdf_parser.parsers.unstructured_parser import UnstructuredParser
    UNSTRUCTURED_AVAILABLE = True
except ImportError:
    UNSTRUCTURED_AVAILABLE = False
    logging.warning("Unstructured is not available. Install with 'pip install unstructured[pdf]'")


class ParserFactory:
    """Factory for creating PDF parsers based on the requested backend."""
    
    @classmethod
    def _get_parsers(cls) -> Dict[str, Type[BaseParser]]:
        """Get dictionary of available parsers."""
        parsers = {}
        
        # Add available parsers
        if DOCLING_AVAILABLE:
            parsers["docling"] = DoclingParser
        
        if MARKER_AVAILABLE:
            parsers["marker"] = MarkerParser
            
        if UNSTRUCTURED_AVAILABLE:
            parsers["unstructured"] = UnstructuredParser
            
        if not parsers:
            logging.warning("No parsers are available. Install at least one of: docling, marker, unstructured")
            
        return parsers
    
    @classmethod
    def create_parser(
        cls, 
        parser_type: Literal["docling", "marker", "unstructured"],
        pdf_path: Path,
        **kwargs
    ) -> BaseParser:
        """Create a parser instance of the requested type.
        
        Args:
            parser_type: Type of parser to create
            pdf_path: Path to the PDF file to parse
            **kwargs: Additional arguments to pass to the parser
            
        Returns:
            An instance of the requested parser
            
        Raises:
            ValueError: If the requested parser type is not supported
        """
        parsers = cls._get_parsers()
        
        if not parsers:
            raise ImportError(
                "No parsers are available. Install at least one of: "
                "docling, marker, unstructured[pdf]"
            )
        
        if parser_type not in parsers:
            valid_types = ", ".join(parsers.keys())
            raise ValueError(
                f"Unsupported parser type: {parser_type}. Valid types are: {valid_types}"
            )
        
        parser_cls = parsers[parser_type]
        return parser_cls(pdf_path, **kwargs)
    
    @classmethod
    def auto_select_parser(cls, pdf_path: Path, **kwargs) -> BaseParser:
        """Automatically select the best parser for the given PDF.
        
        We use a simple heuristic:
        1. First try Docling for general PDFs with layout
        2. If the PDF has scientific content, use Marker (if available)
        3. Fall back to Unstructured for OCR if the PDF is mostly images
        
        Args:
            pdf_path: Path to the PDF file to parse
            **kwargs: Additional arguments to pass to the parser
            
        Returns:
            The best parser for the given PDF
            
        Raises:
            ImportError: If no parsers are available
        """
        # Get available parsers
        parsers = cls._get_parsers()
        
        if not parsers:
            raise ImportError(
                "No parsers are available. Install at least one of: "
                "docling, marker, unstructured[pdf]"
            )
        
        # Try to use docling (preferred for general PDFs)
        if "docling" in parsers:
            return cls.create_parser("docling", pdf_path, **kwargs)
        
        # Fall back to unstructured if available
        if "unstructured" in parsers:
            return cls.create_parser("unstructured", pdf_path, **kwargs)
        
        # Last resort - use any available parser
        first_available = next(iter(parsers.keys()))
        return cls.create_parser(first_available, pdf_path, **kwargs) 