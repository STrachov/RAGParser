from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional


class BaseParser(ABC):
    """Abstract base class for PDF parsers.
    
    All PDF parser implementations must inherit from this class and implement
    the parse() method.
    """
    
    def __init__(self, pdf_path: Path):
        """Initialize the parser with the path to the PDF file.
        
        Args:
            pdf_path: Path to the PDF file to parse
        """
        self.pdf_path = pdf_path
        
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found at: {self.pdf_path}")
    
    @abstractmethod
    def parse(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Parse the PDF and return structured data.
        
        Returns:
            A tuple containing:
            - The parsed document data as a dictionary
            - A list of extracted tables as dictionaries
        
        Raises:
            Exception: If parsing fails
        """
        pass
    
    def get_name(self) -> str:
        """Get the name of the parser.
        
        Returns:
            The name of the parser implementation
        """
        return self.__class__.__name__ 