import os
import json
import hashlib
import tiktoken
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple

import logging
from langchain.text_splitter import RecursiveCharacterTextSplitter


logger = logging.getLogger(__name__)


class TextSplitter:
    """
    Text splitter that chunks document content into smaller pieces for processing.
    
    Based on the RAG-Challenge-2 text splitter by IlyaRice.
    """
    
    def __init__(self, chunk_size: int = 300, chunk_overlap: int = 50):
        """Initialize the text splitter.
        
        Args:
            chunk_size: Target size of each chunk in tokens
            chunk_overlap: Number of tokens to overlap between chunks
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    def count_tokens(self, text: str, encoding_name: str = "cl100k_base") -> int:
        """Count the number of tokens in a string.
        
        Args:
            text: The text to count tokens in
            encoding_name: The name of the tokenizer to use
            
        Returns:
            The number of tokens in the text
        """
        try:
            encoding = tiktoken.get_encoding(encoding_name)
            tokens = encoding.encode(text)
            return len(tokens)
        except Exception as e:
            logger.warning(f"Failed to use tiktoken encoding {encoding_name}: {e}, using character approximation")
            # Fallback to character count approximation (roughly 4 chars per token)
            return len(text) // 4

    def split(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Split a document into chunks.
        
        Args:
            document: Document data with content to split
            
        Returns:
            Document with chunked content
        """
        chunks = []
        chunk_id = 0
        
        # Process each page in the document
        for page in document.get("content", []):
            page_chunks = self._split_page(page)
            for chunk in page_chunks:
                chunk["id"] = chunk_id
                chunk["type"] = "content"
                chunk_id += 1
                chunks.append(chunk)
        
        # Replace content with chunks
        result = document.copy()
        result["chunks"] = chunks
        return result

    def _split_page(self, page: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Split a page into chunks.
        
        Args:
            page: Page data with text content
            
        Returns:
            List of chunks
        """
        # Create a text splitter from langchain with tiktoken encoding
        # Use gpt-4 instead of gpt-4o for better tiktoken compatibility
        try:
            text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
                model_name="gpt-4",
                chunk_size=self.chunk_size,
                chunk_overlap=self.chunk_overlap,
                separators=["\n\n", "\n", " ", ""]
            )
        except Exception as e:
            logger.warning(f"Failed to create tiktoken-based splitter: {e}, falling back to character-based splitter")
            # Fallback to character-based splitting
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=self.chunk_size * 4,  # Approximate 4 chars per token
                chunk_overlap=self.chunk_overlap * 4,
                separators=["\n\n", "\n", " ", ""]
            )
        
        # Get the text from the page
        page_text = page.get("text", "")
        if not page_text.strip():
            return []
        
        # Split the text and create chunks with metadata
        text_chunks = text_splitter.split_text(page_text)
        chunks_with_meta = []
        
        for chunk_text in text_chunks:
            # Skip empty chunks
            if not chunk_text.strip():
                continue
                
            chunks_with_meta.append({
                "page": page.get("page", 1),
                "length_tokens": self.count_tokens(chunk_text),
                "text": chunk_text
            })
        
        return chunks_with_meta

    @staticmethod
    def _extract_tables_from_chunk(chunk_text: str) -> Tuple[str, List[Dict[str, Any]]]:
        """Extract tables from chunk text (if any).
        
        Args:
            chunk_text: Text that might contain tables
            
        Returns:
            Tuple containing:
            - Text with tables removed
            - List of extracted tables
        """
        # This is a placeholder for more advanced table extraction
        # In a real implementation, we would parse markdown tables here
        return chunk_text, [] 


def main():
    """CLI entrypoint for the text splitter utility."""
    parser = argparse.ArgumentParser(description="Split JSON/PDF files into chunks for processing")
    parser.add_argument("input", type=str, help="Input file path (JSON or directory)")
    parser.add_argument("--output", "-o", type=str, help="Output directory or file path")
    parser.add_argument("--chunk-size", type=int, default=300, help="Chunk size in tokens")
    parser.add_argument("--chunk-overlap", type=int, default=50, help="Chunk overlap in tokens")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")
    
    # Initialize the splitter
    splitter = TextSplitter(chunk_size=args.chunk_size, chunk_overlap=args.chunk_overlap)
    
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input path does not exist: {input_path}")
        return 1
    
    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        if input_path.is_dir():
            output_path = input_path / "chunked"
        else:
            output_path = input_path.with_stem(f"{input_path.stem}_chunked")
    
    # Process files
    if input_path.is_dir():
        # Process all JSON files in the directory
        output_path.mkdir(exist_ok=True)
        
        count = 0
        for json_file in input_path.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    document = json.load(f)
                
                chunked_document = splitter.split(document)
                
                with open(output_path / json_file.name, "w", encoding="utf-8") as f:
                    json.dump(chunked_document, f, indent=2, ensure_ascii=False)
                
                count += 1
                logger.info(f"Processed {json_file.name}")
            except Exception as e:
                logger.error(f"Error processing {json_file.name}: {e}")
        
        logger.info(f"Processed {count} files")
    else:
        # Process a single file
        try:
            with open(input_path, "r", encoding="utf-8") as f:
                document = json.load(f)
            
            chunked_document = splitter.split(document)
            
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(chunked_document, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Processed {input_path} → {output_path}")
        except Exception as e:
            logger.error(f"Error processing {input_path}: {e}")
            return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main()) 