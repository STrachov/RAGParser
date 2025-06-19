import pytest
from pdf_parser.utils.splitter import TextSplitter


@pytest.fixture
def sample_document():
    """Sample document for testing."""
    return {
        "metadata": {
            "title": "Test Document",
            "pages": 2
        },
        "content": [
            {
                "page": 1,
                "text": "This is the first page of the test document. It contains some sample text that will be split into chunks."
            },
            {
                "page": 2,
                "text": """This is the second page of the document. It contains more text to demonstrate splitting.
                
                The text splitter should handle multiple paragraphs and preserve the structure while still creating
                appropriate chunks based on the token limits. This helps ensure that the content is properly
                processed for further analysis or embedding."""
            }
        ]
    }


def test_text_splitter_initialization():
    """Test that TextSplitter initializes with default and custom parameters."""
    # Default initialization
    splitter = TextSplitter()
    assert splitter.chunk_size == 300
    assert splitter.chunk_overlap == 50
    
    # Custom initialization
    custom_splitter = TextSplitter(chunk_size=500, chunk_overlap=100)
    assert custom_splitter.chunk_size == 500
    assert custom_splitter.chunk_overlap == 100


def test_count_tokens():
    """Test token counting functionality."""
    splitter = TextSplitter()
    text = "This is a test sentence."
    token_count = splitter.count_tokens(text)
    assert token_count > 0
    
    # Longer text should have more tokens
    longer_text = "This is a much longer sentence with more words and should therefore have more tokens when counted."
    longer_token_count = splitter.count_tokens(longer_text)
    assert longer_token_count > token_count


def test_split_document(sample_document):
    """Test that a document is properly split into chunks."""
    splitter = TextSplitter(chunk_size=50, chunk_overlap=10)
    result = splitter.split(sample_document)
    
    # Check that the document structure is preserved
    assert "metadata" in result
    assert "chunks" in result
    
    # Check that chunks were created
    assert len(result["chunks"]) > 0
    
    # Check that each chunk has the required fields
    for chunk in result["chunks"]:
        assert "id" in chunk
        assert "type" in chunk
        assert "page" in chunk
        assert "text" in chunk
        assert "length_tokens" in chunk
        
    # Check that chunks have appropriate token lengths
    for chunk in result["chunks"]:
        assert chunk["length_tokens"] <= splitter.chunk_size


def test_empty_document():
    """Test handling of empty documents."""
    splitter = TextSplitter()
    empty_doc = {"metadata": {"title": "Empty"}, "content": []}
    result = splitter.split(empty_doc)
    
    assert "chunks" in result
    assert len(result["chunks"]) == 0 