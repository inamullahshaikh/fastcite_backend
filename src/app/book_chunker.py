import fitz  # PyMuPDF
from typing import List, Dict, Optional, Tuple, Any
import json
from pathlib import Path
import uuid
import os


class BookChunker:
    def __init__(self, pdf_path: str, book_id: str, output_dir: Optional[str] = None):
        """
        Initialize the BookChunker with a PDF file.
        
        Args:
            pdf_path: Path to the PDF file
            book_id: Book ID to use for mini PDFs (instead of generating one)
            output_dir: Directory to save mini PDFs (default: "pdfs" in current directory)
        """
        self.pdf_path = pdf_path
        self.doc = fitz.open(pdf_path)
        self.toc = self.doc.get_toc()
        self.chunks: List[Dict] = []
        # Use provided book_id instead of generating one
        self.bookid = book_id
        # Set output directory for mini PDFs
        if output_dir is None:
            # Default to "pdfs" directory in current working directory
            base_dir = os.getcwd()
        else:
            base_dir = os.path.abspath(output_dir)
        # Use "pdfs" directory (matching existing pipeline)
        self.output_dir = os.path.join(base_dir, "pdfs")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Common front matter titles (case-insensitive matching)
        self.front_matter_keywords = [
            'cover', 'title page', 'copyright', 'dedication', 'preface', 
            'foreword', 'acknowledgment', 'acknowledgement', 'table of contents',
            'contents', 'list of figures', 'list of tables', 'introduction',
            'prologue', 'epigraph'
        ]
        
        # Common back matter titles (case-insensitive matching)
        self.back_matter_keywords = [
            'appendix', 'appendices', 'bibliography', 'references', 'reference',
            'index', 'glossary', 'notes', 'credits', 'about the author',
            'about the authors', 'colophon', 'epilogue', 'afterword'
        ]
        
    def get_page_text(self, page_num: int) -> str:
        """Extract text from a specific page."""
        page = self.doc[page_num]
        return page.get_text()
    
    def find_heading_on_page(self, heading_text: str, page_num: int) -> Optional[Tuple[int, int]]:
        """
        Find if a heading appears on a page and return its position.
        
        Args:
            heading_text: The heading text to search for
            page_num: Page number to search (0-indexed)
            
        Returns:
            Tuple of (start_char, end_char) if found, None otherwise
        """
        page = self.doc[page_num]
        text = page.get_text()
        
        # Try to find the heading in the text (case-insensitive, flexible matching)
        heading_clean = heading_text.strip().lower()
        text_lower = text.lower()
        
        # Try exact match first
        idx = text_lower.find(heading_clean)
        if idx != -1:
            return (idx, idx + len(heading_text))
        
        # Try matching just the first few words (in case of formatting differences)
        heading_words = heading_clean.split()[:3]  # First 3 words
        if len(heading_words) > 0:
            search_text = ' '.join(heading_words)
            idx = text_lower.find(search_text)
            if idx != -1:
                # Find the end of the heading (usually ends with newline or number)
                end_idx = idx + len(search_text)
                # Try to extend to end of line
                while end_idx < len(text) and text[end_idx] not in ['\n', '\r']:
                    end_idx += 1
                return (idx, end_idx)
        
        return None
    
    def extract_text_between_pages(self, start_page: int, end_page: int, 
                                   start_offset: Optional[int] = None,
                                   end_offset: Optional[int] = None) -> str:
        """
        Extract text between two pages, with optional character offsets.
        
        Args:
            start_page: Starting page (0-indexed)
            end_page: Ending page (0-indexed, inclusive)
            start_offset: Character offset on start page (None = start of page)
            end_offset: Character offset on end page (None = end of page)
            
        Returns:
            Extracted text
        """
        text_parts = []
        
        for page_num in range(start_page, end_page + 1):
            page_text = self.get_page_text(page_num)
            
            if page_num == start_page and start_offset is not None:
                page_text = page_text[start_offset:]
            if page_num == end_page and end_offset is not None:
                page_text = page_text[:end_offset]
            
            text_parts.append(page_text)
        
        return '\n'.join(text_parts)
    
    def process_chunks(self) -> List[Dict]:
        """
        Process the book and create chunks based on TOC.
        
        Handles:
        - Multiple headings on the same page
        - Headings in the middle of pages (text above belongs to previous chunk)
        - Proper text boundaries between sections
        
        Returns:
            List of chunk dictionaries
        """
        if not self.toc:
            raise ValueError("No table of contents found in the PDF")
        
        chunks = []
        num_pages = len(self.doc)
        
        # First pass: determine boundaries for each chunk
        chunk_boundaries = []
        
        for i, toc_entry in enumerate(self.toc):
            level, title, page = toc_entry
            toc_page = max(0, page - 1)
            
            # Find where this heading appears
            heading_pos = self.find_heading_on_page(title, toc_page)
            
            # Determine where this chunk should start
            if i == 0:
                # First chunk starts from beginning of document
                chunk_start_page = 0
                chunk_start_offset = None
            else:
                # Start from where previous heading ended
                prev_toc_page = max(0, self.toc[i - 1][2] - 1)
                prev_heading_pos = self.find_heading_on_page(self.toc[i - 1][1], prev_toc_page)
                
                if prev_heading_pos:
                    # Previous heading was found - chunk starts after it
                    if toc_page == prev_toc_page:
                        # Same page - start after previous heading
                        chunk_start_page = prev_toc_page
                        chunk_start_offset = prev_heading_pos[1]
                    else:
                        # Different pages - start from beginning of this page
                        chunk_start_page = toc_page
                        chunk_start_offset = None
                else:
                    # Previous heading not found - use page boundary
                    chunk_start_page = toc_page
                    chunk_start_offset = None
            
            # If current heading is in middle of page, adjust start
            if heading_pos and chunk_start_page == toc_page:
                # Heading is on the start page - chunk should start after heading
                chunk_start_offset = heading_pos[1]
            
            # Determine where this chunk should end
            if i < len(self.toc) - 1:
                next_level, next_title, next_page = self.toc[i + 1]
                next_toc_page = max(0, next_page - 1)
                next_heading_pos = self.find_heading_on_page(next_title, next_toc_page)
                
                if next_heading_pos:
                    # Next heading found - end before it
                    chunk_end_page = next_toc_page
                    chunk_end_offset = next_heading_pos[0]
                else:
                    # Next heading not found - end at start of next page
                    chunk_end_page = next_toc_page
                    chunk_end_offset = 0
            else:
                # Last chunk - end at end of document
                chunk_end_page = num_pages - 1
                chunk_end_offset = None
            
            chunk_boundaries.append({
                'start_page': chunk_start_page,
                'start_offset': chunk_start_offset,
                'end_page': chunk_end_page,
                'end_offset': chunk_end_offset,
                'heading_page': toc_page,
                'heading_pos': heading_pos
            })
        
        # Second pass: extract text and handle text above headings
        for i, (toc_entry, boundary) in enumerate(zip(self.toc, chunk_boundaries)):
            level, title, page = toc_entry
            
            # Check if we need to include text above the heading
            heading_pos = boundary['heading_pos']
            heading_page = boundary['heading_page']
            
            # If heading is in middle of page and this is not the first chunk,
            # we need to prepend text from above the heading
            text_above_heading = ""
            if heading_pos and i > 0:
                # Check if previous chunk ended before this heading
                prev_boundary = chunk_boundaries[i - 1]
                if (prev_boundary['end_page'] == heading_page and 
                    prev_boundary['end_offset'] is not None and
                    prev_boundary['end_offset'] < heading_pos[0]):
                    # There's text between previous chunk end and this heading
                    # This text belongs to previous chunk - we'll handle it below
                    pass
                elif prev_boundary['end_page'] < heading_page:
                    # Previous chunk ended on earlier page
                    # Text from start of this page to heading belongs to previous chunk
                    # We'll handle this by adjusting previous chunk
                    pass
            
            # Extract main text for this chunk
            text = self.extract_text_between_pages(
                boundary['start_page'],
                boundary['end_page'],
                boundary['start_offset'],
                boundary['end_offset']
            )
            
            # Build path
            path = self._build_path(i)
            
            # Format text with heading first
            formatted_text = f"{title}\n\n{text.strip()}" if text.strip() else title
            
            chunk = {
                "bookid": self.bookid,
                "chunkid": str(uuid.uuid4()),
                "title": title,
                "path": path,
                "level": level,
                "start_page": boundary['start_page'] + 1,
                "end_page": boundary['end_page'] + 1,
                "text": formatted_text,
                "related_paths": [],  # Will be populated during merging
                "mini_pdf_path": None  # Will be set when saving mini PDF
            }
            
            chunks.append(chunk)
        
        # Third pass: fix chunks where heading is in middle of page
        # Text above heading should be added to previous chunk
        for i in range(1, len(chunks)):
            current_boundary = chunk_boundaries[i]
            prev_boundary = chunk_boundaries[i - 1]
            
            heading_pos = current_boundary['heading_pos']
            heading_page = current_boundary['heading_page']
            
            if heading_pos:
                # Check if there's text above the heading that belongs to previous chunk
                if prev_boundary['end_page'] == heading_page:
                    # Previous chunk ended on same page
                    if prev_boundary['end_offset'] is None:
                        # Previous chunk ended at end of page, but heading is in middle
                        # Text above heading should be in previous chunk
                        page_text = self.get_page_text(heading_page)
                        text_above = page_text[:heading_pos[0]].strip()
                        if text_above:
                            chunks[i - 1]['text'] += '\n' + text_above
                            chunks[i - 1]['end_page'] = heading_page + 1
                    elif prev_boundary['end_offset'] < heading_pos[0]:
                        # There's a gap - text between belongs to previous chunk
                        page_text = self.get_page_text(heading_page)
                        text_between = page_text[prev_boundary['end_offset']:heading_pos[0]].strip()
                        if text_between:
                            chunks[i - 1]['text'] += '\n' + text_between
                elif prev_boundary['end_page'] < heading_page:
                    # Previous chunk ended on earlier page
                    # Text from start of heading page to heading belongs to previous chunk
                    page_text = self.get_page_text(heading_page)
                    text_above = page_text[:heading_pos[0]].strip()
                    if text_above:
                        chunks[i - 1]['text'] += '\n' + text_above
                        chunks[i - 1]['end_page'] = heading_page + 1
        
        # Filter out front and back matter
        filtered_chunks = [chunk for chunk in chunks if not self._should_exclude_chunk(chunk)]
        
        # Merge chunks based on page range logic
        merged_chunks = self._merge_chunks_by_page_range(filtered_chunks)
        
        # Save mini PDFs for each chunk
        for chunk in merged_chunks:
            mini_pdf_path = self._save_mini_pdf(chunk)
            chunk['mini_pdf_path'] = mini_pdf_path
        
        self.chunks = merged_chunks
        
        # Calculate and print analytics
        self._print_page_range_analytics(merged_chunks)
        
        return merged_chunks
    
    def _build_path(self, toc_index: int) -> str:
        """
        Build the hierarchical path for a TOC entry.
        
        Args:
            toc_index: Index in the TOC list
            
        Returns:
            Path string like "Chapter 1 > Section 1.1 > Subsection 1.1.1"
        """
        current_entry = self.toc[toc_index]
        current_level = current_entry[0]
        path_parts = [current_entry[1]]
        
        # Walk backwards to find parent headings
        for i in range(toc_index - 1, -1, -1):
            entry = self.toc[i]
            entry_level = entry[0]
            
            # If we find a heading at a lower level, it's a parent
            if entry_level < current_level:
                path_parts.insert(0, entry[1])
                current_level = entry_level
                
                # Stop if we've reached the root level
                if entry_level == 1:
                    break
        
        return " > ".join(path_parts)
    
    def _merge_chunks_by_page_range(self, chunks: List[Dict]) -> List[Dict]:
        """
        Merge chunks based on simple pairwise logic:
        - If chunk i starts and ends on the same page P, and chunk i+1 starts on page P 
          and ends on a different page Q (Q > P), merge chunk i into chunk i+1
        - Only check the immediate next chunk, don't look ahead further
        
        Example:
        - Chunk 1: 29-29, Chunk 2: 29-32 -> Merge chunk 1 into chunk 2
        - Chunk 2: 29-32, Chunk 3: 32-32 -> Don't merge (chunk 2 doesn't start and end on same page)
        
        Args:
            chunks: List of chunks to process
            
        Returns:
            List of merged chunks
        """
        if not chunks:
            return chunks
        
        merged = []
        i = 0
        
        while i < len(chunks):
            current_chunk = chunks[i]
            current_start = current_chunk['start_page']
            current_end = current_chunk['end_page']
            
            # Check if current chunk starts and ends on same page
            # and next chunk starts on that page and spans multiple pages
            if (current_start == current_end and 
                i + 1 < len(chunks)):
                next_chunk = chunks[i + 1]
                next_start = next_chunk['start_page']
                next_end = next_chunk['end_page']
                
                # Merge condition: current ends on P, next starts on P and ends on Q (Q > P)
                if next_start == current_end and next_end > next_start:
                    # Merge current chunk into next chunk
                    merged_chunk = self._combine_chunks([current_chunk, next_chunk])
                    merged.append(merged_chunk)
                    i += 2  # Skip both chunks
                else:
                    # No merge condition, keep current chunk as is
                    merged.append(current_chunk)
                    i += 1
            else:
                # Current chunk doesn't meet merge condition, keep as is
                merged.append(current_chunk)
                i += 1
        
        return merged
    
    def _combine_chunks(self, chunks: List[Dict]) -> Dict:
        """
        Combine multiple chunks into one.
        
        Args:
            chunks: List of chunks to combine
            
        Returns:
            Combined chunk
        """
        if len(chunks) == 1:
            return chunks[0]
        
        # Use the first chunk as base
        combined = chunks[0].copy()
        main_path = chunks[0]['path']
        
        # Collect paths from other chunks (excluding the main path)
        related_paths = []
        for chunk in chunks[1:]:  # Skip first chunk (main path)
            if chunk['path'] != main_path and chunk['path'] not in related_paths:
                related_paths.append(chunk['path'])
        
        # Combine text: each chunk already has heading first, so just join them
        combined_texts = [chunk['text'] for chunk in chunks]
        combined_text = '\n\n'.join(combined_texts)
        
        # Update metadata
        combined['title'] = chunks[0]['title']  # Keep first title as main
        combined['path'] = main_path  # Keep first path as main
        combined['related_paths'] = related_paths  # Only other paths, excluding main
        combined['level'] = min(chunk['level'] for chunk in chunks)  # Minimum level
        combined['start_page'] = min(chunk['start_page'] for chunk in chunks)
        combined['end_page'] = max(chunk['end_page'] for chunk in chunks)
        combined['text'] = combined_text
        combined['chunkid'] = str(uuid.uuid4())  # New chunk ID for merged chunk
        combined['mini_pdf_path'] = None  # Will be set when saving mini PDF after merging
        
        return combined
    
    def _is_front_matter(self, title: str) -> bool:
        """
        Check if a title is front matter.
        
        Args:
            title: The title to check
            
        Returns:
            True if it's front matter, False otherwise
        """
        title_lower = title.lower().strip()
        
        # Check exact matches
        for keyword in self.front_matter_keywords:
            if keyword == title_lower:
                return True
        
        # Check if title starts with front matter keyword
        for keyword in self.front_matter_keywords:
            if title_lower.startswith(keyword):
                return True
        
        return False
    
    def _is_back_matter(self, title: str) -> bool:
        """
        Check if a title is back matter.
        
        Args:
            title: The title to check
            
        Returns:
            True if it's back matter, False otherwise
        """
        title_lower = title.lower().strip()
        
        # Check exact matches
        for keyword in self.back_matter_keywords:
            if keyword == title_lower:
                return True
        
        # Check if title starts with back matter keyword
        for keyword in self.back_matter_keywords:
            if title_lower.startswith(keyword):
                return True
        
        # Check if it's a subsection of back matter (e.g., "Index > A")
        # by checking if path contains back matter keywords
        return False
    
    def _save_mini_pdf(self, chunk: Dict) -> str:
        """
        Save a mini PDF containing only the pages for this chunk.
        
        Args:
            chunk: The chunk dictionary with start_page and end_page
            
        Returns:
            Path to the saved mini PDF file
        """
        start_page = chunk['start_page']  # 1-indexed
        end_page = chunk['end_page']  # 1-indexed, inclusive
        
        # Convert to 0-indexed for PyMuPDF (which uses 0-indexed)
        start_page_0 = start_page - 1
        end_page_0 = end_page - 1
        
        # Create a new PDF document
        mini_doc = fitz.open()
        
        # Copy pages from the original document
        for page_num in range(start_page_0, end_page_0 + 1):
            if 0 <= page_num < len(self.doc):
                mini_doc.insert_pdf(self.doc, from_page=page_num, to_page=page_num)
        
        # Generate filename: bookid_startpage_endpage.pdf
        filename = f"{self.bookid}_{start_page}_{end_page}.pdf"
        filepath = os.path.join(self.output_dir, filename)
        
        # Save the mini PDF
        mini_doc.save(filepath)
        mini_doc.close()
        
        return filepath
    
    def _should_exclude_chunk(self, chunk: Dict) -> bool:
        """
        Determine if a chunk should be excluded (front or back matter).
        
        Args:
            chunk: The chunk dictionary
            
        Returns:
            True if chunk should be excluded, False otherwise
        """
        title = chunk.get('title', '')
        path = chunk.get('path', '')
        
        # Check if title is front matter
        if self._is_front_matter(title):
            return True
        
        # Check if title is back matter
        if self._is_back_matter(title):
            return True
        
        # Check if path contains front matter (for subsections like "Preface > Section 1")
        path_lower = path.lower()
        for keyword in self.front_matter_keywords:
            # Check if keyword appears as a separate word in the path
            # This avoids false positives (e.g., "Introduction" in "Introduction to Networks")
            if f' {keyword} ' in path_lower or path_lower.startswith(keyword + ' ') or path_lower.endswith(' ' + keyword):
                return True
        
        # Check if path contains back matter (for subsections like "Index > A")
        for keyword in self.back_matter_keywords:
            # Check if keyword appears as a separate word in the path
            if f' {keyword} ' in path_lower or path_lower.startswith(keyword + ' ') or path_lower.endswith(' ' + keyword):
                return True
        
        return False
    
    def save_chunks(self, output_path: str, format: str = "json"):
        """
        Save chunks to a file.
        
        Args:
            output_path: Path to save the chunks
            format: Output format ("json" or "jsonl")
        """
        if not self.chunks:
            self.process_chunks()
        
        if format == "json":
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(self.chunks, f, indent=2, ensure_ascii=False)
        elif format == "jsonl":
            with open(output_path, 'w', encoding='utf-8') as f:
                for chunk in self.chunks:
                    f.write(json.dumps(chunk, ensure_ascii=False) + '\n')
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _print_page_range_analytics(self, chunks: List[Dict]) -> Dict[str, Any]:
        """
        Analyze and print page range statistics for chunks.
        
        Args:
            chunks: List of chunks to analyze
            
        Returns:
            Dictionary with analytics data
        """
        if not chunks:
            print("\n=== Page Range Analytics ===")
            print("No chunks to analyze")
            return {}
        
        # Calculate page ranges
        page_ranges = []
        zero_page_ranges = 0
        
        for chunk in chunks:
            start = chunk['start_page']
            end = chunk['end_page']
            range_size = end - start + 1  # +1 because both start and end are inclusive
            
            page_ranges.append({
                'chunk_id': chunk.get('chunkid', 'unknown'),
                'title': chunk.get('title', 'unknown'),
                'start_page': start,
                'end_page': end,
                'range_size': range_size
            })
            
            if start == end:
                zero_page_ranges += 1
        
        # Find longest and shortest ranges
        if page_ranges:
            longest = max(page_ranges, key=lambda x: x['range_size'])
            shortest = min(page_ranges, key=lambda x: x['range_size'])
            
            analytics = {
                'total_chunks': len(chunks),
                'longest_page_range': {
                    'size': longest['range_size'],
                    'start_page': longest['start_page'],
                    'end_page': longest['end_page'],
                    'title': longest['title'],
                    'chunk_id': longest['chunk_id']
                },
                'shortest_page_range': {
                    'size': shortest['range_size'],
                    'start_page': shortest['start_page'],
                    'end_page': shortest['end_page'],
                    'title': shortest['title'],
                    'chunk_id': shortest['chunk_id']
                },
                'zero_page_range_count': zero_page_ranges,
                'zero_page_range_percentage': (zero_page_ranges / len(chunks)) * 100 if chunks else 0
            }
            
            # Print analytics
            print("\n=== Page Range Analytics ===")
            print(f"Total chunks: {analytics['total_chunks']}")
            print(f"\nLongest page range: {analytics['longest_page_range']['size']} pages")
            print(f"  - Pages: {analytics['longest_page_range']['start_page']}-{analytics['longest_page_range']['end_page']}")
            print(f"  - Title: {analytics['longest_page_range']['title']}")
            print(f"\nShortest page range: {analytics['shortest_page_range']['size']} pages")
            print(f"  - Pages: {analytics['shortest_page_range']['start_page']}-{analytics['shortest_page_range']['end_page']}")
            print(f"  - Title: {analytics['shortest_page_range']['title']}")
            print(f"\nChunks with 0 page range (start == end): {analytics['zero_page_range_count']}")
            print(f"  - Percentage: {analytics['zero_page_range_percentage']:.2f}%")
            print("=" * 30)
            
            return analytics
        else:
            return {}
    
    def close(self):
        """Close the PDF document."""
        self.doc.close()

