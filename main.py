"""
PDF Chunk Extractor - Optimized and Clean
Extracts smart chunks from PDFs with automatic filtering
"""

import fitz  # PyMuPDF
import os
import json
import re
from typing import List, Dict, Tuple
from datetime import datetime


# ============================================================================
# EXCLUSION PATTERNS
# ============================================================================

DEFAULT_EXCLUDE_PATTERNS = [
    # Publishing/Legal
    r'\b(title|cover|copyright|half-title)\s+page\b',
    r'\b(publisher|publication|imprint|colophon|permissions?)\b',
    
    # Front matter
    r'\b(preface|foreword|acknowledgment|acknowledgement|dedication)\b',
    r'\babout\s+(the\s+)?(author|editor|contributor)s?\b',
    r'\b(table\s+of\s+)?contents\b',
    r'\blist\s+of\s+(figures?|tables?|illustrations?)\b',
    
    # Back matter
    r'\b(appendix|appendices|glossary|index)\b',
    r'\b(bibliography|references?|works?\s+cited)\b',
    r'\b(further|recommended)\s+reading\b',
    r'\b(end)?notes?\b$',
    
    # Marketing/Misc
    r'\b(blank|empty)\s+page\b',
    r'\balso\s+by\b',
    r'\b(praise|testimonial)s?\b',
]


def should_exclude_section(title: str, path: List[str], custom_patterns: List[str] = None) -> Tuple[bool, str]:
    """Check if section should be excluded based on title/path."""
    patterns = DEFAULT_EXCLUDE_PATTERNS + (custom_patterns or [])
    full_text = f"{title} {' '.join(path)}".lower()
    
    for pattern in patterns:
        if re.search(pattern, full_text, re.IGNORECASE):
            return True, pattern
    
    return False, ""


def should_exclude_by_content(text: str, max_check_length: int = 2000) -> Tuple[bool, str]:
    """Check if section should be excluded based on its content (early pages check)."""
    # Only check first portion of text for efficiency
    sample = text[:max_check_length].lower()
    
    # Patterns that indicate front matter content
    front_matter_indicators = [
        (r'copyright.*?all rights reserved', 'copyright notice'),
        (r'isbn[-\s]?\d', 'ISBN/publication info'),
        (r'library of congress', 'cataloging data'),
        (r'published by|publisher|printing.*?edition', 'publisher info'),
        (r'about the authors?.*?university', 'about authors'),
        (r'dedication.*?to\s+\w+\s+and', 'dedication'),
        (r'preface.*?welcome to', 'preface content'),
        (r'table of contents.*?chapter', 'table of contents'),
    ]
    
    for pattern, reason in front_matter_indicators:
        if re.search(pattern, sample, re.IGNORECASE | re.DOTALL):
            return True, reason
    
    return False, ""


# ============================================================================
# TOC PROCESSING
# ============================================================================

def build_toc_tree(toc: List) -> Dict:
    """Build nested TOC tree from PyMuPDF table of contents."""
    root = {"title": "root", "page": 0, "children": []}
    stack = [root]
    
    for level, title, page in toc:
        node = {"title": title, "page": page, "children": []}
        while len(stack) > level:
            stack.pop()
        stack[-1]["children"].append(node)
        stack.append(node)
    
    return root


def collect_sections(node: Dict, parent_path: List[str] = None) -> List[Dict]:
    """Collect all leaf sections from TOC tree."""
    if parent_path is None:
        parent_path = []
    
    path = parent_path + ([node["title"]] if node["title"] != "root" else [])
    sections = []
    
    if not node["children"]:
        # Leaf node
        sections.append({
            "title": node["title"],
            "page": node["page"],
            "path": path
        })
    else:
        # Recurse into children
        for child in node["children"]:
            sections.extend(collect_sections(child, path))
    
    return sections


# ============================================================================
# TEXT EXTRACTION
# ============================================================================

def extract_text(doc: fitz.Document, start_page: int, end_page: int) -> str:
    """Extract text from page range (1-indexed, inclusive)."""
    text_parts = []
    for page_num in range(start_page - 1, end_page):
        if 0 <= page_num < len(doc):
            text_parts.append(doc.load_page(page_num).get_text("text"))
    return "\n".join(text_parts).strip()


def estimate_tokens(text: str) -> int:
    """Estimate token count (rough approximation)."""
    return int(len(text.split()) * 1.3)


# ============================================================================
# PDF OPERATIONS
# ============================================================================

def save_mini_pdf(doc: fitz.Document, start_page: int, end_page: int, 
                  output_dir: str, book_id: str) -> Tuple[str, str]:
    """Save subset of pages as mini-PDF."""
    os.makedirs(output_dir, exist_ok=True)
    
    mini_doc = fitz.open()
    for page_num in range(start_page - 1, end_page):
        if 0 <= page_num < len(doc):
            mini_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
    
    filename = f"{book_id}_{start_page}_{end_page - 1}.pdf"
    filepath = os.path.join(output_dir, filename)
    
    mini_doc.save(filepath)
    mini_doc.close()
    
    return filepath, filename


# ============================================================================
# CHUNK OPTIMIZATION
# ============================================================================

def should_merge(current: Dict, next_chunk: Dict) -> bool:
    """Check if two chunks should be merged (same parent)."""
    if len(current["path"]) != len(next_chunk["path"]):
        return False
    if len(current["path"]) > 1:
        return current["path"][:-1] == next_chunk["path"][:-1]
    return True


def optimize_chunks(sections: List[Dict], doc: fitz.Document, page_count: int,
                   min_pages: int = 2, max_pages: int = 25,
                   min_tokens: int = 300, max_tokens: int = 6000) -> List[Dict]:
    """Merge small consecutive sections into optimized chunks."""
    
    if not sections:
        return []
    
    chunks = []
    i = 0
    
    while i < len(sections):
        # Start new chunk
        start_page = sections[i]["page"]
        end_page = sections[i + 1]["page"] if i + 1 < len(sections) else page_count + 1
        merged_titles = [sections[i]["title"]]
        path = sections[i]["path"]
        
        # Extract initial text
        text = extract_text(doc, start_page, end_page)
        tokens = estimate_tokens(text)
        pages = end_page - start_page
        
        # Try merging with following sections
        j = i + 1
        while j < len(sections):
            next_end = sections[j + 1]["page"] if j + 1 < len(sections) else page_count + 1
            potential_pages = next_end - start_page
            
            # Check merge conditions
            can_merge = (
                (pages < min_pages or tokens < min_tokens) and
                potential_pages <= max_pages and
                should_merge(sections[i], sections[j])
            )
            
            if not can_merge:
                break
            
            # Merge
            end_page = next_end
            merged_titles.append(sections[j]["title"])
            text = extract_text(doc, start_page, end_page)
            tokens = estimate_tokens(text)
            pages = end_page - start_page
            j += 1
        
        # Check if merged chunk is too large
        if pages > max_pages or tokens > max_tokens:
            # Don't merge, use just first section
            end_page = sections[i + 1]["page"] if i + 1 < len(sections) else page_count + 1
            merged_titles = [sections[i]["title"]]
            text = extract_text(doc, start_page, end_page)
            tokens = estimate_tokens(text)
            pages = end_page - start_page
            j = i + 1
        
        # Create chunk
        title = " - ".join(merged_titles) if len(merged_titles) > 1 else merged_titles[0]
        
        chunks.append({
            "title": title,
            "original_titles": merged_titles,
            "path": path,
            "start_page": start_page,
            "end_page": end_page - 1,
            "page_count": pages,
            "token_count": tokens,
            "text": text
        })
        
        i = j
    
    return chunks


# ============================================================================
# MAIN EXTRACTION FUNCTION
# ============================================================================

def extract_pdf_chunks(
    pdf_path: str,
    output_dir: str = "output",
    book_id: str = None,
    save_mini_pdfs: bool = True,
    min_pages: int = 2,
    max_pages: int = 25,
    min_tokens: int = 300,
    max_tokens: int = 6000,
    exclude_sections: bool = True,
    custom_exclude_patterns: List[str] = None
) -> Dict:
    """
    Extract optimized chunks from PDF.
    
    Args:
        pdf_path: Path to PDF file
        output_dir: Output directory
        book_id: Book identifier (defaults to filename)
        save_mini_pdfs: Whether to save mini-PDF files
        min_pages: Minimum pages per chunk
        max_pages: Maximum pages per chunk
        min_tokens: Minimum tokens per chunk
        max_tokens: Maximum tokens per chunk
        exclude_sections: Whether to exclude non-content sections
        custom_exclude_patterns: Additional exclusion patterns
    
    Returns:
        Dictionary with chunks and metadata
    """
    
    # Setup
    if book_id is None:
        book_id = os.path.splitext(os.path.basename(pdf_path))[0]
    
    print(f"📖 Processing: {pdf_path}")
    print(f"📝 Book ID: {book_id}\n")
    
    # Open PDF
    doc = fitz.open(pdf_path)
    page_count = len(doc)
    toc = doc.get_toc()
    
    print(f"📄 Total pages: {page_count}")
    print(f"📑 TOC entries: {len(toc)}\n")
    
    # Build sections from TOC
    if not toc:
        print("⚠️  No TOC found - creating single chunk for entire document\n")
        sections = [{
            "title": "Full Document",
            "page": 1,
            "path": ["Full Document"]
        }]
    else:
        toc_tree = build_toc_tree(toc)
        sections = collect_sections(toc_tree)
    
    # Filter excluded sections
    filtered_sections = []
    excluded_list = []
    
    if exclude_sections:
        for section in sections:
            # First check by title/path
            should_exclude, pattern = should_exclude_section(
                section["title"],
                section["path"],
                custom_exclude_patterns
            )
            
            if should_exclude:
                excluded_list.append({
                    "title": section["title"],
                    "path": " > ".join(section["path"]),
                    "reason": f"title pattern: {pattern}"
                })
                continue
            
            # For early sections (first 50 pages), also check content
            if section["page"] <= 50:
                end_page = sections[sections.index(section) + 1]["page"] if sections.index(section) + 1 < len(sections) else page_count + 1
                text_sample = extract_text(doc, section["page"], min(end_page, section["page"] + 5))
                
                should_exclude_content, reason = should_exclude_by_content(text_sample)
                if should_exclude_content:
                    excluded_list.append({
                        "title": section["title"],
                        "path": " > ".join(section["path"]),
                        "reason": f"content: {reason}"
                    })
                    continue
            
            filtered_sections.append(section)
    else:
        filtered_sections = sections
    
    if excluded_list:
        print(f"🚫 Excluded {len(excluded_list)} sections:")
        for exc in excluded_list[:10]:
            print(f"   • {exc['title']}")
        if len(excluded_list) > 10:
            print(f"   ... and {len(excluded_list) - 10} more")
        print()
    
    # Optimize chunks
    optimized = optimize_chunks(
        filtered_sections, doc, page_count,
        min_pages, max_pages, min_tokens, max_tokens
    )
    
    print(f"📊 Created {len(optimized)} optimized chunks\n")
    
    # Save mini-PDFs and prepare final chunks
    pdf_dir = os.path.join(output_dir, "mini_pdfs")
    final_chunks = []
    
    for idx, chunk in enumerate(optimized, 1):
        if not chunk["text"].strip():
            print(f"⚠️  Skipping empty chunk: {chunk['title']}")
            continue
        
        chunk_data = {
            "chunk_id": idx,
            "title": chunk["title"],
            "original_titles": chunk["original_titles"],
            "path": " > ".join(chunk["path"]),
            "hierarchy": chunk["path"],
            "start_page": chunk["start_page"],
            "end_page": chunk["end_page"],
            "page_count": chunk["page_count"],
            "token_count": chunk["token_count"],
            "text": chunk["text"],
            "text_preview": chunk["text"][:200] + "..." if len(chunk["text"]) > 200 else chunk["text"]
        }
        
        if save_mini_pdfs:
            local_path, filename = save_mini_pdf(
                doc, chunk["start_page"], chunk["end_page"] + 1,
                pdf_dir, book_id
            )
            chunk_data["local_path"] = local_path
            chunk_data["filename"] = filename
        
        final_chunks.append(chunk_data)
        
        print(f"  ✓ Chunk {idx}: {chunk['title'][:60]}... "
              f"(pp.{chunk['start_page']}-{chunk['end_page']}, "
              f"{chunk['page_count']}pg, ~{chunk['token_count']}tok)")
    
    doc.close()
    
    # Calculate statistics
    original_count = len(toc) if toc else page_count
    new_count = len(final_chunks)
    reduction = ((original_count - new_count) / original_count * 100) if original_count > 0 else 0
    avg_pages = sum(c['page_count'] for c in final_chunks) / len(final_chunks) if final_chunks else 0
    avg_tokens = sum(c['token_count'] for c in final_chunks) / len(final_chunks) if final_chunks else 0
    
    # Prepare result
    result = {
        "metadata": {
            "book_id": book_id,
            "source_file": pdf_path,
            "total_pages": page_count,
            "original_toc_entries": len(toc),
            "excluded_sections": len(excluded_list),
            "optimized_chunks": new_count,
            "reduction_percentage": round(reduction, 1),
            "avg_pages_per_chunk": round(avg_pages, 1),
            "avg_tokens_per_chunk": round(avg_tokens, 0),
            "processing_date": datetime.now().isoformat(),
            "parameters": {
                "min_pages": min_pages,
                "max_pages": max_pages,
                "min_tokens": min_tokens,
                "max_tokens": max_tokens,
                "exclude_sections": exclude_sections
            }
        },
        "chunks": final_chunks
    }
    
    # Save to JSON
    json_path = os.path.join(output_dir, f"{book_id}_chunks.json")
    os.makedirs(output_dir, exist_ok=True)
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    # Summary
    print(f"\n✅ Processing complete!")
    print(f"📊 Statistics:")
    print(f"   • Original sections: {original_count}")
    print(f"   • Excluded: {len(excluded_list)}")
    print(f"   • Final chunks: {new_count}")
    print(f"   • Reduction: {reduction:.1f}%")
    print(f"   • Avg pages/chunk: {avg_pages:.1f}")
    print(f"   • Avg tokens/chunk: {avg_tokens:.0f}")
    print(f"\n💾 Saved to: {json_path}")
    if save_mini_pdfs:
        print(f"📁 Mini-PDFs: {pdf_dir}/")
    
    return result


# ============================================================================
# CLI
# ============================================================================

def main():
    """Command-line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Extract optimized chunks from PDF',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pdf_chunk_extractor.py book.pdf
  python pdf_chunk_extractor.py book.pdf --no-exclude
  python pdf_chunk_extractor.py book.pdf --min-pages 3 --max-pages 30
  python pdf_chunk_extractor.py book.pdf --exclude-pattern "\\bexercises?\\b"
        """
    )
    
    parser.add_argument('--pdf_path',default="book2.pdf", help='Path to PDF file')
    parser.add_argument('--output-dir', default='output2', help='Output directory (default: output)')
    parser.add_argument('--book-id', help='Book identifier (default: filename)')
    parser.add_argument('--no-mini-pdfs', action='store_true', help='Skip saving mini-PDFs')
    parser.add_argument('--min-pages', type=int, default=2, help='Minimum pages per chunk (default: 2)')
    parser.add_argument('--max-pages', type=int, default=25, help='Maximum pages per chunk (default: 25)')
    parser.add_argument('--min-tokens', type=int, default=300, help='Minimum tokens per chunk (default: 300)')
    parser.add_argument('--max-tokens', type=int, default=6000, help='Maximum tokens per chunk (default: 6000)')
    parser.add_argument('--no-exclude', action='store_true', help='Don\'t exclude non-content sections')
    parser.add_argument('--exclude-pattern', action='append', help='Additional exclusion patterns (regex)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.pdf_path):
        print(f"❌ Error: PDF file not found: {args.pdf_path}")
        return 1
    
    try:
        extract_pdf_chunks(
            pdf_path=args.pdf_path,
            output_dir=args.output_dir,
            book_id=args.book_id,
            save_mini_pdfs=not args.no_mini_pdfs,
            min_pages=args.min_pages,
            max_pages=args.max_pages,
            min_tokens=args.min_tokens,
            max_tokens=args.max_tokens,
            exclude_sections=not args.no_exclude,
            custom_exclude_patterns=args.exclude_pattern
        )
        return 0
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())