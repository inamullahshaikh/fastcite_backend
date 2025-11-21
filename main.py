"""
PDF Chunk Extractor - TOC Tree Based
Clean semantic chunking algorithm that preserves section boundaries
"""

import fitz  # PyMuPDF
import os
import json
import re
import uuid
from typing import List, Dict, Tuple, Optional
from datetime import datetime

try:
    import tiktoken
    HAS_TIKTOKEN = True
except ImportError:
    HAS_TIKTOKEN = False


# ============================================================================
# EXCLUSION PATTERNS
# ============================================================================

FRONT_MATTER_PATTERNS = [
    r'\b(title|cover|copyright|half-title)\s+page\b',
    r'\b(publisher|publication|imprint|colophon|permissions?)\b',
    r'\b(preface|foreword|acknowledgment|acknowledgement|dedication)\b',
    r'\babout\s+(the\s+)?(author|editor|contributor)s?\b',
    r'\b(table\s+of\s+)?contents\b',
    r'\blist\s+of\s+(figures?|tables?|illustrations?)\b',
]

BACK_MATTER_PATTERNS = [
    r'\b(appendix|appendices|glossary|index)\b',
    r'\b(bibliography|references?|works?\s+cited)\b',
    r'\b(further|recommended)\s+reading\b',
    r'\b(end)?notes?\b$',
    r'\balso\s+by\b',
    r'\b(praise|testimonial)s?\b',
]


def should_exclude(title: str, path: List[str], patterns: List[str]) -> bool:
    """Check if section matches exclusion patterns."""
    full_text = f"{title} {' '.join(path)}".lower()
    return any(re.search(pattern, full_text, re.IGNORECASE) for pattern in patterns)


# ============================================================================
# TOC TREE BUILDING
# ============================================================================

def build_toc_tree(toc: List) -> Dict:
    """Build nested TOC tree from PyMuPDF table of contents."""
    root = {"title": "root", "page": 0, "level": 0, "children": []}
    stack = [root]
    
    for level, title, page in toc:
        node = {"title": title, "page": page, "level": level, "children": []}
        
        # Pop stack to correct level
        while len(stack) > level + 1:
            stack.pop()
        
        # Add to parent
        stack[-1]["children"].append(node)
        stack.append(node)
    
    return root


def flatten_tree(node: Dict, parent_path: List[str] = None) -> List[Dict]:
    """Flatten tree into list of all nodes with their paths."""
    if parent_path is None:
        parent_path = []
    
    nodes = []
    path = parent_path + ([node["title"]] if node["title"] != "root" else [])
    
    if node["title"] != "root":
        nodes.append({
            "title": node["title"],
            "page": node["page"],
            "level": node["level"],
            "path": path,
            "children": node["children"]
        })
    
    for child in node["children"]:
        nodes.extend(flatten_tree(child, path))
    
    return nodes


# ============================================================================
# CALCULATE SECTION BOUNDARIES
# ============================================================================

def calculate_section_end(node: Dict, all_nodes: List[Dict], total_pages: int) -> int:
    """
    Calculate end page for a section.
    End is exclusive - content goes from start_page to end_page (exclusive).
    """
    start_page = node["page"]
    level = node["level"]
    
    # If section has children, it ends at first child's start page
    if node.get("children"):
        first_child_page = node["children"][0]["page"]
        if first_child_page > start_page:
            return first_child_page
    
    # Find next section at same or higher level (sibling or parent's sibling)
    for other in all_nodes:
        if other["page"] > start_page and other["level"] <= level:
            return other["page"]
    
    # No next section found - goes to end of document
    return total_pages + 1


# ============================================================================
# TEXT EXTRACTION
# ============================================================================

def find_heading_on_page(page, heading_title: str) -> int:
    """Find heading position on page. Returns character index or -1."""
    text = page.get_text("text")
    if not text:
        return -1
    
    heading_lower = heading_title.lower().strip()
    text_lower = text.lower()
    
    # Try exact match
    idx = text_lower.find(heading_lower)
    if idx != -1:
        return idx
    
    # Try normalized whitespace
    heading_norm = re.sub(r'\s+', ' ', heading_lower)
    text_norm = re.sub(r'\s+', ' ', text_lower)
    idx = text_norm.find(heading_norm)
    if idx != -1:
        # Approximate mapping back
        return min(len(text), idx + (len(text) - len(text_norm)) // 2)
    
    # Try number pattern (e.g., "3.4" from "3.4 Principles...")
    number_match = re.search(r'\d+\.\d+', heading_title)
    if number_match:
        idx = text_lower.find(number_match.group())
        if idx != -1:
            return idx
    
    return -1


def extract_section_text(doc: fitz.Document, start_page: int, end_page: int, 
                         heading_title: str = None) -> str:
    """
    Extract text for a section.
    start_page: 1-indexed, inclusive
    end_page: 1-indexed, exclusive
    heading_title: If provided, find heading on first page and start from there
    """
    text_parts = []
    
    for page_num in range(start_page - 1, end_page - 1):
        if 0 <= page_num < len(doc):
            page = doc.load_page(page_num)
            page_text = page.get_text("text")
            
            # On first page, find heading position if provided
            if page_num == start_page - 1 and heading_title:
                heading_pos = find_heading_on_page(page, heading_title)
                if heading_pos > 0:
                    page_text = page_text[heading_pos:].lstrip()
            
            text_parts.append(page_text)
    
    return "\n".join(text_parts).strip()


# ============================================================================
# TOKEN COUNTING
# ============================================================================

def count_tokens(text: str) -> int:
    """Count tokens in text. Uses tiktoken if available, else estimates."""
    if HAS_TIKTOKEN:
        try:
            enc = tiktoken.get_encoding("cl100k_base")
            return len(enc.encode(text))
        except:
            pass
    
    # Fallback: rough estimate (1 token ≈ 0.75 words)
    return int(len(text.split()) * 1.3)


# ============================================================================
# CHUNK SPLITTING (at subsection boundaries only)
# ============================================================================

def split_large_section(section: Dict, doc: fitz.Document, max_tokens: int,
                        all_sections: List[Dict]) -> List[Dict]:
    """
    Split a large section into smaller chunks at subsection boundaries.
    Never splits in the middle of a subsection.
    """
    # Handle both formats: section dict from tree or processed chunk
    start_page = section.get("start_page") or section.get("page")
    end_page = section.get("end_page_exclusive") or section.get("end_page")
    section_path = section.get("path")
    section_title = section.get("title", "")
    
    if not all([start_page, end_page, section_path]):
        # If we can't get required fields, return section as-is (with text if it exists)
        if "text" in section:
            # Ensure end_page_exclusive exists if we have end_page
            if "end_page_exclusive" not in section and end_page:
                section["end_page_exclusive"] = end_page
            return [section]
        # Otherwise extract text
        if start_page and end_page:
            text = extract_section_text(doc, start_page, end_page, section_title)
            section["text"] = text
            section["token_count"] = count_tokens(text)
            section["end_page_exclusive"] = end_page
            section["end_page"] = end_page - 1
        return [section]
    
    # Check token count - if already small enough, ensure text exists and return
    token_count = section.get("token_count", 0)
    if token_count == 0:
        # Extract text to get token count
        text = extract_section_text(doc, start_page, end_page, section_title)
        token_count = count_tokens(text)
        section["text"] = text
        section["token_count"] = token_count
    
    if token_count <= max_tokens:
        # Ensure section has all required fields
        if "text" not in section:
            section["text"] = extract_section_text(doc, start_page, end_page, section_title)
        # Ensure end_page_exclusive exists
        if "end_page_exclusive" not in section:
            section["end_page_exclusive"] = end_page
        # Ensure end_page is inclusive for display
        if "end_page" not in section or section.get("end_page") != end_page - 1:
            section["end_page"] = end_page - 1
        return [section]
    
    # Find all subsections (direct children) within this section
    subsection_boundaries = []
    
    for subsec in all_sections:
        subsec_path = subsec["path"]
        # Check if this is a direct child (one level deeper)
        if (len(subsec_path) == len(section_path) + 1 and
            subsec_path[:-1] == section_path and
            start_page <= subsec["page"] < end_page):
            # Use subsection's END page as boundary
            subsection_boundaries.append(subsec["end_page"])
    
    # Sort boundaries
    subsection_boundaries = sorted(set(subsection_boundaries))
    # Filter to only boundaries within our range
    subsection_boundaries = [b for b in subsection_boundaries if start_page < b < end_page]
    
    # If no subsections found, cannot split without breaking semantic structure
    if not subsection_boundaries:
        # Ensure section has text and all required fields before returning
        if "text" not in section:
            section["text"] = extract_section_text(doc, start_page, end_page, section_title)
        if "end_page_exclusive" not in section:
            section["end_page_exclusive"] = end_page
        if "end_page" not in section or section.get("end_page") != end_page - 1:
            section["end_page"] = end_page - 1
        return [section]
    
    # Split at subsection boundaries
    chunks = []
    current_start = start_page
    part_num = 1
    
    for boundary in subsection_boundaries:
        # Extract text up to boundary
        text = extract_section_text(doc, current_start, boundary,
                                   section["title"] if part_num == 1 else None)
        tokens = count_tokens(text)
        
        if tokens > 0:
            chunk = {
                "title": f"{section['title']} (Part {part_num})" if part_num > 1 else section["title"],
                "path": section["path"],
                "level": section["level"],
                "start_page": current_start,
                "end_page": boundary - 1,  # Convert to inclusive for display
                "end_page_exclusive": boundary,
                "token_count": tokens,
                "text": text
            }
            chunks.append(chunk)
            part_num += 1
        
        current_start = boundary
    
    # Handle remaining content after last subsection
    if current_start < end_page:
        text = extract_section_text(doc, current_start, end_page, None)
        tokens = count_tokens(text)
        if tokens > 0:
            chunk = {
                "title": f"{section['title']} (Part {part_num})" if part_num > 1 else section["title"],
                "path": section["path"],
                "level": section["level"],
                "start_page": current_start,
                "end_page": end_page - 1,
                "end_page_exclusive": end_page,
                "token_count": tokens,
                "text": text
            }
            chunks.append(chunk)
    
    # Recursively split any chunks that are still too large
    final_chunks = []
    for chunk in chunks:
        if chunk["token_count"] > max_tokens:
            # Create a section dict for recursive splitting (include text)
            sub_section = {
                "title": chunk["title"],
                "path": chunk["path"],
                "level": chunk["level"],
                "start_page": chunk["start_page"],
                "end_page": chunk["end_page_exclusive"],
                "end_page_exclusive": chunk["end_page_exclusive"],
                "token_count": chunk["token_count"],
                "text": chunk["text"]  # Include text for recursive calls
            }
            further_split = split_large_section(sub_section, doc, max_tokens, all_sections)
            final_chunks.extend(further_split)
        else:
            final_chunks.append(chunk)
    
    return final_chunks


# ============================================================================
# MAIN EXTRACTION FUNCTION
# ============================================================================

def extract_pdf_chunks(
    pdf_path: str,
    output_dir: str = "output",
    book_id: str = None,
    save_mini_pdfs: bool = True,
    min_tokens: int = 200,
    max_tokens: int = 2000,
    exclude_sections: bool = True,
    custom_exclude_front: List[str] = None,
    custom_exclude_back: List[str] = None
) -> Dict:
    """
    Extract semantic chunks from PDF based on TOC structure.
    """
    
    if book_id is None:
        book_id = str(uuid.uuid4())
    
    print(f"📖 Processing: {pdf_path}")
    print(f"📝 Book ID: {book_id}\n")
    
    # Open PDF
    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    toc = doc.get_toc()
    
    print(f"📄 Total pages: {total_pages}")
    print(f"📑 TOC entries: {len(toc)}\n")
    
    # Build TOC tree
    if not toc:
        print("⚠️  No TOC found - creating single chunk for entire document\n")
        toc_tree = {
            "title": "root",
            "page": 0,
            "level": 0,
            "children": [{
                "title": "Full Document",
                "page": 1,
                "level": 1,
                "children": []
            }]
        }
    else:
        toc_tree = build_toc_tree(toc)
    
    # Flatten tree to list of all sections
    all_sections = flatten_tree(toc_tree)
    
    # Calculate end pages for all sections
    for section in all_sections:
        section["end_page"] = calculate_section_end(section, all_sections, total_pages)
    
    print(f"🌳 Found {len(all_sections)} sections in TOC\n")
    
    # Filter out front and back matter
    filtered_sections = []
    excluded_list = []
    
    if exclude_sections:
        # Find main content boundaries
        first_content_idx = 0
        for i, section in enumerate(all_sections):
            if not should_exclude(section["title"], section["path"], FRONT_MATTER_PATTERNS):
                title_lower = section["title"].lower()
                if any(kw in title_lower for kw in ['chapter', 'part', 'section', 'introduction']):
                    first_content_idx = i
                    break
                elif section["page"] > 30:
                    first_content_idx = i
                    break
        
        last_content_idx = len(all_sections) - 1
        for i in range(len(all_sections) - 1, -1, -1):
            if not should_exclude(all_sections[i]["title"], all_sections[i]["path"], BACK_MATTER_PATTERNS):
                last_content_idx = i
                break
        
        print(f"📍 Main content: sections {first_content_idx} to {last_content_idx}\n")
        
        # Filter sections
        for i, section in enumerate(all_sections):
            if i < first_content_idx:
                if should_exclude(section["title"], section["path"], FRONT_MATTER_PATTERNS) or section["page"] <= 20:
                    excluded_list.append({
                        "title": section["title"],
                        "path": " > ".join(section["path"]),
                        "reason": "front matter"
                    })
                    continue
            
            if i > last_content_idx:
                excluded_list.append({
                    "title": section["title"],
                    "path": " > ".join(section["path"]),
                    "reason": "back matter"
                })
                continue
            
            # Exclude parent sections that have children starting on same page
            # (no unique content)
            if section.get("children") and section["children"]:
                if section["children"][0]["page"] == section["page"]:
                    excluded_list.append({
                        "title": section["title"],
                        "path": " > ".join(section["path"]),
                        "reason": "parent with child on same page"
                    })
                    continue
            
            filtered_sections.append(section)
    else:
        filtered_sections = all_sections
    
    if excluded_list:
        print(f"🚫 Excluded {len(excluded_list)} sections\n")
    
    # Process sections into chunks
    print(f"📊 Processing {len(filtered_sections)} sections into chunks...\n")
    
    all_chunks = []
    
    for section in filtered_sections:
        # Extract text for this section
        text = extract_section_text(doc, section["page"], section["end_page"], section["title"])
        tokens = count_tokens(text)
        
        if tokens < min_tokens:
            # Skip very small sections (likely empty or just headings)
            continue
        
        # Create chunk
        chunk = {
            "title": section["title"],
            "path": " > ".join(section["path"]),
            "path_array": section["path"],
            "level": section["level"],
            "start_page": section["page"],
            "end_page": section["end_page"] - 1,  # Convert to inclusive
            "end_page_exclusive": section["end_page"],
            "token_count": tokens,
            "text": text
        }
        
        # If chunk is too large, split at subsection boundaries
        if tokens > max_tokens:
            # Prepare section dict for splitting function with correct keys
            section_for_splitting = {
                "title": section["title"],
                "path": section["path"],
                "level": section["level"],
                "start_page": section["page"],  # Use "page" from section
                "end_page": section["end_page"],  # This is already exclusive
                "end_page_exclusive": section["end_page"],
                "token_count": tokens
            }
            split_chunks = split_large_section(section_for_splitting, doc, max_tokens, all_sections)
            # Convert split chunks to final format
            for split_chunk in split_chunks:
                # Ensure end_page_exclusive exists
                end_page_exclusive = split_chunk.get("end_page_exclusive")
                if end_page_exclusive is None:
                    # If not present, convert from inclusive end_page
                    end_page_exclusive = split_chunk.get("end_page", 0) + 1
                
                final_chunk = {
                    "chunk_id": str(uuid.uuid4()),
                    "book_id": book_id,
                    "title": split_chunk["title"],
                    "path": " > ".join(split_chunk["path"]),
                    "path_array": split_chunk["path"],
                    "level": split_chunk["level"],
                    "start_page": split_chunk["start_page"],
                    "end_page": split_chunk.get("end_page", end_page_exclusive - 1),
                    "end_page_exclusive": end_page_exclusive,
                    "token_count": split_chunk["token_count"],
                    "text": split_chunk["text"]
                }
                all_chunks.append(final_chunk)
        else:
            chunk["chunk_id"] = str(uuid.uuid4())
            chunk["book_id"] = book_id
            all_chunks.append(chunk)
    
    # Save mini-PDFs if requested
    if save_mini_pdfs:
        pdf_dir = os.path.join(output_dir, "mini_pdfs")
        os.makedirs(pdf_dir, exist_ok=True)
        
        for chunk in all_chunks:
            # Ensure end_page_exclusive exists (convert from inclusive end_page if needed)
            end_page_exclusive = chunk.get("end_page_exclusive")
            if end_page_exclusive is None:
                end_page_exclusive = chunk["end_page"] + 1  # Convert inclusive to exclusive
            
            mini_doc = fitz.open()
            for page_num in range(chunk["start_page"] - 1, end_page_exclusive - 1):
                if 0 <= page_num < len(doc):
                    mini_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
            
            filename = f"{chunk['chunk_id']}_{chunk['start_page']}_{chunk['end_page']}.pdf"
            filepath = os.path.join(pdf_dir, filename)
            mini_doc.save(filepath)
            mini_doc.close()
            
            chunk["filename"] = filename
            chunk["local_path"] = filepath
            chunk["end_page_exclusive"] = end_page_exclusive  # Ensure it's set
    
    doc.close()
    
    # Prepare result
    avg_tokens = sum(c["token_count"] for c in all_chunks) / len(all_chunks) if all_chunks else 0
    
    result = {
        "metadata": {
            "book_id": book_id,
            "source_file": pdf_path,
            "total_pages": total_pages,
            "total_sections": len(all_sections),
            "excluded_sections": len(excluded_list),
            "total_chunks": len(all_chunks),
            "avg_tokens_per_chunk": round(avg_tokens, 0),
            "processing_date": datetime.now().isoformat(),
            "parameters": {
                "min_tokens": min_tokens,
                "max_tokens": max_tokens,
                "exclude_sections": exclude_sections
            }
        },
        "chunks": all_chunks,
        "excluded_sections": excluded_list
    }
    
    # Save to JSON
    json_path = os.path.join(output_dir, f"{book_id}_chunks.json")
    os.makedirs(output_dir, exist_ok=True)
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    # Summary
    print(f"\n✅ Processing complete!")
    print(f"📊 Statistics:")
    print(f"   • Total sections: {len(all_sections)}")
    print(f"   • Excluded: {len(excluded_list)}")
    print(f"   • Final chunks: {len(all_chunks)}")
    print(f"   • Avg tokens/chunk: {avg_tokens:.0f}")
    print(f"\n💾 Saved to: {json_path}")
    
    return result


# ============================================================================
# CLI
# ============================================================================

def main():
    """Command-line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Extract semantic chunks from PDF based on TOC structure'
    )
    
    parser.add_argument('pdf_path', help='Path to PDF file')
    parser.add_argument('--output-dir', default='output', help='Output directory (default: output)')
    parser.add_argument('--book-id', help='Book identifier (default: UUID)')
    parser.add_argument('--no-mini-pdfs', action='store_true', help='Skip saving mini-PDFs')
    parser.add_argument('--min-tokens', type=int, default=200, help='Minimum tokens per chunk (default: 200)')
    parser.add_argument('--max-tokens', type=int, default=2000, help='Maximum tokens per chunk (default: 2000)')
    parser.add_argument('--no-exclude', action='store_true', help='Don\'t exclude front/back matter')
    parser.add_argument('--exclude-front', action='append', help='Additional front matter patterns (regex)')
    parser.add_argument('--exclude-back', action='append', help='Additional back matter patterns (regex)')
    
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
            min_tokens=args.min_tokens,
            max_tokens=args.max_tokens,
            exclude_sections=not args.no_exclude,
            custom_exclude_front=args.exclude_front,
            custom_exclude_back=args.exclude_back
        )
        return 0
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
