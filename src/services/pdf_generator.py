"""
PDF Generator Service for Chat Downloads
Generates formatted PDFs with logo, headers, footers, and watermarks
"""
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Optional
import os
from pathlib import Path
import re


class ChatPDFGenerator:
    """Generate PDFs from chat data with formatting, logo, headers, footers, and watermarks."""
    
    def __init__(self, logo_path: Optional[str] = None):
        """
        Initialize the PDF generator.
        
        Args:
            logo_path: Path to the logo image file
        """
        self.logo_path = logo_path
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Setup custom paragraph styles."""
        # Title style - more prominent
        self.styles.add(ParagraphStyle(
            name='ChatTitle',
            parent=self.styles['Heading1'],
            fontSize=28,
            textColor=HexColor('#1e40af'),
            spaceAfter=20,
            spaceBefore=10,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold',
            leading=34
        ))
        
        # Question style - match frontend: rounded blue box with white text
        self.styles.add(ParagraphStyle(
            name='Question',
            parent=self.styles['Normal'],
            fontSize=12,
            textColor=HexColor('#ffffff'),
            spaceAfter=20,
            spaceBefore=25,
            fontName='Helvetica',
            leftIndent=20,
            rightIndent=20,
            borderWidth=0,
            borderPadding=15,
            backColor=HexColor('#2563eb'),  # Blue background like frontend
            alignment=TA_LEFT,
            leading=18
        ))
        
        # Answer style - improved readability
        self.styles.add(ParagraphStyle(
            name='Answer',
            parent=self.styles['Normal'],
            fontSize=11,
            textColor=HexColor('#111827'),
            spaceAfter=25,
            spaceBefore=15,
            leftIndent=0,
            rightIndent=0,
            alignment=TA_LEFT,
            leading=18,
            fontName='Helvetica'
        ))
        
        # Heading styles for markdown
        self.styles.add(ParagraphStyle(
            name='H1',
            parent=self.styles['Heading1'],
            fontSize=20,
            textColor=HexColor('#111827'),
            spaceAfter=12,
            spaceBefore=20,
            fontName='Helvetica-Bold',
            leading=24
        ))
        
        self.styles.add(ParagraphStyle(
            name='H2',
            parent=self.styles['Heading2'],
            fontSize=16,
            textColor=HexColor('#111827'),
            spaceAfter=10,
            spaceBefore=16,
            fontName='Helvetica-Bold',
            leading=20
        ))
        
        self.styles.add(ParagraphStyle(
            name='H3',
            parent=self.styles['Heading3'],
            fontSize=14,
            textColor=HexColor('#111827'),
            spaceAfter=8,
            spaceBefore=12,
            fontName='Helvetica-Bold',
            leading=18
        ))
        
        # Metadata style
        self.styles.add(ParagraphStyle(
            name='Metadata',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=HexColor('#6b7280'),
            spaceAfter=15,
            alignment=TA_CENTER,
            fontName='Helvetica-Oblique'
        ))
        
        # Source documents style
        self.styles.add(ParagraphStyle(
            name='SourceDoc',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=HexColor('#4b5563'),
            spaceAfter=8,
            spaceBefore=5,
            leftIndent=25,
            fontName='Helvetica'
        ))
    
    def _process_markdown(self, text: str) -> List[tuple]:
        """
        Process markdown text and return a list of (style_name, content) tuples.
        This allows proper handling of headings, paragraphs, etc.
        """
        if not text:
            return []
        
        result = []
        lines = text.split('\n')
        current_paragraph = []
        
        for line in lines:
            line_stripped = line.strip()
            
            # Check for headings
            if line_stripped.startswith('###'):
                # H3 heading
                if current_paragraph:
                    result.append(('Answer', '\n'.join(current_paragraph)))
                    current_paragraph = []
                heading_text = line_stripped[3:].strip()
                result.append(('H3', heading_text))
            elif line_stripped.startswith('##'):
                # H2 heading
                if current_paragraph:
                    result.append(('Answer', '\n'.join(current_paragraph)))
                    current_paragraph = []
                heading_text = line_stripped[2:].strip()
                result.append(('H2', heading_text))
            elif line_stripped.startswith('#'):
                # H1 heading
                if current_paragraph:
                    result.append(('Answer', '\n'.join(current_paragraph)))
                    current_paragraph = []
                heading_text = line_stripped[1:].strip()
                result.append(('H1', heading_text))
            elif line_stripped:
                # Regular paragraph line
                current_paragraph.append(line)
            else:
                # Empty line - end current paragraph
                if current_paragraph:
                    result.append(('Answer', '\n'.join(current_paragraph)))
                    current_paragraph = []
        
        # Add any remaining paragraph
        if current_paragraph:
            result.append(('Answer', '\n'.join(current_paragraph)))
        
        return result if result else [('Answer', text)]
    
    def _clean_text(self, text: str, preserve_formatting: bool = True) -> str:
        """Clean and escape text for PDF rendering with proper markdown formatting."""
        if not text:
            return ""
        
        # Remove markdown code blocks but keep content
        text = re.sub(r'```[\w]*\n', '', text)
        text = re.sub(r'```', '', text)
        
        # Escape HTML special characters first
        text = text.replace('&', '&amp;')
        text = text.replace('<', '&lt;')
        text = text.replace('>', '&gt;')
        
        if preserve_formatting:
            # Convert bold (**text**)
            text = re.sub(r'\*\*([^*]+?)\*\*', r'<b>\1</b>', text)
            
            # Convert italic (*text*) - but not if it's part of bold
            text = re.sub(r'(?<!<b>)(?<!</b>)\*([^*<]+?)\*(?!\*)(?!</b>)', r'<i>\1</i>', text)
            
            # Convert inline code (`code`)
            text = re.sub(r'`([^`]+?)`', r'<font name="Courier" size="10">\1</font>', text)
            
            # Convert blockquotes (> text)
            text = re.sub(r'^>\s+(.+?)$', r'<i>\1</i>', text, flags=re.MULTILINE)
        else:
            # Remove all markdown formatting
            text = re.sub(r'\*\*([^*]+?)\*\*', r'\1', text)
            text = re.sub(r'\*([^*]+?)\*', r'\1', text)
            text = re.sub(r'#+\s*', '', text)
            text = re.sub(r'`([^`]+?)`', r'\1', text)
        
        # Remove any remaining unmatched asterisks
        text = re.sub(r'\*+', '', text)
        
        # Convert line breaks
        text = text.replace('\n', '<br/>')
        
        return text
    
    def _add_header_footer(self, canvas_obj, doc):
        """Add professional header and footer to each page."""
        canvas_obj.saveState()
        width, height = letter
        
        # Header background with gradient effect
        header_height = 0.85 * inch
        canvas_obj.setFillColor(HexColor('#1e40af'))
        canvas_obj.rect(0, height - header_height, width, header_height, fill=1, stroke=0)
        
        # Header content
        header_y = height - 0.45 * inch
        
        # Add logo to header if available
        logo_x = 0.75 * inch
        if self.logo_path and os.path.exists(self.logo_path):
            try:
                logo_size = 0.55 * inch
                logo_y = height - 0.75 * inch
                canvas_obj.drawImage(
                    ImageReader(self.logo_path),
                    x=logo_x,
                    y=logo_y,
                    width=logo_size,
                    height=logo_size,
                    preserveAspectRatio=True,
                    mask='auto'
                )
                logo_x = 1.5 * inch  # Adjust text position if logo is present
            except Exception as e:
                print(f"Warning: Could not add logo to header: {e}")
                logo_x = 0.75 * inch
        
        # Header text - white on blue background
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.setFillColor(HexColor('#ffffff'))
        canvas_obj.drawString(logo_x, header_y, "FastCite")
        canvas_obj.setFont("Helvetica", 9)
        canvas_obj.setFillColor(HexColor('#bfdbfe'))
        canvas_obj.drawString(logo_x, header_y - 14, "Study Smarter, Grade Higher")
        
        # Footer with line
        footer_y = 0.6 * inch
        footer_line_y = 0.75 * inch
        
        # Footer line
        canvas_obj.setStrokeColor(HexColor('#e5e7eb'))
        canvas_obj.setLineWidth(0.5)
        canvas_obj.line(0.75 * inch, footer_line_y, width - 0.75 * inch, footer_line_y)
        
        # Footer text
        canvas_obj.setFont("Helvetica", 8)
        canvas_obj.setFillColor(HexColor('#6b7280'))
        page_num = canvas_obj.getPageNumber()
        footer_text = f"Page {page_num} | Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')} | FastCite"
        canvas_obj.drawCentredString(width / 2, footer_y, footer_text)
        
        canvas_obj.restoreState()
    
    def _add_watermark(self, canvas_obj, doc):
        """Add subtle watermark to each page."""
        canvas_obj.saveState()
        width, height = letter
        
        # Set watermark properties - more subtle
        canvas_obj.setFont("Helvetica-Bold", 72)
        canvas_obj.setFillColor(HexColor('#f3f4f6'), alpha=0.15)
        canvas_obj.rotate(45)
        
        # Draw watermark text
        watermark_text = "FASTCITE"
        canvas_obj.drawCentredString(width / 2, -height / 2, watermark_text)
        
        canvas_obj.restoreState()
    
    def generate_chat_pdf(
        self,
        chat_data: Dict,
        output_stream: BytesIO,
        include_all_messages: bool = True,
        message_index: Optional[int] = None
    ) -> BytesIO:
        """
        Generate a PDF from chat data.
        
        Args:
            chat_data: Dictionary containing chat information (title, messages, etc.)
            output_stream: BytesIO stream to write PDF to
            include_all_messages: If True, include all messages; if False, include only one
            message_index: Index of message to include if include_all_messages is False
            
        Returns:
            BytesIO stream containing the PDF
        """
        doc = SimpleDocTemplate(
            output_stream,
            pagesize=letter,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=1.2*inch,
            bottomMargin=1*inch
        )
        
        story = []
        
        # Add logo at the top if available
        if self.logo_path and os.path.exists(self.logo_path):
            try:
                logo = Image(self.logo_path, width=1.5*inch, height=1.5*inch)
                story.append(logo)
                story.append(Spacer(1, 0.1*inch))
            except Exception as e:
                print(f"Warning: Could not add logo: {e}")
        
        # Chat title with better styling
        title = chat_data.get('title', 'Chat Conversation')
        story.append(Paragraph(self._clean_text(title), self.styles['ChatTitle']))
        story.append(Spacer(1, 0.2*inch))
        
        # Chat metadata in a table for better appearance
        metadata_parts = []
        if chat_data.get('created_at'):
            try:
                created_at = chat_data['created_at']
                if isinstance(created_at, str):
                    if created_at.endswith('Z'):
                        created_at = created_at.replace('Z', '+00:00')
                    created_date = datetime.fromisoformat(created_at)
                elif isinstance(created_at, datetime):
                    created_date = created_at
                else:
                    created_date = None
                if created_date:
                    metadata_parts.append(f"Created: {created_date.strftime('%B %d, %Y at %I:%M %p')}")
            except (ValueError, AttributeError) as e:
                print(f"Warning: Could not parse created_at: {e}")
        
        if chat_data.get('updated_at'):
            try:
                updated_at = chat_data['updated_at']
                if isinstance(updated_at, str):
                    if updated_at.endswith('Z'):
                        updated_at = updated_at.replace('Z', '+00:00')
                    updated_date = datetime.fromisoformat(updated_at)
                elif isinstance(updated_at, datetime):
                    updated_date = updated_at
                else:
                    updated_date = None
                if updated_date:
                    metadata_parts.append(f"Last Updated: {updated_date.strftime('%B %d, %Y at %I:%M %p')}")
            except (ValueError, AttributeError) as e:
                print(f"Warning: Could not parse updated_at: {e}")
        
        if chat_data.get('book_name'):
            metadata_parts.append(f"Book: {chat_data['book_name']}")
        
        if metadata_parts:
            # Create styled metadata display
            metadata_text = " | ".join(metadata_parts)
            story.append(Paragraph(self._clean_text(metadata_text), self.styles['Metadata']))
            story.append(Spacer(1, 0.3*inch))
        
        # Messages
        messages = chat_data.get('messages', [])
        
        if include_all_messages:
            # Include all messages
            for idx, msg in enumerate(messages):
                if msg.get('question'):
                    # Question styled like frontend: blue rounded box with white text
                    question_text = self._clean_text(msg['question'], preserve_formatting=False)
                    story.append(Paragraph(question_text, self.styles['Question']))
                
                if msg.get('answer'):
                    # Process answer with markdown formatting
                    markdown_parts = self._process_markdown(msg['answer'])
                    for style_name, content in markdown_parts:
                        if content.strip():
                            cleaned_content = self._clean_text(content, preserve_formatting=True)
                            story.append(Paragraph(cleaned_content, self.styles[style_name]))
                
                # Add source documents if available
                if msg.get('downloaded_files') and len(msg['downloaded_files']) > 0:
                    story.append(Spacer(1, 0.1*inch))
                    sources_header = Paragraph(
                        f"<b>Source Documents ({len(msg['downloaded_files'])}):</b>",
                        self.styles['Answer']
                    )
                    story.append(sources_header)
                    for file in msg['downloaded_files']:
                        file_name = file.get('name', 'Unknown Document')
                        source_text = f"• {self._clean_text(file_name)}"
                        story.append(Paragraph(source_text, self.styles['SourceDoc']))
                
                story.append(Spacer(1, 0.4*inch))
                
                # Add page break between major sections (every 2 Q&A pairs)
                if (idx + 1) % 2 == 0 and idx < len(messages) - 1:
                    story.append(PageBreak())
        else:
            # Include only one message
            if message_index is not None and 0 <= message_index < len(messages):
                msg = messages[message_index]
                if msg.get('question'):
                    # Question styled like frontend
                    question_text = self._clean_text(msg['question'], preserve_formatting=False)
                    story.append(Paragraph(question_text, self.styles['Question']))
                
                if msg.get('answer'):
                    # Process answer with markdown formatting
                    markdown_parts = self._process_markdown(msg['answer'])
                    for style_name, content in markdown_parts:
                        if content.strip():
                            cleaned_content = self._clean_text(content, preserve_formatting=True)
                            story.append(Paragraph(cleaned_content, self.styles[style_name]))
                
                # Add source documents if available
                if msg.get('downloaded_files') and len(msg['downloaded_files']) > 0:
                    story.append(Spacer(1, 0.1*inch))
                    sources_header = Paragraph(
                        f"<b>Source Documents ({len(msg['downloaded_files'])}):</b>",
                        self.styles['Answer']
                    )
                    story.append(sources_header)
                    for file in msg['downloaded_files']:
                        file_name = file.get('name', 'Unknown Document')
                        source_text = f"• {self._clean_text(file_name)}"
                        story.append(Paragraph(source_text, self.styles['SourceDoc']))
        
        # Build PDF with header, footer, and watermark
        def on_first_page(canvas_obj, doc):
            self._add_header_footer(canvas_obj, doc)
            self._add_watermark(canvas_obj, doc)
        
        def on_later_pages(canvas_obj, doc):
            self._add_header_footer(canvas_obj, doc)
            self._add_watermark(canvas_obj, doc)
        
        doc.build(story, onFirstPage=on_first_page, onLaterPages=on_later_pages)
        
        return output_stream
