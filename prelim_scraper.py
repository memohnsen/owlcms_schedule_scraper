"""
  python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
  
  # Run dry-run to preview changes
  source venv/bin/activate && python prelim_scraper.py "https://assets.contentstack.io/v3/assets/blteb7d012fc7ebef7f/blt6510a9b96582ea60/6832224011a22b68335f89f6/2025_-_NCW_-_Preliminary_Schedule_v2_(1).pdf" "Nationals" --dry-run
"""

import os
import re
import requests
from io import BytesIO
from datetime import datetime, time as datetime_time
from typing import List, Dict, Optional
import pdfplumber
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
from tabulate import tabulate

# Load environment variables
load_dotenv()


class ScheduleScraper:
    """Scraper for extracting schedule data from OWLCMS PDFs"""
    
    def __init__(self, supabase_url: Optional[str] = None, supabase_key: Optional[str] = None):
        """
        Initialize the scraper with Supabase credentials
        
        Args:
            supabase_url: Supabase project URL (defaults to SUPABASE_URL env var)
            supabase_key: Supabase API key (defaults to SUPABASE_KEY env var)
        """
        self.supabase_url = supabase_url or os.getenv('SUPABASE_URL')
        self.supabase_key = supabase_key or os.getenv('SUPABASE_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Supabase credentials not found. Set SUPABASE_URL and SUPABASE_KEY in .env")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
    
    def download_pdf(self, url: str) -> BytesIO:
        """
        Download PDF from URL
        
        Args:
            url: URL to the PDF file
            
        Returns:
            BytesIO object containing the PDF data
        """
        print(f"Downloading PDF from {url}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return BytesIO(response.content)
    
    def extract_schedule_data(self, pdf_file: BytesIO, meet_name: str) -> List[Dict]:
        """
        Extract schedule data from PDF
        
        Args:
            pdf_file: BytesIO object containing PDF data
            meet_name: Name of the meet/competition
            
        Returns:
            List of dictionaries containing schedule entries
        """
        print("Extracting data from PDF...")
        schedule_entries = []
        
        # Track date and session across all tables and pages
        self.current_date = None
        self.current_session = None
        
        with pdfplumber.open(pdf_file) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                print(f"Processing page {page_num}/{len(pdf.pages)}...")
                
                # Extract tables from the page
                tables = page.extract_tables()
                
                if not tables:
                    # Try extracting text if no tables found
                    text = page.extract_text()
                    if text:
                        print(f"No tables found on page {page_num}, text extraction:")
                        print(text[:500])  # Print first 500 chars for debugging
                    continue
                
                # Process each table
                for table in tables:
                    if not table or len(table) < 2:
                        continue
                    
                    # Parse the table data
                    entries = self._parse_table(table, meet_name)
                    schedule_entries.extend(entries)
        
        print(f"Extracted {len(schedule_entries)} schedule entries")
        return schedule_entries
    
    def _parse_table(self, table: List[List], meet_name: str) -> List[Dict]:
        """
        Parse a table from the PDF into schedule entries
        
        Args:
            table: 2D list representing a table
            meet_name: Name of the meet
            
        Returns:
            List of schedule entry dictionaries
        """
        entries = []
        
        # A table can have MULTIPLE header rows (multiple date sections)
        # We need to find ALL headers and process each section separately
        header_sections = []
        
        for idx, row in enumerate(table):
            if not row or all(cell is None or str(cell).strip() == '' for cell in row):
                continue
            
            row_text = ' '.join([str(cell or '').lower() for cell in row])
            
            # Look for common header keywords - must have multiple keywords to be a real header
            keywords_found = sum([
                'sess' in row_text,
                'date' in row_text,
                'plat' in row_text,
                'weigh' in row_text,
                'weight' in row_text and 'category' in row_text
            ])
            
            if keywords_found >= 4:  # Must have at least 4 matching keywords
                # Look for date in rows BEFORE this header (within 3 rows)
                date_for_section = None
                for look_back in range(1, min(4, idx + 1)):
                    prev_row = table[idx - look_back]
                    if prev_row:
                        prev_text = ' '.join([str(cell or '').strip() for cell in prev_row])
                        if any(month in prev_text for month in ['January', 'February', 'March', 'April', 'May', 'June', 
                                                                  'July', 'August', 'September', 'October', 'November', 'December']):
                            date_for_section = prev_text
                            break
                
                header_sections.append({
                    'header_row': row, 
                    'start_idx': idx + 1,
                    'date_text': date_for_section
                })
        
        # Process each section
        if header_sections:
            for i, section in enumerate(header_sections):
                # Determine where this section ends (next header or end of table)
                end_idx = header_sections[i + 1]['start_idx'] - 1 if i + 1 < len(header_sections) else len(table)
                data_rows = table[section['start_idx']:end_idx]
                
                entries.extend(self._parse_with_headers(data_rows, section['header_row'], meet_name, section.get('date_text')))
        else:
            # Try to parse without explicit headers
            print("No clear header row found, attempting pattern-based parsing...")
            entries.extend(self._parse_without_headers(table, meet_name))
        
        return entries
    
    def _parse_with_headers(self, data_rows: List[List], headers: List, meet_name: str, date_text: Optional[str] = None) -> List[Dict]:
        """Parse table data using identified headers"""
        entries = []
        
        # Normalize headers
        header_map = {}
        for idx, header in enumerate(headers):
            if header:
                header_lower = str(header).lower().strip().replace('\n', ' ')
                # Map to column indices - order matters! Check specific combinations first
                if 'weight' in header_lower and 'category' in header_lower:
                    header_map['weight_class_idx'] = idx
                elif header_lower == 'weigh':  # Exact match for weigh-in time column
                    header_map['weigh_idx'] = idx
                elif 'date' in header_lower:
                    header_map['date_idx'] = idx
                elif 'sess' in header_lower:
                    header_map['session_idx'] = idx
                elif 'plat' in header_lower:
                    header_map['platform_idx'] = idx
                elif 'time' in header_lower:
                    header_map['time_idx'] = idx
        
        
        # When starting a new table with headers, look for a fresh date
        # First try the date_text passed in (from before the header)
        # Then scan the data rows
        local_date = None
        local_session = None
        
        # Try parsing date_text first (from row before header)
        if date_text:
            parsed_date = self._parse_date_from_text(date_text)
            if parsed_date:
                local_date = parsed_date
                self.current_date = parsed_date
        
        # If no date yet, scan the initial rows to find the date in the Date column
        if not local_date:
            date_idx = header_map.get('date_idx', 0)
            for row in data_rows[:10]:  # Check first 10 rows for date
                if not row or len(row) <= date_idx:
                    continue
                date_cell = str(row[date_idx] or '').strip()
                if date_cell and any(month in date_cell for month in ['January', 'February', 'March', 'April', 'May', 'June', 
                                                                        'July', 'August', 'September', 'October', 'November', 'December']):
                    parsed_date = self._parse_date_from_text(date_cell)
                    if parsed_date:
                        local_date = parsed_date
                        self.current_date = parsed_date
                        break
        
        for row in data_rows:
            if not row or all(cell is None or str(cell).strip() == '' for cell in row):
                continue
            
            try:
                # Use local_date if we found one, otherwise fall back to instance level
                entry = self._extract_entry_from_row(
                    row, header_map, meet_name, 
                    local_date or self.current_date, 
                    local_session or self.current_session
                )
                if entry:
                    # Update tracking
                    if entry.get('date'):
                        local_date = entry['date']
                        self.current_date = entry['date']
                    if entry.get('session_id'):
                        local_session = entry['session_id']
                        self.current_session = entry['session_id']
                    entries.append(entry)
            except Exception as e:
                print(f"Error parsing row {row}: {e}")
                continue
        
        return entries
    
    def _parse_without_headers(self, table: List[List], meet_name: str) -> List[Dict]:
        """Parse table data without explicit headers using pattern matching"""
        entries = []
        
        for row in table:
            if not row or len(row) < 4:
                continue
            
            try:
                # Try to identify date, time, session, platform, weight class patterns
                entry = self._extract_entry_from_row_pattern(row, meet_name)
                if entry:
                    entries.append(entry)
            except Exception as e:
                print(f"Error parsing row {row}: {e}")
                continue
        
        return entries
    
    def _extract_entry_from_row(self, row: List, header_map: Dict, meet_name: str, 
                                 current_date: Optional[str] = None, 
                                 current_session: Optional[int] = None) -> Optional[Dict]:
        """Extract a single entry from a row using header mapping"""
        
        # Get indices from header map
        date_idx = header_map.get('date_idx', 0)
        session_idx = header_map.get('session_idx', 1)
        platform_idx = header_map.get('platform_idx', 2)
        weigh_idx = header_map.get('weigh_idx', 3)
        time_idx = header_map.get('time_idx', 4)
        weight_class_idx = header_map.get('weight_class_idx', 7)
        
        # Extract raw values
        date_str = str(row[date_idx] or '').strip() if date_idx < len(row) else ''
        session_str = str(row[session_idx] or '').strip() if session_idx < len(row) else ''
        platform = str(row[platform_idx] or '').strip() if platform_idx < len(row) else ''
        weigh_time_str = str(row[weigh_idx] or '').strip() if weigh_idx < len(row) else ''
        start_time_str = str(row[time_idx] or '').strip() if time_idx < len(row) else ''
        weight_class = str(row[weight_class_idx] or '').strip() if weight_class_idx < len(row) else ''
        
        # Skip rows that don't have essential data
        if not platform or platform not in ['Red', 'White', 'Blue']:
            return None
        
        if not start_time_str or not weigh_time_str:
            return None
        
        # Use current date/session if not provided in this row
        if date_str and any(month in date_str for month in ['January', 'February', 'March', 'April', 'May', 'June', 
                                                              'July', 'August', 'September', 'October', 'November', 'December']):
            parsed_date = self._parse_date_from_text(date_str)
            if parsed_date:
                current_date = parsed_date
        
        if session_str and session_str.isdigit():
            current_session = int(session_str)
        
        # Must have date and session to create entry
        if not current_date or not current_session:
            return None
        
        # Parse times
        start_time = self._parse_time(start_time_str)
        weigh_time = self._parse_time(weigh_time_str)
        
        if not start_time or not weigh_time:
            return None
        
        # Clean up weight class (remove group letter like A, B, C, etc)
        weight_class = re.sub(r'\s+[A-E]$', '', weight_class).strip()
        
        return {
            'date': current_date,
            'session_id': current_session,
            'start_time': start_time.strftime('%H:%M:%S'),
            'weigh_in_time': weigh_time.strftime('%H:%M:%S'),
            'platform': platform,
            'weight_class': weight_class,
            'meet': meet_name
        }
    
    def _extract_entry_from_row_pattern(self, row: List, meet_name: str) -> Optional[Dict]:
        """Extract a single entry from a row using pattern matching"""
        # This is a placeholder - actual implementation depends on PDF structure
        return None
    
    def _parse_time(self, time_str: str) -> Optional[datetime_time]:
        """Parse time string into time object"""
        if not time_str:
            return None
        
        time_str = str(time_str).strip()
        
        # Try common time formats
        formats = ['%H:%M:%S', '%H:%M', '%I:%M %p', '%I:%M%p']
        
        for fmt in formats:
            try:
                dt = datetime.strptime(time_str, fmt)
                return dt.time()
            except ValueError:
                continue
        
        # Try to extract time using regex
        match = re.search(r'(\d{1,2}):(\d{2})\s*(am|pm)?', time_str.lower())
        if match:
            hour = int(match.group(1))
            minute = int(match.group(2))
            am_pm = match.group(3)
            
            if am_pm == 'pm' and hour < 12:
                hour += 12
            elif am_pm == 'am' and hour == 12:
                hour = 0
            
            return datetime_time(hour, minute)
        
        return None
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string into YYYY-MM-DD format"""
        if not date_str:
            return None
        
        date_str = str(date_str).strip()
        
        # Try common date formats
        formats = ['%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%B %d, %Y', '%b %d, %Y']
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        return None
    
    def _parse_date_from_text(self, date_str: str) -> Optional[str]:
        """Parse date from text format like 'Saturday June 21, 2025'"""
        if not date_str:
            return None
        
        date_str = str(date_str).strip()
        
        # Try to extract month day year pattern
        match = re.search(r'([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})', date_str)
        if match:
            month_str = match.group(1)
            day = match.group(2)
            year = match.group(3)
            
            try:
                dt = datetime.strptime(f"{month_str} {day} {year}", "%B %d %Y")
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                pass
        
        # Fallback to regular parsing
        return self._parse_date(date_str)
    
    def format_for_database(self, entries: List[Dict]) -> List[Dict]:
        """
        Format extracted entries to match database schema
        
        Args:
            entries: List of raw schedule entries
            
        Returns:
            List of formatted entries ready for database insertion
        """
        formatted = []
        
        for entry in entries:
            formatted_entry = {
                'date': entry.get('date'),
                'session_id': entry.get('session_id'),
                'start_time': entry.get('start_time'),
                'weigh_in_time': entry.get('weigh_in_time'),
                'platform': entry.get('platform'),
                'weight_class': entry.get('weight_class'),
                'meet': entry.get('meet')
            }
            
            # Only add if all required fields are present
            if all(formatted_entry.values()):
                formatted.append(formatted_entry)
        
        return formatted
    
    def dry_run(self, meet_name: str, new_entries: List[Dict]) -> Dict:
        """
        Perform a dry run to see what would be changed
        
        Args:
            meet_name: Name of the meet
            new_entries: List of new entries to be upserted
            
        Returns:
            Dictionary with statistics about what would change
        """
        print(f"\n{'='*60}")
        print(f"DRY RUN: {meet_name}")
        print(f"{'='*60}\n")
        
        # Query existing records for this meet
        existing_response = self.supabase.table('session_schedule').select('*').eq('meet', meet_name).execute()
        existing_records = existing_response.data if existing_response.data else []
        
        print(f"Found {len(existing_records)} existing records for '{meet_name}'")
        print(f"Processing {len(new_entries)} new entries\n")
        
        # Create a comparison
        existing_by_key = {}
        for record in existing_records:
            key = (
                record.get('date'),
                record.get('session_id'),
                record.get('platform'),
                record.get('weight_class')
            )
            existing_by_key[key] = record
        
        new_by_key = {}
        for entry in new_entries:
            key = (
                entry.get('date'),
                entry.get('session_id'),
                entry.get('platform'),
                entry.get('weight_class')
            )
            new_by_key[key] = entry
        
        # Determine what would be added/updated
        to_add = []
        to_update = []
        unchanged = []
        
        for key, new_entry in new_by_key.items():
            if key not in existing_by_key:
                to_add.append(new_entry)
            else:
                existing = existing_by_key[key]
                # Check if values differ
                if (existing.get('start_time') != new_entry.get('start_time') or
                    existing.get('weigh_in_time') != new_entry.get('weigh_in_time')):
                    to_update.append({
                        'existing': existing,
                        'new': new_entry
                    })
                else:
                    unchanged.append(new_entry)
        
        # Display summary
        print(f"SUMMARY:")
        print(f"  New entries to add: {len(to_add)}")
        print(f"  Existing entries to update: {len(to_update)}")
        print(f"  Unchanged entries: {len(unchanged)}")
        
        # Show details
        if to_add:
            print(f"\n{'='*60}")
            print(f"NEW ENTRIES TO ADD ({len(to_add)}):")
            print(f"{'='*60}")
            df = pd.DataFrame(to_add)
            print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
        
        if to_update:
            print(f"\n{'='*60}")
            print(f"ENTRIES TO UPDATE ({len(to_update)}):")
            print(f"{'='*60}")
            for item in to_update:
                print(f"\nExisting:")
                print(f"  {item['existing']}")
                print(f"New:")
                print(f"  {item['new']}")
        
        if unchanged:
            print(f"\n{'='*60}")
            print(f"UNCHANGED ENTRIES ({len(unchanged)}):")
            print(f"{'='*60}")
            df = pd.DataFrame(unchanged)
            print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
        
        return {
            'total_new': len(new_entries),
            'total_existing': len(existing_records),
            'to_add': len(to_add),
            'to_update': len(to_update),
            'unchanged': len(unchanged),
            'details': {
                'to_add': to_add,
                'to_update': to_update,
                'unchanged': unchanged
            }
        }
    
    def upsert_to_database(self, entries: List[Dict]) -> Dict:
        """
        Upsert entries to the database
        
        Args:
            entries: List of formatted entries
            
        Returns:
            Response from Supabase
        """
        if not entries:
            print("No entries to upsert")
            return {'data': [], 'count': 0}
        
        print(f"Upserting {len(entries)} entries to database...")
        
        # Supabase upsert - will insert or update based on unique constraints
        response = self.supabase.table('session_schedule').upsert(
            entries,
            on_conflict='date,session_id,platform,weight_class'
        ).execute()
        
        print(f"Successfully upserted {len(entries)} entries")
        return response
    
    def scrape_and_upsert(self, pdf_url: str, meet_name: str, dry_run: bool = False) -> Dict:
        """
        Main method to scrape PDF and upsert to database
        
        Args:
            pdf_url: URL to the PDF file
            meet_name: Name of the meet
            dry_run: If True, only show what would be changed without actually upserting
            
        Returns:
            Dictionary with results
        """
        try:
            # Download PDF
            pdf_file = self.download_pdf(pdf_url)
            
            # Extract data
            raw_entries = self.extract_schedule_data(pdf_file, meet_name)
            
            if not raw_entries:
                print("WARNING: No schedule entries were extracted from the PDF")
                print("This might mean the PDF format is different than expected.")
                return {'success': False, 'error': 'No data extracted'}
            
            # Format for database
            formatted_entries = self.format_for_database(raw_entries)
            
            if not formatted_entries:
                print("WARNING: No valid entries after formatting")
                return {'success': False, 'error': 'No valid entries after formatting'}
            
            # Dry run or actual upsert
            if dry_run:
                result = self.dry_run(meet_name, formatted_entries)
                return {'success': True, 'dry_run': True, 'stats': result}
            else:
                response = self.upsert_to_database(formatted_entries)
                return {'success': True, 'dry_run': False, 'response': response}
        
        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}


def main():
    """Main entry point for CLI usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape OWLCMS schedule from PDF and upsert to Supabase')
    parser.add_argument('url', help='URL to the PDF file')
    parser.add_argument('meet_name', help='Name of the meet/competition')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without actually upserting')
    
    args = parser.parse_args()
    
    scraper = ScheduleScraper()
    result = scraper.scrape_and_upsert(args.url, args.meet_name, dry_run=args.dry_run)
    
    if result['success']:
        print("\n✓ Operation completed successfully")
    else:
        print(f"\n✗ Operation failed: {result.get('error', 'Unknown error')}")
        exit(1)


if __name__ == '__main__':
    main()

