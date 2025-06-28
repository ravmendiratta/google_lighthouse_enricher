#!/usr/bin/env python3
"""
PageSpeed Insights CSV Enricher

A reusable script to enrich any CSV file with PageSpeed Insights data.
Adds desktop and mobile speed index columns for all URLs in the CSV.

Features:
- Parallel processing for speed
- Error tracking and logging
- Resume capability
- Automatic URL column detection
- Progress saving and recovery

Usage:
    python pagespeed_csv_enricher.py input.csv output.csv API_KEY [options]

Requirements:
    pip install pandas requests
"""

import requests
import json
import time
import sys
import pandas as pd
import argparse
from typing import Dict, Optional, List, Tuple
import logging
import pickle
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime
import csv
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PageSpeedEnricher:
    """Main class for enriching CSV files with PageSpeed Insights data"""
    
    def __init__(self, api_key: str, max_workers: int = 5, timeout: int = 30):
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/pagespeed/api/runpagespeed"
        self.max_workers = max_workers
        self.timeout = timeout
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
        self.lock = threading.Lock()
        
        # Error tracking
        self.errors = []
        
    def detect_url_column(self, df: pd.DataFrame) -> str:
        """
        Automatically detect the URL column in the DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            Name of the URL column
            
        Raises:
            ValueError: If no URL column is found
        """
        # Common URL column names
        url_column_names = ['url', 'URL', 'link', 'Link', 'website', 'Website', 'domain', 'Domain']
        
        # Check for exact matches first
        for col_name in url_column_names:
            if col_name in df.columns:
                logger.info(f"Found URL column: '{col_name}'")
                return col_name
        
        # Check for partial matches
        for col in df.columns:
            col_lower = col.lower()
            if any(url_name.lower() in col_lower for url_name in url_column_names):
                logger.info(f"Found URL column: '{col}' (partial match)")
                return col
        
        # Check if any column contains URL-like data
        for col in df.columns:
            if df[col].dtype == 'object':  # String column
                sample_values = df[col].dropna().head(10)
                url_like_count = 0
                for value in sample_values:
                    if isinstance(value, str) and ('http' in value or 'www.' in value or '.com' in value):
                        url_like_count += 1
                
                if url_like_count >= len(sample_values) * 0.7:  # 70% of samples look like URLs
                    logger.info(f"Found URL column: '{col}' (content analysis)")
                    return col
        
        raise ValueError("Could not detect URL column. Please specify manually using --url-column parameter.")
    
    def validate_url(self, url: str) -> bool:
        """
        Validate if a string is a proper URL
        
        Args:
            url: URL string to validate
            
        Returns:
            True if valid URL, False otherwise
        """
        try:
            if not isinstance(url, str):
                return False
            
            # Add protocol if missing
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
    
    def normalize_url(self, url: str) -> str:
        """
        Normalize URL by adding protocol if missing
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL
        """
        if not isinstance(url, str):
            return url
        
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        return url
    
    def get_speed_index(self, url: str, strategy: str) -> Optional[float]:
        """
        Get speed index for a single URL and strategy
        
        Args:
            url: URL to analyze
            strategy: 'desktop' or 'mobile'
            
        Returns:
            Speed index in milliseconds or None if failed
        """
        params = {
            'url': url,
            'key': self.api_key,
            'strategy': strategy,
            'category': 'performance'
        }
        
        try:
            # Rate limiting delay
            time.sleep(0.2)
            
            with self.lock:
                self.request_count += 1
                request_num = self.request_count
            
            response = requests.get(
                self.base_url,
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    # Extract speed index from lighthouse results
                    lighthouse_result = data.get('lighthouseResult', {})
                    audits = lighthouse_result.get('audits', {})
                    speed_index_audit = audits.get('speed-index', {})
                    
                    if 'numericValue' in speed_index_audit:
                        speed_index = speed_index_audit['numericValue']
                        with self.lock:
                            self.success_count += 1
                        logger.debug(f"✅ Request #{request_num}: {url} ({strategy}) = {speed_index:.0f}ms")
                        return speed_index
                    else:
                        self._log_error(url, strategy, "missing_speed_index", 200)
                        return None
                        
                except json.JSONDecodeError as e:
                    self._log_error(url, strategy, "json_decode_error", 200, str(e))
                    return None
                    
            elif response.status_code == 429:
                self._log_error(url, strategy, "rate_limit", 429)
                logger.warning(f"Rate limit hit for {url} ({strategy}) - waiting...")
                time.sleep(2)
                return None
                
            elif response.status_code == 400:
                self._log_error(url, strategy, "bad_request", 400)
                return None
                
            elif response.status_code == 404:
                self._log_error(url, strategy, "not_found", 404)
                return None
                
            else:
                self._log_error(url, strategy, "http_error", response.status_code)
                return None
                
        except requests.exceptions.Timeout:
            self._log_error(url, strategy, "timeout", None)
            return None
        except requests.exceptions.RequestException as e:
            self._log_error(url, strategy, "request_exception", None, str(e))
            return None
        except Exception as e:
            self._log_error(url, strategy, "unexpected_error", None, str(e))
            return None
    
    def _log_error(self, url: str, strategy: str, error_type: str, status_code: int = None, message: str = ""):
        """Log an error"""
        with self.lock:
            self.error_count += 1
            self.errors.append({
                'url': url,
                'strategy': strategy,
                'error_type': error_type,
                'status_code': status_code,
                'message': message,
                'timestamp': datetime.now().isoformat()
            })
    
    def process_url_both_strategies(self, url: str) -> Dict[str, Optional[float]]:
        """
        Process a single URL for both desktop and mobile strategies
        
        Args:
            url: URL to analyze
            
        Returns:
            Dictionary with desktop and mobile speed index values
        """
        # Normalize URL
        normalized_url = self.normalize_url(url)
        
        # Validate URL
        if not self.validate_url(normalized_url):
            logger.warning(f"Invalid URL: {url}")
            return {
                'desktop_speed_index': None,
                'mobile_speed_index': None
            }
        
        results = {
            'desktop_speed_index': None,
            'mobile_speed_index': None
        }
        
        # Get desktop speed index
        desktop_speed = self.get_speed_index(normalized_url, 'desktop')
        results['desktop_speed_index'] = desktop_speed
        
        # Small delay between requests
        time.sleep(0.1)
        
        # Get mobile speed index
        mobile_speed = self.get_speed_index(normalized_url, 'mobile')
        results['mobile_speed_index'] = mobile_speed
        
        return results
    
    def save_progress(self, speed_data: Dict, filename: str):
        """Save progress to a pickle file"""
        try:
            with open(filename, 'wb') as f:
                pickle.dump(speed_data, f)
            logger.debug(f"Progress saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save progress: {str(e)}")
    
    def load_progress(self, filename: str) -> Dict:
        """Load progress from a pickle file"""
        try:
            if os.path.exists(filename):
                with open(filename, 'rb') as f:
                    data = pickle.load(f)
                logger.info(f"Loaded progress: {len(data)} URLs")
                return data
            else:
                return {}
        except Exception as e:
            logger.warning(f"Could not load progress: {str(e)}")
            return {}
    
    def save_error_log(self, filename: str):
        """Save error log to CSV file"""
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                if self.errors:
                    writer = csv.DictWriter(f, fieldnames=['timestamp', 'url', 'strategy', 'error_type', 'status_code', 'message'])
                    writer.writeheader()
                    writer.writerows(self.errors)
            logger.info(f"Error log saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save error log: {str(e)}")
    
    def process_csv(self, input_file: str, output_file: str, url_column: str = None, 
                   resume: bool = True) -> Tuple[bool, Dict]:
        """
        Process CSV file and add PageSpeed data
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            url_column: Name of URL column (auto-detected if None)
            resume: Whether to resume from previous progress
            
        Returns:
            Tuple of (success, statistics)
        """
        logger.info(f"🚀 Starting PageSpeed enrichment")
        logger.info(f"📁 Input: {input_file}")
        logger.info(f"📁 Output: {output_file}")
        
        # Read CSV file
        try:
            df = pd.read_csv(input_file)
            logger.info(f"📊 Loaded {len(df)} rows from CSV")
        except Exception as e:
            logger.error(f"Failed to read CSV file: {str(e)}")
            return False, {}
        
        # Detect URL column
        if url_column is None:
            try:
                url_column = self.detect_url_column(df)
            except ValueError as e:
                logger.error(str(e))
                return False, {}
        elif url_column not in df.columns:
            logger.error(f"Specified URL column '{url_column}' not found in CSV")
            return False, {}
        
        # Get unique URLs
        unique_urls = df[url_column].dropna().unique().tolist()
        logger.info(f"🔗 Found {len(unique_urls)} unique URLs")
        
        # Setup progress file
        progress_file = f"{output_file}.progress"
        error_file = f"{output_file}.errors.csv"
        
        # Load existing progress if resuming
        speed_data = {}
        if resume:
            speed_data = self.load_progress(progress_file)
        
        # Filter URLs that need processing
        urls_to_process = []
        for url in unique_urls:
            if url not in speed_data:
                urls_to_process.append(url)
            else:
                # Check if we have complete data
                existing = speed_data[url]
                if (existing.get('desktop_speed_index') is None or 
                    existing.get('mobile_speed_index') is None):
                    urls_to_process.append(url)
        
        if not urls_to_process:
            logger.info("✅ All URLs already processed!")
        else:
            logger.info(f"🔄 Processing {len(urls_to_process)} URLs with {self.max_workers} workers")
            
            # Process URLs in parallel
            completed_count = len(speed_data)
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_url = {
                    executor.submit(self.process_url_both_strategies, url): url 
                    for url in urls_to_process
                }
                
                # Process results
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        result = future.result()
                        speed_data[url] = result
                        completed_count += 1
                        
                        # Log progress
                        desktop = result.get('desktop_speed_index')
                        mobile = result.get('mobile_speed_index')
                        desktop_str = f"{desktop:.0f}ms" if desktop else "FAILED"
                        mobile_str = f"{mobile:.0f}ms" if mobile else "FAILED"
                        
                        status = "✅" if (desktop and mobile) else "⚠️" if (desktop or mobile) else "❌"
                        logger.info(f"{status} {completed_count}/{len(unique_urls)}: {url[:50]}... D:{desktop_str} M:{mobile_str}")
                        
                        # Save progress periodically
                        if completed_count % 10 == 0:
                            self.save_progress(speed_data, progress_file)
                            success_rate = self.success_count / self.request_count * 100 if self.request_count > 0 else 0
                            logger.info(f"📊 Progress: {completed_count}/{len(unique_urls)} ({completed_count/len(unique_urls)*100:.1f}%) | Success: {success_rate:.1f}%")
                    
                    except Exception as e:
                        logger.error(f"Error processing {url}: {str(e)}")
                        speed_data[url] = {
                            'desktop_speed_index': None,
                            'mobile_speed_index': None
                        }
        
        # Add speed columns to DataFrame
        logger.info("🔄 Mapping speed data to CSV rows...")
        df['desktop_speed_index'] = df[url_column].map(lambda x: speed_data.get(x, {}).get('desktop_speed_index'))
        df['mobile_speed_index'] = df[url_column].map(lambda x: speed_data.get(x, {}).get('mobile_speed_index'))
        
        # Save enriched CSV
        try:
            df.to_csv(output_file, index=False)
            logger.info(f"💾 Enriched CSV saved to: {output_file}")
        except Exception as e:
            logger.error(f"Failed to save CSV: {str(e)}")
            return False, {}
        
        # Save error log
        self.save_error_log(error_file)
        
        # Calculate statistics
        desktop_success = df['desktop_speed_index'].notna().sum()
        mobile_success = df['mobile_speed_index'].notna().sum()
        total_rows = len(df)
        
        stats = {
            'total_rows': total_rows,
            'unique_urls': len(unique_urls),
            'desktop_success': desktop_success,
            'mobile_success': mobile_success,
            'desktop_success_rate': desktop_success / total_rows * 100,
            'mobile_success_rate': mobile_success / total_rows * 100,
            'api_requests': self.request_count,
            'api_success_rate': self.success_count / self.request_count * 100 if self.request_count > 0 else 0,
            'errors': self.error_count
        }
        
        # Print final summary
        logger.info(f"🎉 Processing completed!")
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   Total rows: {total_rows}")
        logger.info(f"   Unique URLs: {len(unique_urls)}")
        logger.info(f"   Desktop success: {desktop_success}/{total_rows} ({stats['desktop_success_rate']:.1f}%)")
        logger.info(f"   Mobile success: {mobile_success}/{total_rows} ({stats['mobile_success_rate']:.1f}%)")
        logger.info(f"   API requests: {self.request_count}")
        logger.info(f"   API success rate: {stats['api_success_rate']:.1f}%")
        
        if desktop_success > 0:
            avg_desktop = df['desktop_speed_index'].mean()
            logger.info(f"   Average desktop speed: {avg_desktop:.0f}ms")
        if mobile_success > 0:
            avg_mobile = df['mobile_speed_index'].mean()
            logger.info(f"   Average mobile speed: {avg_mobile:.0f}ms")
        
        # Clean up progress file
        try:
            os.remove(progress_file)
        except:
            pass
        
        return True, stats

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='Enrich CSV files with PageSpeed Insights data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pagespeed_csv_enricher.py input.csv output.csv YOUR_API_KEY
  python pagespeed_csv_enricher.py input.csv output.csv YOUR_API_KEY --workers 10
  python pagespeed_csv_enricher.py input.csv output.csv YOUR_API_KEY --url-column website
  python pagespeed_csv_enricher.py input.csv output.csv YOUR_API_KEY --no-resume --verbose
        """
    )
    
    parser.add_argument('input_csv', help='Input CSV file path')
    parser.add_argument('output_csv', help='Output CSV file path')
    parser.add_argument('api_key', help='PageSpeed Insights API key')
    parser.add_argument('--url-column', help='Name of URL column (auto-detected if not specified)')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers (default: 5)')
    parser.add_argument('--timeout', type=int, default=30, help='Request timeout in seconds (default: 30)')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh (ignore previous progress)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate inputs
    if not os.path.exists(args.input_csv):
        print(f"Error: Input file '{args.input_csv}' not found")
        sys.exit(1)
    
    if args.workers < 1 or args.workers > 20:
        print("Error: Number of workers must be between 1 and 20")
        sys.exit(1)
    
    # Create enricher
    enricher = PageSpeedEnricher(
        api_key=args.api_key,
        max_workers=args.workers,
        timeout=args.timeout
    )
    
    # Process CSV
    start_time = time.time()
    success, stats = enricher.process_csv(
        input_file=args.input_csv,
        output_file=args.output_csv,
        url_column=args.url_column,
        resume=not args.no_resume
    )
    end_time = time.time()
    
    if success:
        logger.info(f"⏱️ Total processing time: {(end_time - start_time)/60:.1f} minutes")
        print(f"\n✅ Success! Enriched CSV saved to: {args.output_csv}")
        print(f"📊 {stats['desktop_success']}/{stats['total_rows']} rows have desktop speed data")
        print(f"📱 {stats['mobile_success']}/{stats['total_rows']} rows have mobile speed data")
    else:
        print("\n❌ Processing failed. Check the logs for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()

