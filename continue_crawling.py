import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import os
import json
from datetime import datetime
from urllib.parse import urlparse, urljoin
import argparse

# Set a User-Agent to avoid being blocked
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
}

def extract_sentences(text):
    """Extract valid Vietnamese sentences from text."""
    # Pattern to match Vietnamese sentences
    pattern = r'[A-ZÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬĐÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴ][^.!?]*[.!?]'
    sentences = re.findall(pattern, text)
    # Clean sentences (remove extra whitespace)
    sentences = [re.sub(r'\s+', ' ', s).strip() for s in sentences]
    # Filter out very short sentences
    sentences = [s for s in sentences if len(s.split()) >= 5]
    return sentences

def process_article(link):
    """Process each article and extract sentences."""
    try:
        # Random delay to avoid being blocked
        time.sleep(random.uniform(0.5, 1.5))
        
        article_response = requests.get(link, headers=HEADERS, timeout=10)
        article_soup = BeautifulSoup(article_response.content, 'html.parser')
        
        # Extract domain to customize content extraction based on website
        domain = urlparse(link).netloc
        
        # Try to extract article content based on domain
        article_text = ""
        
        if 'vnexpress.net' in domain:
            content_div = article_soup.find('article', class_='fck_detail')
            if content_div:
                paragraphs = content_div.find_all('p')
                article_text = ' '.join([p.get_text() for p in paragraphs])
        
        elif 'vietnamnet.vn' in domain:
            content_div = article_soup.find('div', class_='content-detail')
            if content_div:
                paragraphs = content_div.find_all('p')
                article_text = ' '.join([p.get_text() for p in paragraphs])
        
        elif 'tuoitre.vn' in domain:
            content_div = article_soup.find('div', class_='content fck')
            if content_div:
                paragraphs = content_div.find_all('p')
                article_text = ' '.join([p.get_text() for p in paragraphs])
        
        elif 'thanhnien.vn' in domain:
            content_div = article_soup.find('div', class_='detail-content')
            if content_div:
                paragraphs = content_div.find_all('p')
                article_text = ' '.join([p.get_text() for p in paragraphs])
                
        elif 'dantri.com.vn' in domain:
            content_div = article_soup.find('div', class_='singular-content')
            if content_div:
                paragraphs = content_div.find_all('p')
                article_text = ' '.join([p.get_text() for p in paragraphs])
                
        elif 'baomoi.com' in domain:
            content_div = article_soup.find('div', class_='body-text')
            if content_div:
                paragraphs = content_div.find_all('p')
                article_text = ' '.join([p.get_text() for p in paragraphs])
                
        elif 'vtc.vn' in domain:
            content_div = article_soup.find('div', class_='content-detail')
            if content_div:
                paragraphs = content_div.find_all('p')
                article_text = ' '.join([p.get_text() for p in paragraphs])
                
        elif 'tienphong.vn' in domain:
            content_div = article_soup.find('div', class_='article__body')
            if content_div:
                paragraphs = content_div.find_all('p')
                article_text = ' '.join([p.get_text() for p in paragraphs])
        
        # Generic extraction if specific extractor fails
        if not article_text:
            # Try common content containers
            for class_name in ['article-content', 'article-body', 'post-content', 'entry-content', 'content']:
                content_div = article_soup.find(['div', 'article'], class_=re.compile(class_name, re.I))
                if content_div:
                    paragraphs = content_div.find_all('p')
                    article_text = ' '.join([p.get_text() for p in paragraphs])
                    break
        
        return extract_sentences(article_text)
    except Exception as e:
        print(f"Error processing article {link}: {e}")
        return []

def get_archive_url(base_url, category, year, month, page=1):
    """Get URL for archived content based on site structure"""
    domain = urlparse(base_url).netloc
    
    if 'vnexpress.net' in domain:
        # VnExpress archive format: https://vnexpress.net/thoi-su/2023/10/page/2
        return f"{base_url}{category}/{year}/{month:02d}/p{page}"
    elif 'tuoitre.vn' in domain:
        # TuoiTre archive format: https://tuoitre.vn/thoi-su/2023/10.htm
        if page > 1:
            return f"{base_url}{category}/{year}/{month:02d}/trang-{page}.htm"
        return f"{base_url}{category}/{year}/{month:02d}.htm"
    elif 'thanhnien.vn' in domain:
        # ThanhNien archive format: https://thanhnien.vn/thoi-su/2023/10?page=2
        return f"{base_url}{category}/{year}/{month:02d}?page={page}"
    elif 'vietnamnet.vn' in domain:
        # VietnamNet archive format: https://vietnamnet.vn/thoi-su/2023/10/trang-2
        if page > 1:
            return f"{base_url}{category}/{year}/{month:02d}/trang-{page}"
        return f"{base_url}{category}/{year}/{month:02d}"
    elif 'dantri.com.vn' in domain:
        # Dantri archive format: https://dantri.com.vn/su-kien/2023/10/trang-2.htm
        if page > 1:
            return f"{base_url}{category}/{year}/{month:02d}/trang-{page}.htm"
        return f"{base_url}{category}/{year}/{month:02d}.htm"
    elif 'baomoi.com' in domain:
        # BaoMoi archive format: https://baomoi.com/thoi-su/2023/10/trang-2.epi
        if page > 1:
            return f"{base_url}{category}/{year}/{month:02d}/trang-{page}.epi"
        return f"{base_url}{category}/{year}/{month:02d}.epi"
    
    # Default format if site-specific format is not available
    return f"{base_url}{category}/archive/{year}/{month:02d}?page={page}"

def scrape_archive(base_url, categories, article_pattern, years, months, max_articles_per_page=20, max_pages=10):
    """Scrape archived content from Vietnamese news websites based on year and month."""
    all_sentences = []
    
    for category in categories:
        for year in years:
            for month in months:
                try:
                    # Try different pages for each year/month
                    for page in range(1, max_pages + 1):
                        archive_url = get_archive_url(base_url, category, year, month, page)
                        print(f"Scraping archive: {archive_url}")
                        
                        try:
                            response = requests.get(archive_url, headers=HEADERS, timeout=15)
                            if response.status_code != 200:
                                print(f"Failed to access archive page: {archive_url} (Status: {response.status_code})")
                                # If first page fails, skip this archive
                                if page == 1:
                                    break
                                # If other pages fail, we've likely reached the end of archive
                                continue
                                
                            soup = BeautifulSoup(response.content, 'html.parser')
                        except Exception as e:
                            print(f"Error fetching archive page {archive_url}: {e}")
                            continue
                        
                        # Find article links based on the pattern
                        article_links = []
                        for link in soup.find_all('a', href=True):
                            href = link['href']
                            if article_pattern(href):
                                # Ensure full URL
                                if not href.startswith('http'):
                                    href = urljoin(base_url, href)
                                if href not in article_links:  # Avoid duplicates
                                    article_links.append(href)
                        
                        if not article_links:
                            print(f"No articles found on {archive_url}")
                            # If no articles on first page, this archive might not exist
                            if page == 1:
                                break
                            # If no articles on other pages, we've likely reached the end
                            continue
                            
                        # Limit number of articles to scrape per page
                        article_links = article_links[:max_articles_per_page]
                        
                        # Process articles with a reasonable number of threads
                        with ThreadPoolExecutor(max_workers=5) as executor:
                            for sentences in tqdm(executor.map(process_article, article_links), 
                                                desc=f"Processing {os.path.basename(base_url)} {category} {year}/{month:02d} page {page}",
                                                total=len(article_links)):
                                all_sentences.extend(sentences)
                        
                        # Polite delay between pages
                        time.sleep(random.uniform(2, 4))
                
                except Exception as e:
                    print(f"Error processing category {category} for {year}/{month:02d}: {e}")
                    continue
    
    return all_sentences

def scrape_vnexpress_archive(years, months, max_articles=20, max_pages=10):
    """Scrape archived articles from VnExpress."""
    base_url = "https://vnexpress.net"
    categories = [
        "/thoi-su", "/the-gioi", "/kinh-doanh", "/khoa-hoc", 
        "/giai-tri", "/the-thao", "/phap-luat", "/giao-duc",
        "/suc-khoe", "/doi-song", "/du-lich"
    ]
    
    article_pattern = lambda href: href.startswith('https://vnexpress.net') and '-' in href and '.html' in href
    
    return scrape_archive(base_url, categories, article_pattern, years, months, max_articles, max_pages)

def scrape_tuoitre_archive(years, months, max_articles=20, max_pages=10):
    """Scrape archived articles from Tuoi Tre."""
    base_url = "https://tuoitre.vn"
    categories = [
        "/thoi-su", "/the-gioi", "/phap-luat", "/kinh-doanh",
        "/nhip-song-tre", "/van-hoa", "/giai-tri", "/the-thao",
        "/giao-duc", "/khoa-hoc", "/suc-khoe"
    ]
    
    article_pattern = lambda href: '.htm' in href and not any(c in href for c in ['video', 'photo', 'tag'])
    
    return scrape_archive(base_url, categories, article_pattern, years, months, max_articles, max_pages)

def scrape_thanhnien_archive(years, months, max_articles=20, max_pages=10):
    """Scrape archived articles from Thanh Nien."""
    base_url = "https://thanhnien.vn"
    categories = [
        "/thoi-su", "/the-gioi", "/kinh-te", "/doi-song",
        "/van-hoa", "/giai-tri", "/the-thao", "/suc-khoe",
        "/giao-duc", "/cong-nghe"
    ]
    
    article_pattern = lambda href: href.startswith('/') and not any(c in href for c in ['video', 'photo', 'tag'])
    
    return scrape_archive(base_url, categories, article_pattern, years, months, max_articles, max_pages)

def scrape_vietnamnet_archive(years, months, max_articles=20, max_pages=10):
    """Scrape archived articles from VietnamNet."""
    base_url = "https://vietnamnet.vn"
    categories = [
        "/thoi-su", "/the-gioi", "/kinh-doanh", "/giai-tri", 
        "/the-thao", "/suc-khoe", "/giao-duc", "/doi-song"
    ]
    
    article_pattern = lambda href: href.startswith('/') and href != '/' and not any(c in href for c in ['tag', 'video', 'photo'])
    
    return scrape_archive(base_url, categories, article_pattern, years, months, max_articles, max_pages)

def scrape_baomoi_archive(years, months, max_articles=20, max_pages=10):
    """Scrape archived articles from BaoMoi."""
    base_url = "https://baomoi.com"
    categories = [
        "/tin-moi", "/xa-hoi", "/the-gioi", "/van-hoa", 
        "/kinh-te", "/giai-tri", "/the-thao", "/phap-luat",
        "/khoa-hoc-cong-nghe", "/giao-duc", "/suc-khoe"
    ]
    
    article_pattern = lambda href: href.startswith('/') and '.epi' in href and not any(c in href for c in ['tag', 'video', 'photo'])
    
    return scrape_archive(base_url, categories, article_pattern, years, months, max_articles, max_pages)

def load_existing_sentences(file_path):
    """Load existing sentences from a CSV file."""
    try:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, encoding='utf-8')
            return df['sentence'].tolist()
        else:
            print(f"File {file_path} not found. Starting with empty set.")
            return []
    except Exception as e:
        print(f"Error loading existing sentences: {e}")
        return []

def save_sentences(sentences, filename="vietnamese_sentences.csv"):
    """Save sentences to CSV with unique ID and source metadata."""
    sentences_df = pd.DataFrame({
        'id': range(1, len(sentences) + 1),
        'sentence': sentences,
        'length': [len(s.split()) for s in sentences],
        'collected_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    
    sentences_df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Saved {len(sentences)} sentences to {filename}")
    
    # Also save to text file, one sentence per line
    with open(filename.replace('.csv', '.txt'), 'w', encoding='utf-8') as f:
        for sentence in sentences:
            f.write(sentence + '\n')
    
    return sentences_df

def deduplicate_sentences(sentences, existing_sentences=None):
    """Remove duplicate sentences, including any that exist in existing_sentences list."""
    if existing_sentences:
        # Create a set of existing sentences for fast lookup
        existing_set = set(existing_sentences)
        # Keep only new unique sentences
        unique_sentences = []
        for sentence in sentences:
            if sentence not in existing_set and sentence not in unique_sentences:
                unique_sentences.append(sentence)
        return unique_sentences
    else:
        # Just deduplicate the new sentences
        return list(dict.fromkeys(sentences))

def main(target_count=50000, existing_file="vietnamese_sentences_final.csv", output_file="vietnamese_sentences_50k.csv"):
    """Main function to continue crawling Vietnamese sentences from archives."""
    # Load existing sentences
    existing_sentences = load_existing_sentences(existing_file)
    current_count = len(existing_sentences)
    
    needed_sentences = target_count - current_count
    if needed_sentences <= 0:
        print(f"Already have {current_count} sentences, which is >= target {target_count}. No need to crawl more.")
        return existing_sentences
    
    print(f"Starting to crawl for {needed_sentences} more Vietnamese sentences...")
    print(f"Already have {current_count} sentences from {existing_file}")
    
    # Define archive years and months to crawl
    # Start with recent months but not too recent to avoid overlap with previous crawl
    # Then go further back in time if needed
    years_to_try = [2023, 2022, 2021, 2020, 2019]
    months_to_try = [10, 7, 4, 1]  # Quarterly distribution for variety
    
    all_new_sentences = []
    
    # Track progress
    progress = tqdm(total=needed_sentences, desc="Additional sentences collected")
    
    # Try different sites and archives until we reach the target
    sites = [
        ('VnExpress', scrape_vnexpress_archive),
        ('TuoiTre', scrape_tuoitre_archive),
        ('ThanhNien', scrape_thanhnien_archive),
        ('VietnamNet', scrape_vietnamnet_archive),
        ('BaoMoi', scrape_baomoi_archive)
    ]
    
    for year in years_to_try:
        for month in months_to_try:
            if len(all_new_sentences) >= needed_sentences:
                break
                
            print(f"\n{'='*20} Scraping archives for {year}/{month:02d} {'='*20}")
            
            for site_name, scrape_func in sites:
                if len(all_new_sentences) >= needed_sentences:
                    break
                    
                print(f"\n--- Scraping {site_name} archives for {year}/{month:02d} ---")
                try:
                    # Scrape this site's archive for this year/month
                    site_sentences = scrape_func([year], [month], max_articles=30, max_pages=5)
                    
                    # Deduplicate against existing sentences
                    new_unique_sentences = deduplicate_sentences(site_sentences, existing_sentences)
                    
                    # Also deduplicate against sentences we've already collected in this run
                    new_unique_sentences = deduplicate_sentences(new_unique_sentences, all_new_sentences)
                    
                    all_new_sentences.extend(new_unique_sentences)
                    
                    # Update progress
                    progress.update(min(len(new_unique_sentences), needed_sentences - progress.n))
                    print(f"Found {len(new_unique_sentences)} new unique sentences from {site_name} {year}/{month:02d}. Total new: {len(all_new_sentences)}")
                    
                    # Save intermediate results when we have a significant amount
                    if len(all_new_sentences) % 5000 < 100 and len(all_new_sentences) > 0:
                        current_total = current_count + len(all_new_sentences)
                        interim_filename = f"vietnamese_sentences_interim_{current_total}.csv"
                        combined_sentences = existing_sentences + all_new_sentences
                        save_sentences(combined_sentences, interim_filename)
                    
                    # Polite delay between sites
                    time.sleep(random.uniform(3, 5))
                    
                except Exception as e:
                    print(f"Error scraping {site_name} archive for {year}/{month:02d}: {e}")
    
    # Combine existing and new sentences
    final_sentences = existing_sentences + all_new_sentences
    print(f"\nTotal sentences: {len(final_sentences)} (Original: {current_count} + New: {len(all_new_sentences)})")
    
    # Save final dataset
    save_sentences(final_sentences, output_file)
    
    progress.close()
    print(f"\nFinished! Now have {len(final_sentences)} unique Vietnamese sentences.")
    return final_sentences

if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Continue crawling Vietnamese sentences from news website archives')
    parser.add_argument('--target', type=int, default=50000, help='Target total number of sentences (default: 50000)')
    parser.add_argument('--existing', type=str, default="vietnamese_sentences_final.csv", help='Existing sentences file (default: vietnamese_sentences_final.csv)')
    parser.add_argument('--output', type=str, default="vietnamese_sentences_50k.csv", help='Output file name (default: vietnamese_sentences_50k.csv)')
    parser.add_argument('--delay', type=float, default=1.0, help='Base delay multiplier between requests (default: 1.0)')
    args = parser.parse_args()
    
    # Adjust global settings based on arguments
    if args.delay != 1.0:
        original_sleep = time.sleep
        def custom_sleep(seconds):
            return original_sleep(seconds * args.delay)
        time.sleep = custom_sleep
    
    # Print summary of settings
    print(f"Starting archive crawler with the following settings:")
    print(f"- Target total: {args.target} sentences")
    print(f"- Existing sentences file: {args.existing}")
    print(f"- Output: {args.output}")
    print(f"- Delay multiplier: {args.delay}x")
    print("\nPress Ctrl+C to stop the crawler at any time. Progress will be saved.")
    
    try:
        # Run the main function with the provided arguments
        sentences = main(
            target_count=args.target,
            existing_file=args.existing,
            output_file=args.output
        )
        
    except KeyboardInterrupt:
        print("\nCrawler stopped by user. Saving progress...")
        # Save whatever progress we've made so far
        if 'all_new_sentences' in locals() and all_new_sentences and 'existing_sentences' in locals():
            interrupted_sentences = existing_sentences + all_new_sentences
            save_sentences(interrupted_sentences, f"interrupted_{args.output}")
        print("Progress saved. Exiting.")
    
    except Exception as e:
        print(f"Error during crawling: {e}")
        import traceback
        traceback.print_exc()