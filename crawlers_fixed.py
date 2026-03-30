"""
Vietnamese News Crawler - Fixed Version
Crawls articles from Báo Mới, Thanh Niên, VNExpress, Dân Trí
"""

import requests
from bs4 import BeautifulSoup
import time
import os
import re
from datetime import datetime
from typing import List
from urllib.parse import urljoin

# Configuration
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

OUTPUT_DIR = 'raw_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

MAX_SENTENCES = 50000  # Maximum number of sentences to collect

# ==================== Text Processing ====================
def clean_text(text: str) -> str:
    """Clean and normalize Vietnamese text"""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove special characters but keep Vietnamese diacritics
    text = re.sub(r'[\n\r\t]', ' ', text)
    
    return text

def split_into_sentences(text: str) -> List[str]:
    """Split Vietnamese text into sentences"""
    if not text:
        return []
    
    # Vietnamese sentence endings
    text = text.replace('.\n', '.')
    text = text.replace('!\n', '!')
    text = text.replace('?\n', '?')
    
    # Split by sentence endings: . ! ? and also by em dash (–)
    sentences = re.split(r'(?<=[.!?–\-])\s+(?=[A-ZÀỂẦẬĂẰẲẴẶẢẠẪẨễ])', text)
    
    # If no period-based split worked, split by longer text blocks
    if len(sentences) < 2:
        sentences = re.split(r'(?<=[.!?])\s+', text)
    
    # If still just one sentence, split by newlines as last resort
    if len(sentences) < 2:
        sentences = text.split('\n')
    
    # Clean each sentence - require at least 5 characters to be valid
    sentences = [clean_text(s) for s in sentences if len(clean_text(s)) > 5]
    
    return sentences

# ==================== VNExpress Crawler ====================
def crawl_vnexpress(num_articles: int = 10000) -> List[str]:
    """Crawl articles from VNExpress"""
    sentences = []
    urls_visited = set()
    
    print("Crawling VNExpress...")
    
    base_urls = [
        'https://vnexpress.net/thoi-su',
        'https://vnexpress.net/the-gioi',
        'https://vnexpress.net/kinh-doanh',
        'https://vnexpress.net/khoa-hoc-cong-nghe',
        'https://vnexpress.net/goc-nhin',
        'https://vnexpress.net/spotlight',
        'https://vnexpress.net/bat-dong-san',
        'https://vnexpress.net/suc-khoe',
        'https://vnexpress.net/giai-tri',
        'https://vnexpress.net/the-thao',
        'https://vnexpress.net/phap-luat',
        'https://vnexpress.net/giao-duc',
        'https://vnexpress.net/doi-song',
        'https://vnexpress.net/oto-xe-may',
        'https://vnexpress.net/du-lich',
        'https://vnexpress.net/anh',
        'https://vnexpress.net/infographics',
        'https://vnexpress.net/y-kien',
        'https://vnexpress.net/tam-su',
        'https://vnexpress.net/thu-gian',
    ]
    
    for base_url in base_urls:
        if len(urls_visited) >= num_articles:
            break
            
        try:
            response = requests.get(base_url, headers=HEADERS, timeout=15)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all .html links
            article_links = soup.find_all('a', href=re.compile(r'\.html$', re.I))
            
            for links in article_links[:30]:
                if len(urls_visited) >= num_articles:
                    break
                
                try:
                    article_url = links.get('href')
                    if not article_url or article_url in urls_visited:
                        continue
                    
                    article_url = urljoin(base_url, article_url)
                    
                    if 'vnexpress.net' not in article_url:
                        continue
                    
                    urls_visited.add(article_url)
                    
                    # Fetch and parse article
                    art_response = requests.get(article_url, headers=HEADERS, timeout=15)
                    art_soup = BeautifulSoup(art_response.content, 'html.parser')
                    
                    # Extract paragraphs
                    paragraphs = art_soup.find_all('p')
                    
                    if paragraphs:
                        text = ' '.join([p.get_text() for p in paragraphs])
                        text = clean_text(text)
                        
                        if len(text) > 100:
                            article_sentences = split_into_sentences(text)
                            if article_sentences:
                                sentences.extend(article_sentences)
                                title = links.get_text()[:50].strip()
                                print(f"  ✓ [{len(article_sentences)} sents] {title}...")
                    
                    time.sleep(0.5)
                
                except Exception as e:
                    continue
        
        except Exception as e:
            continue
    
    print(f"VNExpress: {len(sentences)} sentences\n")
    return sentences

# ==================== VNExpress Crawler (với Pagination) ====================
def crawl_vnexpress(num_articles: int = 50, max_pages: int = 20, max_sentences: int = None) -> List[str]:
    """Crawl articles from VNExpress with pagination support
    
    VNExpress pagination pattern:
    - Page 1: https://vnexpress.net/thoi-su
    - Page 2: https://vnexpress.net/thoi-su-p2
    - Page 3: https://vnexpress.net/thoi-su-p3
    """
    if max_sentences is None:
        max_sentences = MAX_SENTENCES
    
    sentences = []
    urls_visited = set()
    
    print("Crawling VNExpress (with pagination)...")
    
    # Only use top categories for pagination
    base_urls = [
        'https://vnexpress.net/thoi-su',
        'https://vnexpress.net/the-gioi',
        'https://vnexpress.net/kinh-doanh',
        'https://vnexpress.net/khoa-hoc-cong-nghe',
        'https://vnexpress.net/goc-nhin',
        'https://vnexpress.net/spotlight',
        'https://vnexpress.net/bat-dong-san',
        'https://vnexpress.net/suc-khoe',
        'https://vnexpress.net/giai-tri',
        'https://vnexpress.net/the-thao',
        'https://vnexpress.net/phap-luat',
        'https://vnexpress.net/giao-duc',
        'https://vnexpress.net/doi-song',
        'https://vnexpress.net/oto-xe-may',
        'https://vnexpress.net/du-lich',
        'https://vnexpress.net/anh',
        'https://vnexpress.net/infographics',
        'https://vnexpress.net/y-kien',
        'https://vnexpress.net/tam-su',
        'https://vnexpress.net/thu-gian',
    ]
    
    for base_url in base_urls:
        if len(urls_visited) >= num_articles or len(sentences) >= max_sentences:
            break
        
        category_name = base_url.split('/')[-1]
        print(f"\n  Category: {category_name}")
        
        # Crawl multiple pages for each category
        for page in range(1, max_pages + 1):
            if len(urls_visited) >= num_articles or len(sentences) >= max_sentences:
                break
            
            # Construct page URL
            if page == 1:
                page_url = base_url
            else:
                page_url = f"{base_url}-p{page}"
            
            try:
                print(f"    Page {page}: ", end="")
                response = requests.get(page_url, headers=HEADERS, timeout=15)
                response.encoding = 'utf-8'
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find all .html links on this page
                article_links = soup.find_all('a', href=re.compile(r'\.html$', re.I))
                
                if not article_links:
                    print("No articles (stopping pagination)")
                    break
                
                print(f"Found {len(article_links)} articles")
                articles_on_page = 0
                
                for links in article_links[:30]:
                    if len(urls_visited) >= num_articles or len(sentences) >= max_sentences:
                        break
                    
                    try:
                        article_url = links.get('href')
                        if not article_url or article_url in urls_visited:
                            continue
                        
                        article_url = urljoin(page_url, article_url)
                        
                        if 'vnexpress.net' not in article_url:
                            continue
                        
                        urls_visited.add(article_url)
                        
                        # Fetch and parse article
                        art_response = requests.get(article_url, headers=HEADERS, timeout=15)
                        art_soup = BeautifulSoup(art_response.content, 'html.parser')
                        
                        # Extract paragraphs
                        paragraphs = art_soup.find_all('p')
                        
                        if paragraphs:
                            text = ' '.join([p.get_text() for p in paragraphs])
                            text = clean_text(text)
                            
                            if len(text) > 100:
                                article_sentences = split_into_sentences(text)
                                if article_sentences:
                                    # Only add sentences if we haven't reached the max
                                    remaining = max_sentences - len(sentences)
                                    if remaining > 0:
                                        sentences.extend(article_sentences[:remaining])
                                        articles_on_page += 1
                                        title = links.get_text()[:45].strip()
                                        print(f"        ✓ [{len(article_sentences[:remaining])} sents] {title}...")
                        
                        time.sleep(0.3)
                    
                    except Exception as e:
                        continue
                
            except Exception as e:
                print(f"Error: {str(e)[:50]}")
                continue
    
    print(f"\nVNExpress: {len(sentences)} sentences total\n")
    return sentences

def crawl_thanhnien(num_articles: int = 50, max_sentences: int = None) -> List[str]:
    """Crawl articles from Thanh Niên"""
    if max_sentences is None:
        max_sentences = MAX_SENTENCES
    
    sentences = []
    urls_visited = set()
    
    print("Crawling Thanh Niên...")
    
    base_urls = [
        'https://thanhnien.vn/chinh-tri/',
        'https://thanhnien.vn/thoi-su/',
        'https://thanhnien.vn/the-gioi/',
        'https://thanhnien.vn/kinh-te/',
        'https://thanhnien.vn/doi-song/',
        'https://thanhnien.vn/suc-khoe/',
        'https://thanhnien.vn/gioi-tre/',
        'https://thanhnien.vn/giao-duc/',
        'https://thanhnien.vn/du-lich/',
        'https://thanhnien.vn/van-hoa/',
        'https://thanhnien.vn/giai-tri/',
        'https://thanhnien.vn/the-thao/',
        'https://thanhnien.vn/cong-nghe/',
        'https://thanhnien.vn/xe/',
        'https://thanhnien.vn/video/',
        'https://thanhnien.vn/tieu-dung-thong-minh/',
        'https://thanhnien.vn/thoi-trang-tre/',
    ]
    
    for base_url in base_urls:
        if len(urls_visited) >= num_articles or len(sentences) >= max_sentences:
            break
        
        try:
            response = requests.get(base_url, headers=HEADERS, timeout=15)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all links
            article_links = soup.find_all('a', href=re.compile(r'/.*\.(html|htm)$', re.I))
            
            for link in article_links[:30]:
                if len(urls_visited) >= num_articles or len(sentences) >= max_sentences:
                    break
                
                try:
                    article_url = link.get('href')
                    if not article_url or article_url in urls_visited:
                        continue
                    
                    article_url = urljoin(base_url, article_url)
                    
                    if 'thanhnien.vn' not in article_url:
                        continue
                    
                    urls_visited.add(article_url)
                    
                    # Fetch and parse
                    art_response = requests.get(article_url, headers=HEADERS, timeout=15)
                    art_soup = BeautifulSoup(art_response.content, 'html.parser')
                    
                    paragraphs = art_soup.find_all('p')
                    
                    if paragraphs:
                        text = ' '.join([p.get_text() for p in paragraphs])
                        text = clean_text(text)
                        
                        if len(text) > 100:
                            article_sentences = split_into_sentences(text)
                            if article_sentences:
                                # Only add sentences if we haven't reached the max
                                remaining = max_sentences - len(sentences)
                                if remaining > 0:
                                    sentences.extend(article_sentences[:remaining])
                                    title = link.get_text()[:50].strip()
                                    print(f"  ✓ [{len(article_sentences[:remaining])} sents] {title}...")
                    
                    time.sleep(0.5)
                
                except:
                    continue
        
        except:
            continue
    
    print(f"Thanh Niên: {len(sentences)} sentences\n")
    return sentences

# ==================== Main Function ====================
def crawl_all_news_sources(articles_per_source: int = 10000, output_file: str = None, vnexpress_pages: int = 20, max_sentences: int = MAX_SENTENCES):
    """Crawl from all sources and save results
    
    Parameters:
    - articles_per_source: Total articles target per source
    - output_file: Output filename (optional)
    - vnexpress_pages: Number of pages to crawl from VNExpress (default: 20)
    - max_sentences: Maximum number of sentences to collect (default: 50000)
    """
    
    print("=" * 70)
    print("Vietnamese News Crawler with BeautifulSoup - WITH PAGINATION")
    print("=" * 70)
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"VNExpress pages to crawl: {vnexpress_pages}")
    print(f"Maximum sentences limit: {max_sentences}\n")
    
    all_sentences = []
    
    # # Crawl all sources
    # vnexpress_sentences = crawl_vnexpress(articles_per_source, max_pages=vnexpress_pages, max_sentences=max_sentences)
    # all_sentences.extend(vnexpress_sentences)
    
    # Stop if we've reached the maximum
    if len(all_sentences) < max_sentences:
        thanhnien_sentences = crawl_thanhnien(articles_per_source, max_sentences=max_sentences - len(all_sentences))
        all_sentences.extend(thanhnien_sentences)
    
    # Remove duplicates
    unique_sentences = list(dict.fromkeys(all_sentences))
    
    # Trim to maximum if needed
    if len(unique_sentences) > max_sentences:
        unique_sentences = unique_sentences[:max_sentences]
    
    # Save results
    if output_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{OUTPUT_DIR}/crawled_news_{timestamp}.txt"
    else:
        output_file = os.path.join(OUTPUT_DIR, output_file)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for sentence in unique_sentences:
            f.write(sentence + '\n')
    
    # Summary
    print("=" * 70)
    print("Crawling Complete:")
    # print(f"  VNExpress:    {len(vnexpress_sentences)} sentences")
    print(f"  Thanh Niên:   {len(thanhnien_sentences)} sentences")
    print(f"  ─────────────────────")
    print(f"  Total:        {len(all_sentences)} sentences")
    print(f"  Unique:       {len(unique_sentences)} sentences")
    print(f"\n✓ Saved to: {output_file}")
    print(f"End: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

if __name__ == '__main__':
    
    crawl_all_news_sources(articles_per_source=10000, vnexpress_pages=20, output_file='sentences_50000.txt')
