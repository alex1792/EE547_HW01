#!/usr/bin/env python3
import json
import os
import re
import time
from datetime import datetime, timezone

def strip_html(html_content):
    """Remove HTML tags and extract text."""
    # Remove script and style elements
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Extract links before removing tags
    links = re.findall(r'href=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    
    # Extract images
    images = re.findall(r'src=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html_content)
    
    # Clean whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text, links, images

def count_statistics(text):
    """Count words, sentences, paragraphs and calculate average word length."""
    # Count words (split on whitespace and filter empty strings)
    words = [word for word in text.split() if word.strip()]
    word_count = len(words)
    
    # Count sentences (split on sentence-ending punctuation)
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    sentence_count = len(sentences)
    
    # Count paragraphs (split on double newlines or multiple spaces)
    paragraphs = re.split(r'\n\s*\n', text)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    paragraph_count = len(paragraphs)
    
    # Calculate average word length
    if word_count > 0:
        avg_word_length = sum(len(word) for word in words) / word_count
    else:
        avg_word_length = 0.0
    
    return {
        "word_count": word_count,
        "sentence_count": sentence_count,
        "paragraph_count": paragraph_count,
        "avg_word_length": round(avg_word_length, 2)
    }

def process_html_file(html_file_path, output_file_path):
    """Process a single HTML file and return the result."""
    try:
        # Read HTML content
        with open(html_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            html_content = f.read()
        
        # Extract text, links, and images
        text, links, images = strip_html(html_content)
        
        # Calculate statistics
        statistics = count_statistics(text)
        
        # Create output data
        result = {
            "source_file": os.path.basename(html_file_path),
            "text": text,
            "statistics": statistics,
            "links": links,
            "images": images,
            "processed_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Write to output file
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        return result
        
    except Exception as e:
        print(f"Error processing {html_file_path}: {e}", flush=True)
        return None

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Processor starting", flush=True)

    # Create output directory
    os.makedirs("/shared/processed", exist_ok=True)
    os.makedirs("/shared/status", exist_ok=True)
    
    # Wait for fetcher to complete
    fetch_status_file = "/shared/status/fetch_complete.json"
    while not os.path.exists(fetch_status_file):
        print("Waiting for fetcher to complete...", flush=True)
        time.sleep(2)
    
    # Read fetch status to get list of files to process
    with open(fetch_status_file, 'r') as f:
        fetch_status = json.load(f)
    
    
    
    # Process each successful fetch
    results = []
    successful_files = [r for r in fetch_status["results"] if r["status"] == "success"]
    
    for result in successful_files:
        source_file = result["file"]
        html_file_path = f"/shared/raw/{source_file}"
        output_file = source_file.replace('.html', '.json')
        output_file_path = f"/shared/processed/{output_file}"
        
        print(f"Processing {source_file}...", flush=True)
        
        # Process the HTML file
        processed_result = process_html_file(html_file_path, output_file_path)
        
        if processed_result:
            results.append({
                "source_file": source_file,
                "output_file": output_file,
                "status": "success",
                "word_count": processed_result["statistics"]["word_count"],
                "link_count": len(processed_result["links"]),
                "image_count": len(processed_result["images"])
            })
            print(f"  -> {processed_result['statistics']['word_count']} words, "
                  f"{len(processed_result['links'])} links, "
                  f"{len(processed_result['images'])} images", flush=True)
        else:
            results.append({
                "source_file": source_file,
                "output_file": output_file,
                "status": "failed"
            })
    
    # Write completion status
    status = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "files_processed": len(results),
        "successful": sum(1 for r in results if r["status"] == "success"),
        "failed": sum(1 for r in results if r["status"] == "failed"),
        "results": results
    }
    
    with open("/shared/status/process_complete.json", 'w') as f:
        json.dump(status, f, indent=2)
    
    print(f"[{datetime.now(timezone.utc).isoformat()}] Processor complete", flush=True)
    print(f"Processed {status['successful']} files successfully, {status['failed']} failed", flush=True)

if __name__ == "__main__":
    main()