#!/usr/bin/env python3
import sys
import os
import json
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import Counter, defaultdict
import re
import time


# Stopwords list as specified
STOPWORDS = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
             'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
             'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
             'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
             'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it',
             'we', 'they', 'what', 'which', 'who', 'when', 'where', 'why', 'how',
             'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other', 'some',
             'such', 'as', 'also', 'very', 'too', 'only', 'so', 'than', 'not'}


def log_event(message, log_events=None):
    """Log an event with timestamp."""
    timestamp = datetime.utcnow().isoformat() + "Z"
    log_message = f"[{timestamp}] {message}"
    print(log_message)
    if log_events is not None:
        log_events.append(log_message)
    return log_message


def log_error(message, log_events=None):
    """Log an error with timestamp."""
    timestamp = datetime.utcnow().isoformat() + "Z"
    error_message = f"[{timestamp}] ERROR: {message}"
    print(error_message, file=sys.stderr)
    if log_events is not None:
        log_events.append(error_message)
    return error_message


def log_warning(message, log_events=None):
    """Log a warning with timestamp."""
    timestamp = datetime.utcnow().isoformat() + "Z"
    warning_message = f"[{timestamp}] WARNING: {message}"
    print(warning_message, file=sys.stderr)
    if log_events is not None:
        log_events.append(warning_message)
    return warning_message


def validate_arguments():
    """Validate command line arguments."""
    if len(sys.argv) != 4:
        print("Usage: python arxiv_processor.py <search_query> <max_results> <output_directory>")
        print("Example: python arxiv_processor.py 'cat:cs.LG' 10 /data/output")
        sys.exit(1)
    
    search_query = sys.argv[1]
    max_results = sys.argv[2]
    output_dir = sys.argv[3]
    
    # Validate max_results
    try:
        max_results = int(max_results)
        if max_results < 1 or max_results > 100:
            raise ValueError("Max results must be between 1 and 100")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    return search_query, max_results, output_dir


def query_arxiv_api_with_retry(search_query, max_results, max_retries=3):
    """Query the ArXiv API with retry logic for rate limiting."""
    base_url = "http://export.arxiv.org/api/query"
    
    params = {
        'search_query': search_query,
        'start': 0,
        'max_results': max_results
    }
    
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url) as response:
                if response.status == 429:  # Rate limited
                    if attempt < max_retries - 1:
                        print(f"Rate limited (HTTP 429). Waiting 3 seconds before retry {attempt + 2}/{max_retries}...")
                        time.sleep(3)
                        continue
                    else:
                        raise Exception("Rate limited: Maximum retry attempts reached")
                
                xml_data = response.read().decode('utf-8')
                return xml_data
                
        except urllib.error.URLError as e:
            error_msg = f"Network error accessing ArXiv API: {e}"
            if attempt < max_retries - 1:
                print(f"Network error. Retrying in 3 seconds... (attempt {attempt + 2}/{max_retries})")
                time.sleep(3)
                continue
            else:
                raise Exception(error_msg)
        except Exception as e:
            if "Rate limited" in str(e) and attempt < max_retries - 1:
                continue
            raise e
    
    raise Exception("Failed to fetch data from ArXiv API after maximum retries")


def parse_xml_response_with_error_handling(xml_data, log_events):
    """Parse the XML response from ArXiv API with error handling."""
    try:
        root = ET.fromstring(xml_data)
        
        # Define namespace
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        
        papers = []
        entries = root.findall('atom:entry', ns)
        
        for i, entry in enumerate(entries):
            try:
                paper = {}
                
                # Extract paper ID (last part after '/')
                id_elem = entry.find('atom:id', ns)
                if id_elem is not None and id_elem.text:
                    paper['id'] = id_elem.text.split('/')[-1]
                else:
                    log_warning(f"Paper {i+1}: Missing or empty ID field", log_events)
                    paper['id'] = f"unknown_{i+1}"
                
                # Extract title
                title_elem = entry.find('atom:title', ns)
                if title_elem is not None and title_elem.text:
                    paper['title'] = title_elem.text.strip()
                else:
                    log_warning(f"Paper {paper['id']}: Missing or empty title field", log_events)
                    paper['title'] = "No title available"
                
                # Extract authors
                authors = []
                author_elems = entry.findall('atom:author', ns)
                if author_elems:
                    for author in author_elems:
                        name_elem = author.find('atom:name', ns)
                        if name_elem is not None and name_elem.text:
                            authors.append(name_elem.text.strip())
                
                if not authors:
                    log_warning(f"Paper {paper['id']}: No authors found", log_events)
                    authors = ["Unknown author"]
                
                paper['authors'] = authors
                
                # Extract abstract
                summary_elem = entry.find('atom:summary', ns)
                if summary_elem is not None and summary_elem.text:
                    paper['abstract'] = summary_elem.text.strip()
                else:
                    log_warning(f"Paper {paper['id']}: Missing or empty abstract field", log_events)
                    paper['abstract'] = ""
                
                # Extract categories
                categories = []
                category_elems = entry.findall('atom:category', ns)
                for category in category_elems:
                    term = category.get('term')
                    if term:
                        categories.append(term)
                
                if not categories:
                    log_warning(f"Paper {paper['id']}: No categories found", log_events)
                    categories = ["Unknown category"]
                
                paper['categories'] = categories
                
                # Extract published date
                published_elem = entry.find('atom:published', ns)
                if published_elem is not None and published_elem.text:
                    paper['published'] = published_elem.text.strip()
                else:
                    log_warning(f"Paper {paper['id']}: Missing published date", log_events)
                    paper['published'] = ""
                
                # Extract updated date
                updated_elem = entry.find('atom:updated', ns)
                if updated_elem is not None and updated_elem.text:
                    paper['updated'] = updated_elem.text.strip()
                else:
                    log_warning(f"Paper {paper['id']}: Missing updated date", log_events)
                    paper['updated'] = ""
                
                papers.append(paper)
                
            except Exception as e:
                log_warning(f"Error processing paper {i+1}: {e}", log_events)
                continue
        
        return papers
        
    except ET.ParseError as e:
        log_error(f"XML parsing error: {e}", log_events)
        # Try to extract partial data if possible
        try:
            # Attempt to parse with recovery
            parser = ET.XMLParser(recover=True)
            root = ET.fromstring(xml_data, parser)
            log_warning("Attempting to parse malformed XML with recovery", log_events)
            return parse_xml_response_with_error_handling(xml_data, log_events)
        except Exception:
            log_error("Failed to parse XML even with recovery", log_events)
            return []
    except Exception as e:
        log_error(f"Error processing XML data: {e}", log_events)
        return []


def analyze_abstract(abstract):
    """Analyze abstract and return statistics with Unicode handling."""
    if not abstract:
        return {
            'total_words': 0,
            'unique_words': 0,
            'total_sentences': 0,
            'avg_words_per_sentence': 0.0,
            'avg_word_length': 0.0
        }
    
    try:
        # Word analysis - handle Unicode properly
        words = re.findall(r'\b\w+\b', abstract.lower())
        total_words = len(words)
        unique_words = len(set(words))
        
        # Average word length
        if words:
            avg_word_length = sum(len(word) for word in words) / len(words)
        else:
            avg_word_length = 0.0
        
        # Sentence analysis
        sentences = re.split(r'[.!?]+', abstract)
        sentences = [s.strip() for s in sentences if s.strip()]
        total_sentences = len(sentences)
        
        if sentences:
            sentence_word_counts = []
            for sentence in sentences:
                sentence_words = re.findall(r'\b\w+\b', sentence.lower())
                sentence_word_counts.append(len(sentence_words))
            avg_words_per_sentence = sum(sentence_word_counts) / len(sentence_word_counts)
        else:
            avg_words_per_sentence = 0.0
        
        return {
            'total_words': total_words,
            'unique_words': unique_words,
            'total_sentences': total_sentences,
            'avg_words_per_sentence': round(avg_words_per_sentence, 2),
            'avg_word_length': round(avg_word_length, 2)
        }
        
    except Exception as e:
        # Fallback for problematic abstracts
        return {
            'total_words': 0,
            'unique_words': 0,
            'total_sentences': 0,
            'avg_words_per_sentence': 0.0,
            'avg_word_length': 0.0
        }


def extract_technical_terms(abstract):
    """Extract technical terms from abstract with Unicode handling."""
    if not abstract:
        return [], [], []
    
    try:
        # Uppercase terms (acronyms, proper nouns with capitals)
        uppercase_terms = re.findall(r'\b[A-Z][a-zA-Z]*[A-Z][a-zA-Z]*\b|\b[A-Z]{2,}\b', abstract)
        
        # Numeric terms
        numeric_terms = re.findall(r'\b[a-zA-Z]*\d+[a-zA-Z]*\b', abstract)
        
        # Hyphenated terms
        hyphenated_terms = re.findall(r'\b[a-zA-Z]+-[a-zA-Z-]+\b', abstract)
        
        return list(set(uppercase_terms)), list(set(numeric_terms)), list(set(hyphenated_terms))
        
    except Exception:
        return [], [], []


def create_papers_json(papers, output_dir, log_events):
    """Create papers.json file with the required format."""
    papers_data = []
    
    for paper in papers:
        try:
            abstract_stats = analyze_abstract(paper.get('abstract', ''))
            
            paper_data = {
                'arxiv_id': paper.get('id', ''),
                'title': paper.get('title', ''),
                'authors': paper.get('authors', []),
                'abstract': paper.get('abstract', ''),
                'categories': paper.get('categories', []),
                'published': paper.get('published', ''),
                'updated': paper.get('updated', ''),
                'abstract_stats': abstract_stats
            }
            papers_data.append(paper_data)
        except Exception as e:
            log_warning(f"Error processing paper {paper.get('id', 'unknown')} for JSON output: {e}", log_events)
            continue
    
    try:
        papers_file = os.path.join(output_dir, 'papers.json')
        with open(papers_file, 'w', encoding='utf-8') as f:
            json.dump(papers_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log_error(f"Error writing papers.json: {e}", log_events)
        raise
    
    return papers_data


def create_corpus_analysis(papers_data, search_query, output_dir, log_events):
    """Create corpus_analysis.json file with the required format."""
    try:
        processing_timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Corpus statistics
        total_abstracts = len(papers_data)
        total_words = sum(paper['abstract_stats']['total_words'] for paper in papers_data)
        unique_words_global = set()
        
        abstract_lengths = []
        for paper in papers_data:
            abstract = paper['abstract']
            try:
                words = re.findall(r'\b\w+\b', abstract.lower())
                unique_words_global.update(words)
                abstract_lengths.append(len(words))
            except Exception:
                continue
        
        unique_words_global_count = len(unique_words_global)
        
        if abstract_lengths:
            avg_abstract_length = sum(abstract_lengths) / len(abstract_lengths)
            longest_abstract_words = max(abstract_lengths)
            shortest_abstract_words = min(abstract_lengths)
        else:
            avg_abstract_length = 0.0
            longest_abstract_words = 0
            shortest_abstract_words = 0
        
        # Word frequency analysis (excluding stopwords) - case insensitive
        word_frequency = Counter()
        word_documents = defaultdict(set)
        
        for paper in papers_data:
            abstract = paper['abstract']
            try:
                words = re.findall(r'\b\w+\b', abstract.lower())  # Case insensitive
                filtered_words = [word for word in words if word not in STOPWORDS and len(word) > 1]
                
                word_frequency.update(filtered_words)
                for word in filtered_words:
                    word_documents[word].add(paper['arxiv_id'])
            except Exception:
                continue
        
        # Top 50 words
        top_50_words = []
        for word, frequency in word_frequency.most_common(50):
            top_50_words.append({
                'word': word,
                'frequency': frequency,
                'documents': len(word_documents[word])
            })
        
        # Technical terms extraction
        all_uppercase_terms = set()
        all_numeric_terms = set()
        all_hyphenated_terms = set()
        
        for paper in papers_data:
            uppercase, numeric, hyphenated = extract_technical_terms(paper['abstract'])
            all_uppercase_terms.update(uppercase)
            all_numeric_terms.update(numeric)
            all_hyphenated_terms.update(hyphenated)
        
        # Category distribution
        category_distribution = Counter()
        for paper in papers_data:
            category_distribution.update(paper['categories'])
        
        corpus_analysis = {
            'query': search_query,
            'papers_processed': total_abstracts,
            'processing_timestamp': processing_timestamp,
            'corpus_stats': {
                'total_abstracts': total_abstracts,
                'total_words': total_words,
                'unique_words_global': unique_words_global_count,
                'avg_abstract_length': round(avg_abstract_length, 2),
                'longest_abstract_words': longest_abstract_words,
                'shortest_abstract_words': shortest_abstract_words
            },
            'top_50_words': top_50_words,
            'technical_terms': {
                'uppercase_terms': sorted(list(all_uppercase_terms)),
                'numeric_terms': sorted(list(all_numeric_terms)),
                'hyphenated_terms': sorted(list(all_hyphenated_terms))
            },
            'category_distribution': dict(category_distribution)
        }
        
        corpus_file = os.path.join(output_dir, 'corpus_analysis.json')
        with open(corpus_file, 'w', encoding='utf-8') as f:
            json.dump(corpus_analysis, f, indent=2, ensure_ascii=False)
        
        return corpus_analysis
        
    except Exception as e:
        log_error(f"Error creating corpus analysis: {e}", log_events)
        raise


def create_processing_log(log_events, output_dir):
    """Create processing.log file with the required format."""
    try:
        log_file = os.path.join(output_dir, 'processing.log')
        with open(log_file, 'w', encoding='utf-8') as f:
            for event in log_events:
                f.write(event + '\n')
    except Exception as e:
        print(f"Error writing processing log: {e}", file=sys.stderr)


def main():
    """Main function."""
    log_events = []
    
    try:
        # Validate and parse arguments
        search_query, max_results, output_dir = validate_arguments()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Start logging
        log_events.append(log_event(f"Starting ArXiv query: {search_query}", log_events))
        
        # Query ArXiv API with retry logic
        try:
            xml_data = query_arxiv_api_with_retry(search_query, max_results)
        except Exception as e:
            log_error(f"Failed to fetch data from ArXiv API: {e}", log_events)
            create_processing_log(log_events, output_dir)
            sys.exit(1)
        
        # Parse XML response with error handling
        papers = parse_xml_response_with_error_handling(xml_data, log_events)
        
        if not papers:
            log_error("No papers could be processed from the API response", log_events)
            create_processing_log(log_events, output_dir)
            sys.exit(1)
        
        log_events.append(log_event(f"Fetched {len(papers)} results from ArXiv API", log_events))
        
        # Process each paper
        for paper in papers:
            log_events.append(log_event(f"Processing paper: {paper.get('id', 'unknown')}", log_events))
        
        # Create output files
        start_time = datetime.utcnow()
        
        try:
            # File 1: papers.json
            papers_data = create_papers_json(papers, output_dir, log_events)
            
            # File 2: corpus_analysis.json
            corpus_analysis = create_corpus_analysis(papers_data, search_query, output_dir, log_events)
            
            # File 3: processing.log
            end_time = datetime.utcnow()
            processing_time = (end_time - start_time).total_seconds()
            log_events.append(log_event(f"Completed processing: {len(papers)} papers in {processing_time:.2f} seconds", log_events))
            
            create_processing_log(log_events, output_dir)
            
            print(f"\nProcessing complete!")
            print(f"Output files written to: {output_dir}")
            print(f"  - papers.json: Paper metadata with abstract statistics")
            print(f"  - corpus_analysis.json: Aggregate analysis across all papers")
            print(f"  - processing.log: Processing log with timestamps")
            
        except Exception as e:
            log_error(f"Error creating output files: {e}", log_events)
            create_processing_log(log_events, output_dir)
            sys.exit(1)
            
    except KeyboardInterrupt:
        log_event("Processing interrupted by user", log_events)
        create_processing_log(log_events, output_dir)
        sys.exit(1)
    except Exception as e:
        log_error(f"Unexpected error: {e}", log_events)
        create_processing_log(log_events, output_dir)
        sys.exit(1)


if __name__ == "__main__":
    main()
