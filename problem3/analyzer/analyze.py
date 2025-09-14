#!/usr/bin/env python3
import json
import os
import re
import time
from datetime import datetime, timezone
from collections import Counter

def jaccard_similarity(doc1_words, doc2_words):
    """Calculate Jaccard similarity between two documents."""
    set1 = set(doc1_words)
    set2 = set(doc2_words)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0.0

def tokenize_text(text):
    """Tokenize text into words (lowercase, alphanumeric only)."""
    words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
    return words

def extract_ngrams(words, n):
    """Extract n-grams from a list of words."""
    if len(words) < n:
        return []
    
    ngrams = []
    for i in range(len(words) - n + 1):
        ngram = ' '.join(words[i:i+n])
        ngrams.append(ngram)
    
    return ngrams

def calculate_readability_metrics(all_texts):
    """Calculate readability metrics for the corpus."""
    total_sentences = 0
    total_words = 0
    total_syllables = 0
    total_chars = 0
    
    for text in all_texts:
        # Count sentences
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        total_sentences += len(sentences)
        
        # Count words and characters
        words = tokenize_text(text)
        total_words += len(words)
        total_chars += sum(len(word) for word in words)
        
        # Estimate syllables (rough approximation)
        for word in words:
            # Simple syllable counting: vowels and vowel groups
            syllable_count = len(re.findall(r'[aeiouy]+', word))
            if syllable_count == 0:
                syllable_count = 1  # At least one syllable per word
            total_syllables += syllable_count
    
    # Calculate metrics
    if total_sentences > 0:
        avg_sentence_length = total_words / total_sentences
    else:
        avg_sentence_length = 0
    
    if total_words > 0:
        avg_word_length = total_chars / total_words
        avg_syllables_per_word = total_syllables / total_words
    else:
        avg_word_length = 0
        avg_syllables_per_word = 0
    
    # Simplified Flesch Reading Ease score
    if avg_sentence_length > 0 and avg_syllables_per_word > 0:
        complexity_score = 206.835 - (1.015 * avg_sentence_length) - (84.6 * avg_syllables_per_word)
    else:
        complexity_score = 0
    
    return {
        "avg_sentence_length": round(avg_sentence_length, 2),
        "avg_word_length": round(avg_word_length, 2),
        "complexity_score": round(complexity_score, 2)
    }

def analyze_corpus(processed_files):
    """Analyze the entire corpus of processed documents."""
    all_words = []
    all_texts = []
    word_frequency = Counter()
    bigram_frequency = Counter()
    trigram_frequency = Counter()
    documents_data = []
    
    print(f"Analyzing {len(processed_files)} documents...", flush=True)
    
    for file_info in processed_files:
        if file_info["status"] == "success":
            file_path = f"/shared/processed/{file_info['output_file']}"
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                text = data.get("text", "")
                all_texts.append(text)
                
                # Tokenize text
                words = tokenize_text(text)
                all_words.extend(words)
                word_frequency.update(words)
                
                # Extract n-grams
                bigrams = extract_ngrams(words, 2)
                trigrams = extract_ngrams(words, 3)
                bigram_frequency.update(bigrams)
                trigram_frequency.update(trigrams)
                
                documents_data.append({
                    "filename": file_info['output_file'],
                    "words": words,
                    "word_count": len(words)
                })
                
            except Exception as e:
                print(f"Error reading {file_info['output_file']}: {e}", flush=True)
    
    return {
        "all_words": all_words,
        "all_texts": all_texts,
        "word_frequency": word_frequency,
        "bigram_frequency": bigram_frequency,
        "trigram_frequency": trigram_frequency,
        "documents_data": documents_data
    }

def calculate_document_similarities(documents_data):
    """Calculate pairwise Jaccard similarities between documents."""
    similarities = []
    
    print("Calculating document similarities...", flush=True)
    
    for i in range(len(documents_data)):
        for j in range(i + 1, len(documents_data)):
            doc1_words = documents_data[i]["words"]
            doc2_words = documents_data[j]["words"]
            
            similarity = jaccard_similarity(doc1_words, doc2_words)
            
            similarities.append({
                "doc1": documents_data[i]["filename"],
                "doc2": documents_data[j]["filename"],
                "similarity": round(similarity, 4)
            })
    
    return similarities


def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Analyzer starting", flush=True)

    # Create output directory
    os.makedirs("/shared/analysis", exist_ok=True)
    os.makedirs("/shared/status", exist_ok=True)
    
    # Wait for processor to complete
    process_status_file = "/shared/status/process_complete.json"
    while not os.path.exists(process_status_file):
        print("Waiting for processor to complete...", flush=True)
        time.sleep(2)
    
    # Read process status
    with open(process_status_file, 'r') as f:
        process_status = json.load(f)
    
    
    
    # Get successful files
    successful_files = [r for r in process_status["results"] if r["status"] == "success"]
    
    if not successful_files:
        print("No successful files to analyze", flush=True)
        return
    
    # Analyze corpus
    corpus_data = analyze_corpus(successful_files)
    
    # Calculate document similarities
    similarities = calculate_document_similarities(corpus_data["documents_data"])
    
    # Calculate readability metrics
    readability = calculate_readability_metrics(corpus_data["all_texts"])
    
    # Prepare top words with frequency
    total_words = len(corpus_data["all_words"])
    top_100_words = []
    for word, count in corpus_data["word_frequency"].most_common(100):
        frequency = count / total_words if total_words > 0 else 0
        top_100_words.append({
            "word": word,
            "count": count,
            "frequency": round(frequency, 4)
        })
    
    # Prepare top bigrams
    top_bigrams = []
    for bigram, count in corpus_data["bigram_frequency"].most_common(20):
        top_bigrams.append({
            "bigram": bigram,
            "count": count
        })
    
    # Create final report
    final_report = {
        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
        "documents_processed": len(successful_files),
        "total_words": total_words,
        "unique_words": len(corpus_data["word_frequency"]),
        "top_100_words": top_100_words,
        "document_similarity": similarities,
        "top_bigrams": top_bigrams,
        "readability": readability
    }
    
    # Write final report
    with open("/shared/analysis/final_report.json", 'w') as f:
        json.dump(final_report, f, indent=2, ensure_ascii=False)
    
    print(f"[{datetime.now(timezone.utc).isoformat()}] Analyzer complete", flush=True)
    print(f"Analysis complete:", flush=True)
    print(f"  - {final_report['documents_processed']} documents processed", flush=True)
    print(f"  - {final_report['total_words']} total words", flush=True)
    print(f"  - {final_report['unique_words']} unique words", flush=True)
    print(f"  - {len(similarities)} document pairs compared", flush=True)
    print(f"  - Average complexity score: {readability['complexity_score']}", flush=True)

if __name__ == "__main__":
    main()