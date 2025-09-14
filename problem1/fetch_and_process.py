import urllib.request
import sys, json, time, datetime, os, re


def read_urls(input_file):
    '''
    Read urls from input file, in each iteration, read one url at the same time
    return a list of urls
    '''
    urls = []
    
    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            for line in file:
                url = line.strip()
                if url:
                    urls.append(url)
    except FileNotFoundError:
        print(f"Error: File not found: {input_file}")
        return []
    return urls

def count_words(text):
    words = text.split()
    return len(words)

def fetch_url(url):
    # measure the response time in milliseconds
    start_time = time.time()
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    iso_timestamp_z = timestamp.isoformat(timespec='seconds') + 'Z'

    response_time_ms = 0
    content_length = 0
    word_count = None
    status_code = 0
    
    # try fetch the url
    try:
        request = urllib.request.Request(url)
        with urllib.request.urlopen(request, timeout=10) as response:
            end_time = time.time()
            response_time_ms = (end_time - start_time) * 1000

            status_code = response.getcode()
            response_body = response.read()
            content_length = len(response_body)

            

            print('=' * 100)
            print(f"URL: {url}")
            print(f"Status Code: {status_code}")
            print(f"Response time: {response_time_ms} ms")
            print(f"Response Body Size: {len(response_body)} bytes")
            print(f"Content Type: {response.getheader('Content-Type')}")
            
            words_count = None
            if 'text' in response.getheader('Content-Type'):
                text_content = response_body.decode('utf-8')
                words_count = count_words(text_content)
                print(f"Words Count: {words_count}")
            print('=' * 100)
            

            return {
                'url': url,
                'status_code': status_code,
                'response_time_ms': response_time_ms,
                'content_length': content_length,
                'words_count': words_count,
                'timestamp': iso_timestamp_z,
                'error': None,
            }

    except urllib.error.HTTPError as e:
        '''HTTP Errors (400 - 599)'''
        end_time = time.time()
        response_time_ms = (end_time - start_time) * 1000
        status_code = e.code

        try:
            response_body = e.read()
            content_length = len(response_body)
        except:
            response_body = b''
            content_length = 0
        
        # check if content type is text
        content_type = e.headers.get('Content-Type', '') if e.headers else ''
        print('=' * 100)
        print(f"Content Type: {content_type}")

        if content_type and 'text' in content_type.lower():
            try:
                text_content = response_body.decode('utf-8')
                word_count = count_words(text_content)
                print(f"Words Count: {word_count}")
            except:
                word_count = None
                print("Words Count: Failed to decode")
        print('=' * 100)
        
        return {
            'url': url,
            'status_code': status_code,
            'response_time_ms': response_time_ms,
            'content_length': content_length,
            'word_count': word_count,
            'timestamp': iso_timestamp_z,
            'error': str(e),
        }
    except urllib.error.URLError as e:
        # URL errors (connection error)
        end_time = time.time()
        response_time_ms = (end_time - start_time) * 1000
        
        print('=' * 100)
        print(f"URL Error: {e.reason}")
        print('=' * 100)
        
        return {
            'url': url,
            'status_code': 0,
            'response_time_ms': response_time_ms,
            'content_length': 0,
            'word_count': None,
            'timestamp': iso_timestamp_z,
            'error': str(e),
        }

    
    except Exception as e:
        end_time = time.time()
        response_time_ms = (end_time - start_time) * 1000

        print('=' * 100)
        print(f"Error: {e}")
        print('=' * 100)
        return {
                'url': url,
                'status_code': 0,
                'response_time_ms': response_time_ms,
                'content_length': 0,
                'words_count': None,
                'timestamp': iso_timestamp_z,
                'error': str(e),
        }

def write_to_json(response_dict, output_directory_path, file_name):
    file_path = output_directory_path + '/' + file_name
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(response_dict, file, ensure_ascii=False, indent=4)
        file.write('\n')
        
def write_error_log(errors, output_directory_path):
    file_path = output_directory_path + '/errors.log'
    with open(file_path, 'w', encoding='utf-8') as file:
        for error in errors:
            content = f"[{error['timestamp']}] [{error['url']}]: [{error['error']}]"
            file.write(content + '\n')

    
def main():
    '''
    takes two command line arguements:
    1. the input file containing the urls
    2. the output file to save the results
    '''
    # start time
    start_time = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds') + 'Z'

    if len(sys.argv) != 3:
        print("Usage: python fetch_and_process.py <input_file> <output_file>")
        sys.exit(1)
    
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]

    # read urls from input file
    try:
        urls = read_urls(input_file_path)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    response_dicts = []
    errors_dicts = []
    successful_request_cnt = 0
    total_response_time_ms = 0.0
    total_bytes_downloaded = 0
    status_code_distribution = {}

    # process urls
    for url in urls:
        # request url and save the response
        response_dict = fetch_url(url)  
        response_dicts.append(response_dict) 

        if response_dict['status_code'] == 200:
            successful_request_cnt += 1
        else:
            errors_dicts.append(response_dict)
        total_response_time_ms += response_dict['response_time_ms']
        total_bytes_downloaded += response_dict['content_length']
        if response_dict['status_code'] in status_code_distribution:
            status_code_distribution[response_dict['status_code']] += 1
        else:
            status_code_distribution[response_dict['status_code']] = 1

        # time.sleep(10)

    # write responses.json
    write_to_json(response_dicts, output_file_path, 'responses.json')

    end_time = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds') + 'Z'

    # write summary.json
    summary_dict = {
        "total_urls": len(urls),
        "successful_requests": successful_request_cnt,
        "failed_requests": len(urls) - successful_request_cnt,
        "average_response_time_ms": total_response_time_ms / len(urls),
        "total_bytes_downloaded": total_bytes_downloaded,
        "status_code_distribution": status_code_distribution,
        "processing_start": start_time,
        "processing_end": end_time,
    }

    # write summary.json
    write_to_json(summary_dict, output_file_path, 'summary.json')

    # write errors.log
    write_error_log(errors_dicts, output_file_path)

    sys.exit(0)

if __name__ == "__main__":
    main()