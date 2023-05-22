import requests
import pandas as pd

def add_user_entry(results, url, view_name, user_type='non_user'):
    results.append( {
                    'url' : url,
                    'view_name' : view_name,
                    'view_type' : user_type
                })

def process_urls(view_name, urls):
    non_user_keywords = ['braze', 'crew', 'api', 'dwh', 'oldedu', 'graphql']
    keyword_marker = 0
    results = []

    for url in urls:
        if url.split('//')[-1].split('/')[0] == 'preply.com':
            response = requests.get(url, allow_redirects=False)
            try:
                response.headers['Access-Control-Allow-Credentials']
            except:
                for keyword in non_user_keywords:
                    if (keyword in url or keyword in view_name):
                        keyword_marker = 1
                        break
                    else:
                        continue
                        
                if keyword_marker == 0:
                    add_user_entry(results, url, view_name, 'user')
                break
            else:
                continue
        else:
            continue
    

    if results:
        return pd.DataFrame(results)
    else:
        add_user_entry(results, url, view_name, 'non_user')
        return pd.DataFrame(results)
