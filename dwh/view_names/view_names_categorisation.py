import requests
import pandas as pd

# function to simplify appending the results
def add_user_entry(results, url, view_name, user_type='non_user'):
    results.append( {
                    'url' : url,
                    'view_name' : view_name,
                    'view_type' : user_type
                })
# below function is set in such a way, that it aims to identify if either of the urls
# assigned to a particular view can be accessed by a real user
# it iterates through urls checking for different variables
# by the end of the for loop, either there was at least 1 user accessible url found
# which breaks the loop and returns this url together with view and type
# if no such url was found then results are artificially appended indicating non_user type
def process_urls(view_name, urls):
    # empty results list
    results = []
    for url in urls:
        # checking if host is preply or monolith
        # if monolith - straight to disqualify
        if url.split('//')[-1].split('/')[0] != 'preply.com':
            continue

        # collecting server response
        response = requests.get(url, allow_redirects=True)

        if 'text/html' not in response.headers['Content-Type']:
            continue

        if 'Access-Control-Allow-Credentials' in response.headers:
            continue

        if response.status_code in (403, 404, 405):
            continue 

        add_user_entry(results, url, view_name, 'user')
        break

    if results:
        return pd.DataFrame(results)
    else:
        add_user_entry(results, url, view_name, 'non_user')
        return pd.DataFrame(results)
