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
    # list of keywords that, if appear in url or view_name, indicate it is not a user view
    non_user_keywords = ['braze', 'crew', 'api', 'dwh', 'oldedu', 'graphql']
    # marker variable to be switched on/off based on the logic below
    keyword_marker = 0
    # empty results list
    results = []

    for url in urls:
        # checking if host is preply or monolith
        # if monolith - straight to disqualify
        if url.split('//')[-1].split('/')[0] == 'preply.com':
            # collecting server response
            response = requests.get(url, allow_redirects=False)
            try:
                # checking for header value that is specific to non_user
                response.headers['Access-Control-Allow-Credentials']
            except:
                # if failed, means likely it is a user view - checking if keywords exist
                for keyword in non_user_keywords:
                    if (keyword in url or keyword in view_name):
                        keyword_marker = 1
                        break
                    else:
                        continue
                        
                if keyword_marker == 0:
                    # if keywords not present in url/view_name then add this as a user view
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
