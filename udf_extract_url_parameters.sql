-- UDF to standardise the way we extract parameters from Preply's URLs

CREATE OR REPLACE FUNCTION EXTRACT_URL_PARAMETERS(url STRING, parameters ARRAY)
  RETURNS OBJECT
  LANGUAGE JAVASCRIPT
  AS '
  function extractParameter(url, parameter) {
    var regex = new RegExp(parameter.replace(/[-]/g, "\\$&") + "=([^&]+)"); // standard regex for all parameters should be the same
    // it matches all text cahracters, including hyphens, all the way up to the "&" sign
    var match = regex.exec(url);
    return match ? match[1] : null;
  }

  var extractedParameters = {};
  // looping over all parameters provided to the function
  for (var i = 0; i < PARAMETERS.length; i++) {
    var parameter = PARAMETERS[i];
    var extractedValue = extractParameter(URL, parameter);
    if (extractedValue !== null) {
      extractedParameters[parameter] = extractedValue;
    }
  }
  return extractedParameters;
  ';

-- sample URL is real, taken from DDS.PAGE_VISIT
WITH sample_data as (
SELECT
'https://preply.com/zh/online/english-tutors?campaignid=376670411&network=s&adgroupid=1148990415608683&keyword=%2B%E8%8B%B1%E8%AA%9E%20%2B%E8%AA%B2%E7%A8%8B&matchtype=p&creative=&adposition=&targetid=kwd-71812456812340:loc-200&placement=&loc_physical_ms=200&device=m&creative=&adwc=&adwg=&msclkid=37a512bbd23517d18f8372d34f2e96a6&utm_source=bing&utm_medium=cpc&utm_campaign=stu_sem_generic_web_0_cht_xx_multiplesub_bmm&utm_term=%2B%E8%8B%B1%E8%AA%9E%20%2B%E8%AA%B2%E7%A8%8B&utm_content=english_course_method_bmm' AS url
),

url_parameters AS (
  SELECT
    url
    -- providing array of parameters to look for in the URL
    , EXTRACT_URL_PARAMETERS(url, ARRAY_CONSTRUCT('utm_source'
                                                , 'utm_medium'
                                                , 'utm_content'
                                                , 'network'
                                                , 'campaignid'
                                                , 'adgroupid'
                                                , 'utm_campaign')) AS extracted_parameters
  FROM sample_data
)

-- extracting parameters in a similar way to JSON extraction and parsing as a string for safety
SELECT
  extracted_parameters:"utm_source"::STRING AS utm_source,
  extracted_parameters:"utm_medium"::STRING AS utm_medium,
  extracted_parameters:"utm_content"::STRING AS utm_content,
  extracted_parameters:"network"::STRING AS network,
  extracted_parameters:"campaignid"::STRING AS campaignid,
  extracted_parameters:"adgroupid"::STRING AS adgroupid,
  extracted_parameters:"utm_campaign"::STRING AS utm_campaign,
  url,
  extracted_parameters
FROM url_parameters;
