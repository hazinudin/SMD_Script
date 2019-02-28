import urllib
import json
import os
import ssl


class TokenGenerator(object):
    def __init__(self, username, password, referer, tokengen_url):
        """
        This class will process the inputs to generate token from the ArcGIS Server
        :param username:
        :param password:
        :param referer:
        :param tokengen_url:
        """
        # Payload to be sent for requesting ArcGIS server token
        payload = {
            "username": username,
            "password": password,
            "referer": referer,
            "f": "pjson"
        }

        # SSL context to avoid SSL ERROR
        tlsv1_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)

        encoded_payload = urllib.urlencode(payload)  # Encoded payload
        response = urllib.urlopen(tokengen_url, encoded_payload,
                                  context=tlsv1_context).read()  # Get the response
        response_dict = json.loads(response)  # Read the response JSON

        self.token = response_dict['token']
        self.expiration = response_dict['expires']


# If the script is not imported
if __name__ == '__main__':
    os.chdir('E:/SMD_Script')  # Change the directory to the SMD_Script root directory

    with open('smd_config.json') as config_f:  # Load the config.json
        config = json.load(config_f)

    arcgis_server_details = config['arcgis_server']

    UserName = arcgis_server_details['username']
    Password = arcgis_server_details['password']
    Referer = arcgis_server_details['referer']
    URL = arcgis_server_details['token_url']
    token_gent = TokenGenerator(UserName, Password, Referer, URL)

    print token_gent.token
    print token_gent.expiration
