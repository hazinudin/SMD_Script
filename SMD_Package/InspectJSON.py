import json
import sys
from SMD_Package import output_message
from arcpy import SetParameterAsText


def input_json_check(json_string, param_ind, escape_str=True, req_keys=None):

    if escape_str:  # If True, then decode the string escape first
        json_string = json_string.decode('string_escape')

    try:
        input_dict = json.loads(json_string)
    except TypeError:
        message = "Cannot load input string JSON, incorrect JSON format"
        SetParameterAsText(param_ind, output_message("Failed", message))
        sys.exit(0)
    except ValueError:
        message = "No JSON object could be decoded"
        SetParameterAsText(param_ind, output_message("Failed", message))
        sys.exit(0)

    for req_key in req_keys:
        if req_key not in input_dict:
            message = "Required key is missing from the input JSON. Missing key=[{0}]".format(req_key)
            SetParameterAsText(param_ind, output_message("Failed", message))
            sys.exit(0)

    return input_dict
