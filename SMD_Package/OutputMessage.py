import json

def output_message(status, message):
    """
    This function will create a string with a JSON format containing a sucess or failed message
    :param status: The status of the process that will be written into a JSON string
    :param message: The message which explain the details of the status
    :return: string with JSON format
    """
    message = {
        "status": status,
        "msg": message
    }

    return json.dumps(message)
